package delivery

import (
	"bytes"
	"context"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/common"
	"github.com/prometheus/client_golang/prometheus"
)

const posNotFound int64 = -1

// MarkupKey - key for search position.
type MarkupKey struct {
	typeFrame TypeFrame
	common.SegmentKey
}

// StorageManager - manager for file refill. Contains file markup for quick access to data.
type StorageManager struct {
	// title frame
	title *Title
	// marking positions of Segments and Snapshots
	markupMap map[MarkupKey]int64
	// last status of writers
	ackStatus *AckStatus
	// last statuses write, need write frame if have differents
	statuses Statuses
	// last statuses write
	lastWriteSegment []uint32
	// storage for save data
	storage *FileStorage
	// last position when writing to
	lastWriteOffset int64
	// state open file
	isOpenFile bool
	// are there any rejects
	hasRejects atomic.Bool
	// stat
	currentSize prometheus.Gauge
	readBytes   prometheus.Histogram
	writeBytes  prometheus.Histogram
}

// NewStorageManager - init new MarkupMap.
func NewStorageManager(
	cfg *FileStorageConfig,
	shardsNumberPower uint8,
	blockID uuid.UUID,
	registerer prometheus.Registerer,
	names ...string,
) (*StorageManager, error) {
	var err error
	factory := NewConflictRegisterer(registerer)
	sm := &StorageManager{
		markupMap:  make(map[MarkupKey]int64),
		hasRejects: atomic.Bool{},
		currentSize: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "odarix_core_delivery_storage_manager_current_refill_size",
				Help: "Size of current refill.",
			},
		),
		readBytes: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_storage_manager_read_bytes",
				Help:    "Number of read bytes.",
				Buckets: prometheus.ExponentialBucketsRange(1024, 125829120, 10),
			},
		),
		writeBytes: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_storage_manager_write_bytes",
				Help:    "Number of write bytes.",
				Buckets: prometheus.ExponentialBucketsRange(1024, 125829120, 10),
			},
		),
	}

	// init storage
	sm.storage, err = NewFileStorage(cfg)
	if err != nil {
		return nil, err
	}

	// trying to recover from a storage
	ok, err := sm.restore()
	if err != nil {
		switch err {
		case ErrServiceDataNotRestored{}:
			if err = sm.storage.Truncate(0); err != nil {
				return nil, err
			}
		case ErrUnknownFrameType:
			if err = sm.storage.Truncate(sm.lastWriteOffset); err != nil {
				return nil, err
			}
			ok = true
		default:
			return nil, err
		}
	}
	if !ok {
		sm.title = NewTitle(shardsNumberPower, blockID)
		sm.ackStatus = NewAckStatus(names, shardsNumberPower)
		sm.lastWriteSegment = newShardStatuses(1 << shardsNumberPower)
		sm.setLastWriteOffset(0)
	}

	sm.statuses = sm.ackStatus.GetCopyAckStatuses()

	if sm.ackStatus.Shards() != 1<<shardsNumberPower {
		return sm, ErrShardsNotEqual
	}

	if !sm.ackStatus.names.Equal(names...) {
		return sm, ErrDestinationsNamesNotEqual
	}

	return sm, nil
}

// BlockID - return if exist blockID or nil.
func (sm *StorageManager) BlockID() uuid.UUID {
	return sm.title.GetBlockID()
}

// Shards - return number of Shards.
func (sm *StorageManager) Shards() int {
	return sm.ackStatus.Shards()
}

// Destinations - return number of Destinations.
func (sm *StorageManager) Destinations() int {
	return sm.ackStatus.Destinations()
}

// LastSegment - return last ack segment by shard and destination.
func (sm *StorageManager) LastSegment(shardID uint16, dest string) uint32 {
	return sm.ackStatus.Last(shardID, dest)
}

// CheckSegmentsSent - checking if all segments have been sent.
func (sm *StorageManager) CheckSegmentsSent() bool {
	// if there is at least 1 reject, then you cannot delete the refill
	if sm.hasRejects.Load() {
		return false
	}

	for d := 0; d < sm.ackStatus.Destinations(); d++ {
		for shardID := range sm.lastWriteSegment {
			i := d*len(sm.lastWriteSegment) + shardID
			// check first status
			if sm.statuses[i] == math.MaxUint32 && sm.lastWriteSegment[shardID] != math.MaxUint32 {
				return false
			}

			// check not first status
			if sm.statuses[i] < sm.lastWriteSegment[shardID] {
				return false
			}
		}
	}

	return true
}

// Ack - increment status by destination and shard if segment is next for current value.
func (sm *StorageManager) Ack(key common.SegmentKey, dest string) {
	sm.ackStatus.Ack(key, dest)
}

// Reject - accumulates rejects and serializes and writes to refill while recording statuses.
func (sm *StorageManager) Reject(segKey common.SegmentKey, dest string) {
	sm.hasRejects.Store(true)
	sm.ackStatus.Reject(segKey, dest)
}

// getSnapshotPosition - return position in storage.
func (sm *StorageManager) getSnapshotPosition(segKey common.SegmentKey) int64 {
	mk := MarkupKey{
		typeFrame:  SnapshotType,
		SegmentKey: segKey,
	}
	pos, ok := sm.markupMap[mk]
	if ok {
		return pos
	}

	return posNotFound
}

// setSnapshotPosition - set segment position in storage.
func (sm *StorageManager) setSnapshotPosition(segKey common.SegmentKey, position int64) {
	mk := MarkupKey{
		typeFrame:  SnapshotType,
		SegmentKey: segKey,
	}
	sm.markupMap[mk] = position

	if sm.lastWriteSegment[segKey.ShardID] == math.MaxUint32 || segKey.Segment > sm.lastWriteSegment[segKey.ShardID] {
		sm.lastWriteSegment[segKey.ShardID] = segKey.Segment
	}
}

// GetSnapshot - return snapshot from storage.
func (sm *StorageManager) GetSnapshot(ctx context.Context, segKey common.SegmentKey) (common.Snapshot, error) {
	// get position
	pos := sm.getSnapshotPosition(segKey)
	if pos == posNotFound {
		return nil, ErrSnapshotNotFoundRefill
	}

	// read frame
	snapshotData, err := ReadFrameSnapshot(ctx, sm.storage, pos)
	if err != nil {
		return nil, err
	}

	sm.readBytes.Observe(float64(snapshotData.SizeOf()))

	return snapshotData, nil
}

// getSegmentPosition - return position in storage.
func (sm *StorageManager) getSegmentPosition(segKey common.SegmentKey) int64 {
	mk := MarkupKey{
		typeFrame:  SegmentType,
		SegmentKey: segKey,
	}
	pos, ok := sm.markupMap[mk]
	if ok {
		return pos
	}

	return posNotFound
}

// setSegmentPosition - set segment position in storage.
func (sm *StorageManager) setSegmentPosition(segKey common.SegmentKey, position int64) {
	mk := MarkupKey{
		typeFrame:  SegmentType,
		SegmentKey: segKey,
	}
	sm.markupMap[mk] = position

	if sm.lastWriteSegment[segKey.ShardID] == math.MaxUint32 || segKey.Segment > sm.lastWriteSegment[segKey.ShardID] {
		sm.lastWriteSegment[segKey.ShardID] = segKey.Segment
	}
}

// GetSegment - return segment from storage.
func (sm *StorageManager) GetSegment(ctx context.Context, segKey common.SegmentKey) (Segment, error) {
	// get position
	pos := sm.getSegmentPosition(segKey)
	if pos == posNotFound {
		return nil, SegmentNotFoundInRefill(segKey)
	}

	// read frame
	segmentData, err := ReadFrameSegment(ctx, sm.storage, pos)
	if err != nil {
		return nil, err
	}

	sm.readBytes.Observe(float64(segmentData.SizeOf()))

	return segmentData, nil
}

// GetAckStatus - return last AckStatus.
func (sm *StorageManager) GetAckStatus() *AckStatus {
	return sm.ackStatus
}

// restoreFromBody - restore from body frame.
func (sm *StorageManager) restoreFromBody(h *HeaderFrame, off int64) error {
	// TODO restore bad frame
	switch h.GetType() {
	case TitleType:
		return sm.restoreTitle(off)
	case DestinationNamesType:
		return sm.restoreDestinationsNames(off)
	case SnapshotType:
		return sm.restoreSnapshot(h, off)
	case SegmentType:
		return sm.restoreSegment(h, off)
	case StatusType:
		return sm.restoreStatuses(h, off)
	case RejectStatusType:
		sm.hasRejects.Store(true)
		// skip reject statuses
		return nil
	}

	return nil
}

// restore - restore title from frame.
func (sm *StorageManager) restoreTitle(off int64) error {
	var err error
	sm.title, err = ReadTitle(sm.storage, off)
	if err != nil {
		return err
	}

	// init lastWriteSegment for future reference and not to panic
	sm.lastWriteSegment = newShardStatuses(1 << sm.title.shardsNumberPower)

	return nil
}

// restore - restore Destinations Names from frame.
func (sm *StorageManager) restoreDestinationsNames(off int64) error {
	// init lastWriteSegment for future reference and not to panic
	// init with shardsNumberPower to init statuses for null values
	sm.ackStatus = NewAckStatusEmpty(sm.title.shardsNumberPower)
	return sm.ackStatus.ReadDestinationsNames(sm.storage, off)
}

// checkRestoredServiceData - check restored service data(title, destinations names),
// these data are required to be restored, without them you cant read the rest
func (sm *StorageManager) checkRestoredServiceData() bool {
	return sm.title != nil && sm.ackStatus != nil
}

// restore - restore Snapshot from frame.
func (sm *StorageManager) restoreSnapshot(h *HeaderFrame, off int64) error {
	if !sm.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	sm.setSnapshotPosition(
		common.SegmentKey{
			ShardID: h.GetShardID(),
			Segment: h.GetSegmentID(),
		},
		off-int64(h.SizeOf()),
	)

	return nil
}

// restore - restore Segment from frame.
func (sm *StorageManager) restoreSegment(h *HeaderFrame, off int64) error {
	if !sm.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	sm.setSegmentPosition(
		common.SegmentKey{
			ShardID: h.GetShardID(),
			Segment: h.GetSegmentID(),
		},
		off-int64(h.SizeOf()),
	)

	return nil
}

// restoreStatuses - restore states of writers from frame.
func (sm *StorageManager) restoreStatuses(h *HeaderFrame, off int64) error {
	if !sm.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	buf := make([]byte, h.size)
	if _, err := sm.storage.ReadAt(buf, off); err != nil {
		return err
	}
	return sm.ackStatus.status.UnmarshalBinary(buf)
}

// restore - restore MarkupMap from storage.
func (sm *StorageManager) restore() (bool, error) {
	ctx := context.Background()
	// check file
	ok, err := sm.storage.FileExist()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	// open file
	if err = sm.storage.OpenFile(); err != nil {
		return false, err
	}
	sm.isOpenFile = true

	var off int64
	for {
		// read header frame
		h, err := ReadHeader(ctx, sm.storage, off)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, err
		}
		off += int64(h.SizeOf())

		// restore data from body
		err = sm.restoreFromBody(h, off)
		if err != nil {
			return false, err
		}

		// move cursor position
		off += int64(h.GetSize())
		sm.setLastWriteOffset(off)
	}

	// check for nil file
	if !sm.checkRestoredServiceData() {
		return false, ErrServiceDataNotRestored{}
	}

	return true, nil
}

// writeTitle - write title in storage.
func (sm *StorageManager) writeTitle(ctx context.Context) error {
	frame := NewTitleFrame(sm.title.GetShardsNumberPower(), sm.BlockID()).Encode()
	n, err := sm.storage.WriteAt(ctx, frame, sm.lastWriteOffset)
	sm.writeBytes.Observe(float64(n))
	if err != nil {
		return err
	}

	// move position
	sm.moveLastWriteOffset(int64(n))

	return nil
}

// writeDestinationNames - write destination names in storage.
func (sm *StorageManager) writeDestinationNames(ctx context.Context) error {
	frame := NewDestinationsNamesFrame(sm.ackStatus.names).Encode()
	n, err := sm.storage.WriteAt(ctx, frame, sm.lastWriteOffset)
	sm.writeBytes.Observe(float64(n))
	if err != nil {
		return err
	}

	// move position
	sm.moveLastWriteOffset(int64(n))

	return nil
}

// openNewFile - open new file for new writes and write first frames.
func (sm *StorageManager) openNewFile(ctx context.Context) error {
	// open file
	if err := sm.storage.OpenFile(); err != nil {
		return err
	}

	// write title data
	if err := sm.writeTitle(ctx); err != nil {
		return err
	}

	// write destinations names
	if err := sm.writeDestinationNames(ctx); err != nil {
		return err
	}

	// set flag
	sm.isOpenFile = true

	return nil
}

// checkSegment - check for consistency segment.
func (sm *StorageManager) checkSegment(key common.SegmentKey) error {
	if key.IsFirst() {
		return nil
	}

	if sm.getSnapshotPosition(key) != posNotFound {
		return nil
	}

	if sm.getSegmentPosition(key.Prev()) != posNotFound {
		return nil
	}

	return ErrSnapshotRequired
}

// WriteSegment - write Segment in storage.
func (sm *StorageManager) WriteSegment(ctx context.Context, key common.SegmentKey, seg Segment) error {
	if !sm.isOpenFile {
		if err := sm.openNewFile(ctx); err != nil {
			return err
		}
	}

	// check for consistency segment
	err := sm.checkSegment(key)
	if err != nil {
		return err
	}
	frame := NewSegmentFrame(key.ShardID, key.Segment, seg.Bytes()).Encode()
	n, err := sm.storage.WriteAt(ctx, frame, sm.lastWriteOffset)
	sm.writeBytes.Observe(float64(n))
	if err != nil {
		return err
	}

	sm.setSegmentPosition(key, sm.lastWriteOffset)
	sm.moveLastWriteOffset(int64(n))

	return nil
}

// WriteSnapshot - write Snapshot in storage.
func (sm *StorageManager) WriteSnapshot(ctx context.Context, segKey common.SegmentKey, snapshot Snapshot) error {
	if !sm.isOpenFile {
		if err := sm.openNewFile(ctx); err != nil {
			return err
		}
	}

	frame := NewSnapshotFrame(segKey.ShardID, segKey.Segment, snapshot.Bytes()).Encode()
	n, err := sm.storage.WriteAt(ctx, frame, sm.lastWriteOffset)
	sm.writeBytes.Observe(float64(n))
	if err != nil {
		return err
	}

	sm.setSnapshotPosition(common.SegmentKey{ShardID: segKey.ShardID, Segment: segKey.Segment}, sm.lastWriteOffset)
	sm.moveLastWriteOffset(int64(n))

	return nil
}

// WriteAckStatus - write AckStatus to storage if there are differences in the manager.
func (sm *StorageManager) WriteAckStatus(ctx context.Context) error {
	// check open file
	if !sm.isOpenFile {
		return nil
	}
	// lock and get copy all statuses
	// TODO: separate acks and rejects
	sm.ackStatus.Lock()
	ss := sm.ackStatus.GetCopyAckStatuses()
	rejects := sm.ackStatus.RotateRejects()
	sm.ackStatus.Unlock()

	if len(rejects) != 0 {
		frame, err := NewRejectStatusesFrame(rejects)
		if err != nil {
			sm.ackStatus.UnrotateRejects(rejects)
			return err
		}
		n, err := sm.storage.WriteAt(ctx, frame.Encode(), sm.lastWriteOffset)
		sm.writeBytes.Observe(float64(n))
		if err != nil {
			sm.ackStatus.UnrotateRejects(rejects)
			return err
		}
		sm.moveLastWriteOffset(int64(n))
	}
	if !sm.statuses.Equal(ss) {
		frame, err := NewStatusesFrame(ss)
		if err != nil {
			// TODO: unrotate
			return err
		}
		n, err := sm.storage.WriteAt(ctx, frame.Encode(), sm.lastWriteOffset)
		sm.writeBytes.Observe(float64(n))
		if err != nil {
			// TODO: unrotate
			return err
		}
		sm.moveLastWriteOffset(int64(n))
		sm.statuses = ss
	}
	return nil
}

// moveLastWriteOffset - increase the last offset position by n.
func (sm *StorageManager) moveLastWriteOffset(n int64) {
	sm.lastWriteOffset += n
	sm.currentSize.Set(float64(sm.lastWriteOffset))
}

// setLastWriteOffset - set last offset position to n.
func (sm *StorageManager) setLastWriteOffset(offset int64) {
	sm.lastWriteOffset = offset
	sm.currentSize.Set(float64(sm.lastWriteOffset))
}

// FileExist - check file exist.
func (sm *StorageManager) FileExist() (bool, error) {
	return sm.storage.FileExist()
}

// Rename file
func (sm *StorageManager) Rename(name string) error {
	return sm.storage.Rename(name)
}

// IntermediateRename - rename the current file to blockID with temporary extension for further conversion to refill.
func (sm *StorageManager) IntermediateRename(name string) error {
	return sm.storage.IntermediateRename(name)
}

// GetIntermediateName returns true with file name if file was intermediate renamed
func (sm *StorageManager) GetIntermediateName() (string, bool) {
	return sm.storage.GetIntermediateName()
}

// DeleteCurrentFile - close and delete current file..
func (sm *StorageManager) DeleteCurrentFile() error {
	// reinit, because file deleted
	sm.lastWriteSegment = newShardStatuses(1 << sm.title.shardsNumberPower)
	sm.markupMap = make(map[MarkupKey]int64)
	sm.statuses.Reset()

	// check open file
	if !sm.isOpenFile {
		return nil
	}

	// set flag and delete
	sm.isOpenFile = false

	return sm.storage.DeleteCurrentFile()
}

// Close - close storage.
func (sm *StorageManager) Close() error {
	// set flag and close
	sm.isOpenFile = false
	err := sm.storage.Close()
	if err != nil {
		return err
	}

	return nil
}

/*
+--------------------------------+
|              Frame             |
|+------------------------------+|
||             Header           ||
|+------------------------------+|
||            11 byte           ||
||      1 typeFrame uint8       ||
||      2 shardID uint16        ||
||      4 segmentID uint32      ||
||      4   size uint32         ||
|+------------------------------+|
|+------------------------------+|
||             Body             ||
|+------------------------------+|
||             Title            ||
|+--------------or--------------+|
||       DestinationNames       ||
|+--------------or--------------+|
|| BinaryBody(Snapshot/Segment) ||
|+--------------or--------------+|
||           Statuses           ||
|+--------------or--------------+|
||        RejectStatuses        ||
|+------------------------------+|
+--------------------------------+
*/

const (
	sizeOfTypeFrame = 1
	sizeOfUint8     = 1
	sizeOfUint16    = 2
	sizeOfUint32    = 4
	sizeOfUUID      = 16
)

var (
	// ErrUnknownFrameType - error for unknown type frame.
	ErrUnknownFrameType = errors.New("unknown frame type")
	// ErrHeaderIsNil - error for nil header in frame.
	ErrHeaderIsNil = errors.New("header is nil")
	// ErrFrameTypeNotMatch - error for frame type does not match the requested one.
	ErrFrameTypeNotMatch = errors.New("frame type does not match")
)

// TypeFrame - type of frame.
type TypeFrame uint8

// Validate - validate type frame.
func (tf TypeFrame) Validate() error {
	if tf < TitleType || tf > RefillShardEOFType {
		return ErrUnknownFrameType
	}

	return nil
}

const (
	// UnknownType - unknown type frame.
	UnknownType TypeFrame = iota
	// TitleType - title type frame.
	TitleType
	// DestinationNamesType - destination names type frame.
	DestinationNamesType
	// SnapshotType - snapshot type frame.
	SnapshotType
	// SegmentType - segment type frame.
	SegmentType
	// StatusType - destinations states type frame.
	StatusType
	// RejectStatusType - reject statuses type frame.
	RejectStatusType
	// RefillShardEOFType - refill shard EOF type frame.
	RefillShardEOFType
)

// Frame - frame for write file.
type Frame struct {
	header *HeaderFrame
	body   []byte
}

// NewFrame - init new Frame.
func NewFrame(typeFrame TypeFrame, b []byte, shardID uint16, segmentID uint32) *Frame {
	return &Frame{
		header: NewHeaderFrame(typeFrame, segmentID, uint32(len(b)), shardID),
		body:   b,
	}
}

// NewFrameEmpty - init new Frame for read.
func NewFrameEmpty() *Frame {
	return &Frame{
		header: NewHeaderFrameEmpty(),
	}
}

// NewFrameWithHeader - init new Frame with header.
func NewFrameWithHeader(header *HeaderFrame) *Frame {
	return &Frame{
		header: header,
	}
}

// GetHeader - return header frame.
func (fr *Frame) GetHeader() *HeaderFrame {
	return fr.header
}

// GetBody - return body frame.
func (fr *Frame) GetBody() []byte {
	return fr.body
}

// Encode - encoding to byte.
func (fr *Frame) Encode() []byte {
	buf := make([]byte, 0, fr.SizeOf())
	buf = append(buf, fr.header.EncodeBinary()...)
	if fr.body != nil {
		buf = append(buf, fr.body...)
	}

	return buf
}

// SizeOf - get size frame.
func (fr *Frame) SizeOf() int {
	size := fr.header.SizeOf()

	if fr.body != nil {
		size += len(fr.body)
	}

	return size
}

// NewTitleFrame - init new frame.
func NewTitleFrame(nos uint8, blockID uuid.UUID) *Frame {
	return NewFrame(TitleType, NewTitle(nos, blockID).EncodeBinary(), 0, 0)
}

// ReadFrameTitle - read frame from position pos and return title.
func ReadFrameTitle(ctx context.Context, r io.ReaderAt, off int64) (*Title, int64, error) {
	h, err := ReadHeader(ctx, r, off)
	if err != nil {
		return nil, off, err
	}
	off += int64(h.SizeOf())

	if h.typeFrame != TitleType {
		return nil, off, ErrFrameTypeNotMatch
	}

	title, err := ReadTitle(r, off)
	if err != nil {
		return nil, off, err
	}
	off += int64(title.SizeOf())

	return title, off, nil
}

// NewDestinationsNamesFrameWithNames - init new frame.
func NewDestinationsNamesFrameWithNames(names ...string) *Frame {
	return NewFrame(DestinationNamesType, NewDestinationsNames(names...).EncodeBinary(), 0, 0)
}

// NewDestinationsNamesFrame - init new frame.
func NewDestinationsNamesFrame(dn *DestinationsNames) *Frame {
	return NewFrame(DestinationNamesType, dn.EncodeBinary(), 0, 0)
}

// ReadDestinationsNamesFrame - read frame from position pos and return DestinationsNames.
func ReadDestinationsNamesFrame(ctx context.Context, r io.ReaderAt, off int64) (*DestinationsNames, int64, error) {
	h, err := ReadHeader(ctx, r, off)
	if err != nil {
		return nil, off, err
	}
	off += int64(h.SizeOf())

	if h.typeFrame != DestinationNamesType {
		return nil, off, ErrFrameTypeNotMatch
	}

	dn, err := ReadDestinationsNames(ctx, r, off)
	if err != nil {
		return nil, off, err
	}
	off += int64(h.GetSize())

	return dn, off, nil
}

// NewRejectStatusesFrame - init new frame.
func NewRejectStatusesFrame(rs encoding.BinaryMarshaler) (*Frame, error) {
	body, err := rs.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return NewFrame(RejectStatusType, body, 0, 0), nil
}

// ReadFrameRejectStatuses - read frame from position pos and return reject statuses.
//
//nolint:dupl // this is not duplicate
func ReadFrameRejectStatuses(ctx context.Context, r io.ReaderAt, off int64) (RejectStatuses, error) {
	h, err := ReadHeader(ctx, r, off)
	if err != nil {
		return nil, err
	}
	off += int64(h.SizeOf())

	if h.typeFrame != RejectStatusType {
		return nil, ErrFrameTypeNotMatch
	}
	buf := make([]byte, h.size)
	if _, err := r.ReadAt(buf, off); err != nil {
		return nil, err
	}
	var rs RejectStatuses
	if err := rs.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	return rs, nil
}

// NewStatusesFrame - init new frame.
func NewStatusesFrame(ss encoding.BinaryMarshaler) (*Frame, error) {
	blob, err := ss.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return NewFrame(StatusType, blob, 0, 0), nil
}

// ReadFrameStatuses - read frame from position pos and return statuses.
//
//nolint:dupl // this is not duplicate
func ReadFrameStatuses(ctx context.Context, r io.ReaderAt, off int64) (Statuses, error) {
	h, err := ReadHeader(ctx, r, off)
	if err != nil {
		return nil, err
	}
	off += int64(h.SizeOf())

	if h.typeFrame != StatusType {
		return nil, ErrFrameTypeNotMatch
	}
	buf := make([]byte, h.size)
	if _, err := r.ReadAt(buf, off); err != nil {
		return nil, err
	}
	var ss Statuses
	if err := ss.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	return ss, nil
}

// NewSegmentFrame - init new frame.
func NewSegmentFrame(shardID uint16, segmentID uint32, segmentData []byte) *Frame {
	return NewFrame(SegmentType, NewBinaryBody(segmentData).EncodeBinary(), shardID, segmentID)
}

// ReadFrameSegment - read frame from position pos and return BinaryBody.
func ReadFrameSegment(ctx context.Context, r io.ReaderAt, off int64) (*BinaryBody, error) {
	h, err := ReadHeader(ctx, r, off)
	if err != nil {
		return nil, err
	}
	off += int64(h.SizeOf())

	if h.typeFrame != SegmentType {
		return nil, ErrFrameTypeNotMatch
	}

	return ReadBinaryBodyBody(ctx, r, off)
}

// NewSnapshotFrame - init new frame.
func NewSnapshotFrame(shardID uint16, segmentID uint32, data []byte) *Frame {
	return NewFrame(SnapshotType, NewBinaryBody(data).EncodeBinary(), shardID, segmentID)
}

// ReadFrameSnapshot - read frame from position pos and return BinaryBody.
func ReadFrameSnapshot(ctx context.Context, r io.ReaderAt, off int64) (*BinaryBody, error) {
	h, err := ReadHeader(ctx, r, off)
	if err != nil {
		return nil, err
	}
	off += int64(h.SizeOf())

	if h.typeFrame != SnapshotType {
		return nil, ErrFrameTypeNotMatch
	}

	return ReadBinaryBodyBody(ctx, r, off)
}

// headerSize -contant size.
// sum = 1(typeFrame=uint8)+2(shardID=uint16)+4(segmentID=uint32)+4(size=uint32)
const headerSize int = 11

// HeaderFrame - header frame.
type HeaderFrame struct {
	typeFrame TypeFrame
	shardID   uint16
	segmentID uint32
	size      uint32
}

// NewHeaderFrame - init Header with parameter.
func NewHeaderFrame(typeFrame TypeFrame, segmentID, size uint32, shardID uint16) *HeaderFrame {
	return &HeaderFrame{
		typeFrame: typeFrame,
		shardID:   shardID,
		segmentID: segmentID,
		size:      size,
	}
}

// NewHeaderFrameEmpty - init Header for read.
func NewHeaderFrameEmpty() *HeaderFrame {
	return &HeaderFrame{}
}

// GetType - return type frame.
func (h *HeaderFrame) GetType() TypeFrame {
	return h.typeFrame
}

// GetShardID - return shardID.
func (h *HeaderFrame) GetShardID() uint16 {
	return h.shardID
}

// GetSegmentID - return segmentID.
func (h *HeaderFrame) GetSegmentID() uint32 {
	return h.segmentID
}

// GetSize - return size body.
func (h *HeaderFrame) GetSize() uint32 {
	return h.size
}

// SizeOf - size of Header.
func (*HeaderFrame) SizeOf() int {
	return headerSize
}

// FullSize - size of Header + size body.
func (h *HeaderFrame) FullSize() int32 {
	return int32(h.size) + int32(headerSize)
}

// EncodeBinary - encoding to byte.
func (h *HeaderFrame) EncodeBinary() []byte {
	var offset int
	buf := make([]byte, h.SizeOf())

	// write typeFrame and move offset
	buf[0] = byte(h.typeFrame)
	offset += sizeOfTypeFrame

	// write shardID and move offset
	binary.LittleEndian.PutUint16(buf[offset:offset+sizeOfUint16], h.shardID)
	offset += sizeOfUint16

	// write segmentID and move offset
	binary.LittleEndian.PutUint32(buf[offset:offset+sizeOfUint32], h.segmentID)
	offset += sizeOfUint32

	// write size frame and move offset
	binary.LittleEndian.PutUint32(buf[offset:offset+sizeOfUint32], h.size)

	return buf
}

// DecodeBinary - decoding from byte.
func (h *HeaderFrame) DecodeBinary(r io.ReaderAt, pos int64) error {
	// read typeFrame
	buf := make([]byte, sizeOfTypeFrame)
	if _, err := r.ReadAt(buf, pos); err != nil {
		return err
	}
	h.typeFrame = TypeFrame(buf[0])
	pos += sizeOfTypeFrame

	// validate type frame
	if err := h.typeFrame.Validate(); err != nil {
		return ErrUnknownFrameType
	}

	// read shardID
	buf = append(buf[:0], make([]byte, sizeOfUint16)...)
	if _, err := r.ReadAt(buf, pos); err != nil {
		return err
	}
	h.shardID = binary.LittleEndian.Uint16(buf)
	pos += sizeOfUint16

	// read segmentID
	buf = append(buf[:0], make([]byte, sizeOfUint32)...)
	if _, err := r.ReadAt(buf, pos); err != nil {
		return err
	}
	h.segmentID = binary.LittleEndian.Uint32(buf)
	pos += sizeOfUint32

	// read size frame
	if _, err := r.ReadAt(buf, pos); err != nil {
		return err
	}
	h.size = binary.LittleEndian.Uint32(buf)

	return nil
}

// ReadHeader - read and return only header and skip body.
func ReadHeader(ctx context.Context, r io.ReaderAt, off int64) (*HeaderFrame, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	h := NewHeaderFrameEmpty()
	// TODO: use ReadTimeout for file descriptor
	if err := h.DecodeBinary(r, off); err != nil {
		return nil, err
	}

	return h, nil
}

// titleSize -contant size.
// sum = 1(numberOfShards=uint8)+16(blockID=uuid.UUID)
const titleSize int = 17

// Title - title body frame with number of shards and block ID.
type Title struct {
	shardsNumberPower uint8
	blockID           uuid.UUID
}

// NewTitle - init Title.
func NewTitle(snp uint8, blockID uuid.UUID) *Title {
	return &Title{
		shardsNumberPower: snp,
		blockID:           blockID,
	}
}

// NewTitleEmpty - init Title for read.
func NewTitleEmpty() *Title {
	return &Title{}
}

// GetShardsNumberPower - get number of shards.
func (tb *Title) GetShardsNumberPower() uint8 {
	return tb.shardsNumberPower
}

// GetBlockID - get block ID.
func (tb *Title) GetBlockID() uuid.UUID {
	return tb.blockID
}

// SizeOf - get size body.
func (*Title) SizeOf() int {
	return titleSize
}

// EncodeBinary - encoding to byte.
func (tb *Title) EncodeBinary() []byte {
	var offset int
	buf := make([]byte, tb.SizeOf())

	// write numberOfShards and move offset
	buf[0] = tb.shardsNumberPower
	offset += sizeOfUint8

	// write blockID and move offset
	buf = append(buf[:offset], tb.blockID[:]...)

	return buf
}

// decodeBinary - decoding from byte.
func (tb *Title) decodeBinary(r io.ReaderAt, off int64) error {
	// read numberOfShards
	buf := make([]byte, sizeOfUint8)
	if _, err := r.ReadAt(buf, off); err != nil {
		return err
	}
	tb.shardsNumberPower = buf[0]
	off += sizeOfUint8

	// read blockID
	buf = append(buf[:0], make([]byte, sizeOfUUID)...)
	if _, err := r.ReadAt(buf, off); err != nil {
		return err
	}
	return tb.blockID.UnmarshalBinary(buf)
}

// ReadTitle - read body to Title.
func ReadTitle(r io.ReaderAt, off int64) (*Title, error) {
	title := NewTitleEmpty()
	err := title.decodeBinary(r, off)
	if err != nil {
		return nil, err
	}

	return title, nil
}

// BinaryBody - unsent segment/snapshot for save refill.
type BinaryBody struct {
	data []byte
}

// NewBinaryBody - init BinaryBody with data segment/snapshot.
func NewBinaryBody(segmentData []byte) *BinaryBody {
	return &BinaryBody{
		data: segmentData,
	}
}

// NewBinaryBodyEmpty - init SegmentBody for read.
func NewBinaryBodyEmpty() *BinaryBody {
	return &BinaryBody{}
}

// Bytes - get body data in bytes.
func (sb *BinaryBody) Bytes() []byte {
	return sb.data
}

// SizeOf - get size body.
func (sb *BinaryBody) SizeOf() int {
	return sizeOfUint32 + len(sb.data)
}

// EncodeBinary - encoding to byte.
func (sb *BinaryBody) EncodeBinary() []byte {
	var offset int
	buf := make([]byte, sb.SizeOf())

	// write len data and move offset
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(sb.data)))
	offset += sizeOfUint32

	// write data
	buf = append(buf[:offset], sb.data...)

	return buf
}

// decodeBinary - decoding from byte.
func (sb *BinaryBody) decodeBinary(r io.ReaderAt, off int64) error {
	// read len data
	buf := make([]byte, sizeOfUint32)
	if _, err := r.ReadAt(buf, off); err != nil {
		return err
	}
	lenSLice := binary.LittleEndian.Uint32(buf)
	off += sizeOfUint32

	// read data
	sb.data = make([]byte, lenSLice)
	if _, err := r.ReadAt(sb.data, off); err != nil {
		return err
	}

	return nil
}

// Destroy - clear memory, for implements.
func (sb *BinaryBody) Destroy() {
	sb.data = nil
}

// ReadBinaryBodyBody - read body to BinaryBody.
func ReadBinaryBodyBody(ctx context.Context, r io.ReaderAt, off int64) (*BinaryBody, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	binaryBody := NewBinaryBodyEmpty()
	if err := binaryBody.decodeBinary(r, off); err != nil {
		return nil, err
	}

	return binaryBody, nil
}

// stringViewSize -contant size.
// sum = 4(begin=int32)+4(length=int32)
const stringViewSize = 8

// stringView - string view for compact storage.
type stringView struct {
	begin  int32
	length int32
}

// toString - serialize to string.
func (sv stringView) toString(data []byte) string {
	b := data[sv.begin : sv.begin+sv.length]
	return *(*string)(unsafe.Pointer(&b)) //nolint:gosec // this is memory optimisation
}

// NotFoundName - not found name.
const NotFoundName = -1

// DestinationsNames - list of destinations to form the state of writers.
type DestinationsNames struct {
	names []stringView
	data  []byte
}

// NewDestinationsNames - init DestinationsNames with names.
func NewDestinationsNames(names ...string) *DestinationsNames {
	sort.Strings(names)
	d := make([]byte, 0)
	n := make([]stringView, 0, len(names))
	for _, name := range names {
		byteName := []byte(name)
		n = append(
			n,
			stringView{
				begin:  int32(len(d)),
				length: int32(len(byteName)),
			},
		)
		d = append(d, byteName...)
	}

	return &DestinationsNames{
		names: n,
		data:  d,
	}
}

// NewDestinationsNamesEmpty - init DestinationsNames for read.
func NewDestinationsNamesEmpty() *DestinationsNames {
	return &DestinationsNames{}
}

// Len - number of Destinations.
func (dn *DestinationsNames) Len() int {
	return len(dn.names)
}

// Equal - equal current DestinationsNames with new.
func (dn *DestinationsNames) Equal(names ...string) bool {
	if len(names) != len(dn.names) {
		return false
	}

	sort.Strings(names)

	for i, ns := range dn.ToString() {
		if ns != names[i] {
			return false
		}
	}

	return true
}

// ToString - serialize to string.
func (dn *DestinationsNames) ToString() []string {
	namesString := make([]string, 0, len(dn.names))
	for _, nv := range dn.names {
		namesString = append(namesString, nv.toString(dn.data))
	}

	return namesString
}

// IDToString - search name for id.
func (dn *DestinationsNames) IDToString(id int32) string {
	if id > int32(len(dn.names)-1) || id < 0 {
		return ""
	}

	return dn.names[int(id)].toString(dn.data)
}

// StringToID - search id for name.
func (dn *DestinationsNames) StringToID(name string) int32 {
	for i, nameView := range dn.names {
		if name == nameView.toString(dn.data) {
			return int32(i)
		}
	}

	return NotFoundName
}

// Range - calls f sequentially for each key and value present in the stlice struct.
// If f returns false, range stops the iteration.
func (dn *DestinationsNames) Range(fn func(name string, id int) bool) {
	for i, nameView := range dn.names {
		if !fn(nameView.toString(dn.data), i) {
			return
		}
	}
}

// EncodeBinary - encoding to byte.
func (dn *DestinationsNames) EncodeBinary() []byte {
	var offset int
	buf := make(
		[]byte,
		sizeOfUint32+stringViewSize*len(
			dn.names,
		)+sizeOfUint32+len(
			dn.data,
		),
	)

	// write len names and move offset
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(dn.names)))
	offset += sizeOfUint32

	// write names and move offset
	for _, nameView := range dn.names {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(nameView.begin))
		binary.LittleEndian.PutUint32(buf[offset+4:offset+8], uint32(nameView.length))
		offset += stringViewSize
	}

	// write len data and move offset
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(dn.data)))
	offset += sizeOfUint32

	// write data
	buf = append(buf[:offset], dn.data...)

	return buf
}

// decodeBinary - decoding from byte.
func (dn *DestinationsNames) decodeBinary(r io.ReaderAt, off int64) error {
	// read len names
	buf := make([]byte, sizeOfUint32)
	if _, err := r.ReadAt(buf, off); err != nil {
		return err
	}
	lenSLice := binary.LittleEndian.Uint32(buf)
	off += sizeOfUint32

	// read names
	dn.names = make([]stringView, lenSLice)
	buf = append(buf[:0], make([]byte, stringViewSize)...)
	for i := 0; i < int(lenSLice); i++ {
		if _, err := r.ReadAt(buf, off); err != nil {
			return err
		}

		dn.names[i] = stringView{
			begin:  int32(binary.LittleEndian.Uint32(buf[:4])),
			length: int32(binary.LittleEndian.Uint32(buf[4:8])),
		}
		off += stringViewSize
	}

	// read len data
	buf = buf[:sizeOfUint32]
	if _, err := r.ReadAt(buf, off); err != nil {
		return err
	}
	lenSLice = binary.LittleEndian.Uint32(buf)
	off += sizeOfUint32

	// read data
	dn.data = make([]byte, lenSLice)
	if _, err := r.ReadAt(dn.data, off); err != nil {
		return err
	}

	return nil
}

// ReadDestinationsNames - read body to DestinationsNames.
func ReadDestinationsNames(ctx context.Context, r io.ReaderAt, off int64) (*DestinationsNames, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	dn := NewDestinationsNamesEmpty()
	if err := dn.decodeBinary(r, off); err != nil {
		return nil, err
	}

	return dn, nil
}

// Statuses - slice with statuses.
type Statuses []uint32

// NewStatusesEmpty - init empty Statuses.
func NewStatusesEmpty(shardsNumberPower uint8, lenDests int) Statuses {
	// create statuses at least for 1 dest
	if lenDests < 1 {
		lenDests = 1
	}

	return make(Statuses, (1<<shardsNumberPower)*lenDests)
}

// MarshalBinary implements encoding.BinaryMarshaler
func (ss Statuses) MarshalBinary() ([]byte, error) {
	// 4(length slice as.status) + 4(status(uint32))*(number of statuses)
	buf := make([]byte, 0, sizeOfUint32+sizeOfUint32*len(ss))

	// write length statuses on destinations and move offset
	buf = binary.AppendUvarint(buf, uint64(len(ss)))

	for i := range ss {
		buf = binary.AppendUvarint(buf, uint64(atomic.LoadUint32(&ss[i])))
	}

	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (ss *Statuses) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if cap(*ss) < int(length) {
		*ss = make([]uint32, length)
	}
	*ss = (*ss)[:0]
	var val uint64
	for i := 0; i < int(length); i++ {
		val, err = binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		*ss = append(*ss, uint32(val))
	}

	return nil
}

// Equal - equal current Statuses with new.
func (ss Statuses) Equal(newss Statuses) bool {
	if len(ss) != len(newss) {
		return false
	}

	for i := range ss {
		if ss[i] != atomic.LoadUint32(&newss[i]) {
			return false
		}
	}

	return true
}

// Reset - reset all statuses.
func (ss Statuses) Reset() {
	for i := range ss {
		ss[i] = math.MaxUint32
	}
}

// newShardStatuses - init empty shards status.
func newShardStatuses(shardsNumber int) []uint32 {
	status := make([]uint32, shardsNumber)
	for i := range status {
		status[i] = math.MaxUint32
	}
	return status
}

// Reject - rejected segment struct.
type Reject struct {
	NameID  uint32
	Segment uint32
	ShardID uint16
}

// RejectStatuses - RejectStatuses - slice with rejected segment struct.
type RejectStatuses []Reject

// MarshalBinary implements encoding.MarshalBinary
func (rjss RejectStatuses) MarshalBinary() ([]byte, error) {
	// 4(length slice status) + (4(NameID(uint32))+4(Segment(uint32))+2(ShardID(uint16)))*(number of statuses)
	buf := make([]byte, 0, sizeOfUint32+((sizeOfUint32+sizeOfUint32+sizeOfUint16)*len(rjss)))

	// write length statuses on destinations and move offset
	buf = binary.AppendUvarint(buf, uint64(len(rjss)))

	for i := range rjss {
		buf = binary.AppendUvarint(buf, uint64(rjss[i].NameID))
		buf = binary.AppendUvarint(buf, uint64(rjss[i].Segment))
		buf = binary.AppendUvarint(buf, uint64(rjss[i].ShardID))
	}

	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (rjss *RejectStatuses) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if cap(*rjss) < int(length) {
		*rjss = make(RejectStatuses, length)
	}
	*rjss = (*rjss)[:0]
	var nameID, segment, shardID uint64
	for i := 0; i < int(length); i++ {
		nameID, err = binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		segment, err = binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		shardID, err = binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		*rjss = append(*rjss, Reject{
			NameID:  uint32(nameID),
			Segment: uint32(segment),
			ShardID: uint16(shardID),
		})
	}

	return nil
}

// Rejects - reject statuses with rotates for concurrency.
type Rejects struct {
	mutex   *sync.Mutex
	active  []Reject
	reserve []Reject
}

// the number is chosen with a finger to the sky
const defaultCapacityRejectStatuses = 512

// NewRejects - init new Rejects.
func NewRejects() *Rejects {
	return &Rejects{
		mutex:   new(sync.Mutex),
		active:  make([]Reject, 0, defaultCapacityRejectStatuses),
		reserve: make([]Reject, 0, defaultCapacityRejectStatuses),
	}
}

// Rotate - rotate statuses.
func (rjs *Rejects) Rotate() RejectStatuses {
	rjs.mutex.Lock()
	defer rjs.mutex.Unlock()

	rjs.active, rjs.reserve = rjs.reserve[:0], rjs.active
	return rjs.reserve
}

// Unrotate refill given rejects back.
func (rjs *Rejects) Unrotate(rejects RejectStatuses) {
	rjs.mutex.Lock()
	defer rjs.mutex.Unlock()

	rjs.active, rjs.reserve = append(rejects, rjs.active...), rjs.active[:0]
}

// Add - add reject.
func (rjs *Rejects) Add(nameID, segment uint32, shardID uint16) {
	rjs.mutex.Lock()
	defer rjs.mutex.Unlock()

	rjs.active = append(rjs.active, Reject{nameID, segment, shardID})
}

// AckStatus keeps destinations list with last acked segments numbers per shard
type AckStatus struct {
	names   *DestinationsNames
	status  Statuses
	rejects *Rejects
	rwmx    *sync.RWMutex
}

// NewAckStatus is a constructor
func NewAckStatus(destinations []string, shardsNumberPower uint8) *AckStatus {
	dn := NewDestinationsNames(destinations...)
	shardsNumber := 1 << shardsNumberPower
	status := make(Statuses, len(destinations)*shardsNumber)

	dn.Range(func(_ string, id int) bool {
		for i := 0; i < shardsNumber; i++ {
			status[id*shardsNumber+i] = math.MaxUint32
		}
		return true
	})

	return &AckStatus{
		names:   dn,
		status:  status,
		rejects: NewRejects(),
		rwmx:    new(sync.RWMutex),
	}
}

// NewAckStatusEmpty - init empty AckStatus.
func NewAckStatusEmpty(shardsNumberPower uint8) *AckStatus {
	return &AckStatus{
		names:   NewDestinationsNamesEmpty(),
		status:  NewStatusesEmpty(shardsNumberPower, 0),
		rejects: NewRejects(),
		rwmx:    new(sync.RWMutex),
	}
}

// Destinations returns number of destinations
func (as *AckStatus) Destinations() int {
	return as.names.Len()
}

// Shards returns number of shards
func (as *AckStatus) Shards() int {
	return len(as.status) / as.names.Len()
}

// Ack increment status by destination and shard if segment is next for current value
func (as *AckStatus) Ack(key common.SegmentKey, dest string) {
	id := as.names.StringToID(dest)
	if id == NotFoundName {
		panic(fmt.Sprintf(
			"AckStatus: ack unexpected destination name %s",
			dest,
		))
	}

	as.rwmx.RLock()
	old := atomic.SwapUint32(
		&as.status[id*int32(as.Shards())+int32(key.ShardID)],
		key.Segment,
	)
	as.rwmx.RUnlock()
	if old != math.MaxUint32 && old > key.Segment {
		panic(fmt.Sprintf(
			"AckStatus: ack segment %d less old %d",
			key.Segment,
			old,
		))
	}
}

// Reject - add rejected segment.
func (as *AckStatus) Reject(segKey common.SegmentKey, dest string) {
	id := as.names.StringToID(dest)
	if id == NotFoundName {
		panic(fmt.Sprintf(
			"AckStatus: ack unexpected destination name %s",
			dest,
		))
	}

	as.rwmx.RLock()
	as.rejects.Add(uint32(id), segKey.Segment, segKey.ShardID)
	as.rwmx.RUnlock()
}

// Index returns destination index by name
func (as *AckStatus) Index(dest string) (int, bool) {
	id := as.names.StringToID(dest)
	if id == NotFoundName {
		return -1, false
	}

	return int(id), true
}

// Last return last ack segment by shard and destination
func (as *AckStatus) Last(shardID uint16, dest string) uint32 {
	id := as.names.StringToID(dest)
	if id == NotFoundName {
		return 0
	}

	return atomic.LoadUint32(&as.status[id*int32(as.Shards())+int32(shardID)])
}

// IsAck returns true if segment ack by all destinations
func (as *AckStatus) IsAck(key common.SegmentKey) bool {
	for id := 0; id < as.Destinations(); id++ {
		if atomic.LoadUint32(&as.status[id*as.Shards()+int(key.ShardID)])+1 <= key.Segment {
			return false
		}
	}

	return true
}

// GetNames - return DestinationsNames.
func (as *AckStatus) GetNames() *DestinationsNames {
	return as.names
}

// GetCopyAckStatuses - retrun copy statuses.
func (as *AckStatus) GetCopyAckStatuses() Statuses {
	newss := make(Statuses, len(as.status))
	copy(newss, as.status)
	return newss
}

// RotateRejects - rotate rejects statuses..
func (as *AckStatus) RotateRejects() RejectStatuses {
	return as.rejects.Rotate()
}

// UnrotateRejects - unrotate rejects statuses..
func (as *AckStatus) UnrotateRejects(rejects RejectStatuses) {
	as.rejects.Unrotate(rejects)
}

// ReadDestinationsNames - read body frame to DestinationsNames.
func (as *AckStatus) ReadDestinationsNames(r io.ReaderAt, off int64) error {
	err := as.names.decodeBinary(r, off)
	if err != nil {
		return err
	}

	as.status = make(Statuses, len(as.names.names)*len(as.status))
	for i := range as.status {
		as.status[i] = math.MaxUint32
	}

	return nil
}

// Lock - locks rw for writing.
func (as *AckStatus) Lock() {
	as.rwmx.Lock()
}

// Unlock - unlocks rw for writing.
func (as *AckStatus) Unlock() {
	as.rwmx.Unlock()
}
