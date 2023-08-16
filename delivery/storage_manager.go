package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/util"
	"github.com/prometheus/client_golang/prometheus"
)

const posNotFound int64 = -1

// MarkupKey - key for search position.
type MarkupKey struct {
	typeFrame frames.TypeFrame
	common.SegmentKey
}

// StorageManager - manager for file refill. Contains file markup for quick access to data.
type StorageManager struct {
	// title frame
	title *frames.Title
	// marking positions of Segments and Snapshots
	markupMap map[MarkupKey]int64
	// last status of writers
	ackStatus *AckStatus
	// last statuses write, need write frame if have differents
	statuses frames.Statuses
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
		case frames.ErrUnknownFrameType:
			if err = sm.storage.Truncate(sm.lastWriteOffset); err != nil {
				return nil, err
			}
			ok = true
		default:
			return nil, err
		}
	}
	if !ok {
		sm.title = frames.NewTitle(shardsNumberPower, blockID)
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
		typeFrame:  frames.SnapshotType,
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
		typeFrame:  frames.SnapshotType,
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
	snapshotData, err := frames.ReadFrameSnapshot(ctx, util.NewOffsetReader(sm.storage, pos))
	if err != nil {
		return nil, err
	}

	sm.readBytes.Observe(float64(snapshotData.Size()))

	return snapshotData, nil
}

// getSegmentPosition - return position in storage.
func (sm *StorageManager) getSegmentPosition(segKey common.SegmentKey) int64 {
	mk := MarkupKey{
		typeFrame:  frames.SegmentType,
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
		typeFrame:  frames.SegmentType,
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
	segmentData, err := frames.ReadFrameSegment(ctx, util.NewOffsetReader(sm.storage, pos))
	if err != nil {
		return nil, err
	}

	sm.readBytes.Observe(float64(segmentData.Size()))

	return segmentData, nil
}

// GetAckStatus - return last AckStatus.
func (sm *StorageManager) GetAckStatus() *AckStatus {
	return sm.ackStatus
}

// restoreFromBody - restore from body frame.
func (sm *StorageManager) restoreFromBody(ctx context.Context, h *frames.Header, off int64, size int) error {
	// TODO restore bad frame
	switch h.GetType() {
	case frames.TitleType:
		return sm.restoreTitle(ctx, off, size)
	case frames.DestinationNamesType:
		return sm.restoreDestinationsNames(ctx, off, size)
	case frames.SnapshotType:
		return sm.restoreSnapshot(h, off)
	case frames.SegmentType:
		return sm.restoreSegment(h, off)
	case frames.StatusType:
		return sm.restoreStatuses(h, off)
	case frames.RejectStatusType:
		sm.hasRejects.Store(true)
		// skip reject statuses
		return nil
	}

	return nil
}

// restore - restore title from frame.
func (sm *StorageManager) restoreTitle(ctx context.Context, off int64, size int) error {
	var err error
	sm.title, err = frames.ReadAtTitle(ctx, sm.storage, off, size)
	if err != nil {
		return err
	}

	// init lastWriteSegment for future reference and not to panic
	sm.lastWriteSegment = newShardStatuses(1 << sm.title.GetShardsNumberPower())

	return nil
}

// restore - restore Destinations Names from frame.
func (sm *StorageManager) restoreDestinationsNames(ctx context.Context, off int64, size int) error {
	// init lastWriteSegment for future reference and not to panic
	// init with shardsNumberPower to init statuses for null values
	sm.ackStatus = NewAckStatusEmpty(sm.title.GetShardsNumberPower())
	return sm.ackStatus.ReadDestinationsNames(ctx, sm.storage, off, size)
}

// checkRestoredServiceData - check restored service data(title, destinations names),
// these data are required to be restored, without them you cant read the rest
func (sm *StorageManager) checkRestoredServiceData() bool {
	return sm.title != nil && sm.ackStatus != nil
}

// restore - restore Snapshot from frame.
func (sm *StorageManager) restoreSnapshot(h *frames.Header, off int64) error {
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
func (sm *StorageManager) restoreSegment(h *frames.Header, off int64) error {
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
func (sm *StorageManager) restoreStatuses(h *frames.Header, off int64) error {
	if !sm.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	buf := make([]byte, h.GetSize())
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
		h, err := frames.ReadHeader(ctx, util.NewOffsetReader(sm.storage, off))
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, err
		}
		off += int64(h.SizeOf())

		// restore data from body
		err = sm.restoreFromBody(ctx, h, off, int(h.GetSize()))
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
	fe, err := frames.NewTitleFrame(sm.title.GetShardsNumberPower(), sm.BlockID())
	if err != nil {
		return err
	}
	n, err := fe.WriteTo(sm.storage.Writer(ctx, sm.lastWriteOffset))
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
	fe, err := frames.NewDestinationsNamesFrameWithMsg(protocolVersion, sm.ackStatus.names)
	if err != nil {
		return err
	}

	n, err := fe.WriteTo(sm.storage.Writer(ctx, sm.lastWriteOffset))
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
	fe := frames.NewWriteFrame(protocolVersion, frames.SegmentType, key.ShardID, key.Segment, seg)
	n, err := fe.WriteTo(sm.storage.Writer(ctx, sm.lastWriteOffset))
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

	fe := frames.NewWriteFrame(protocolVersion, frames.SnapshotType, segKey.ShardID, segKey.Segment, snapshot)
	n, err := fe.WriteTo(sm.storage.Writer(ctx, sm.lastWriteOffset))
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
		fe, err := frames.NewRejectStatusesFrame(rejects)
		if err != nil {
			sm.ackStatus.UnrotateRejects(rejects)
			return err
		}
		n, err := fe.WriteTo(sm.storage.Writer(ctx, sm.lastWriteOffset))
		sm.writeBytes.Observe(float64(n))
		if err != nil {
			sm.ackStatus.UnrotateRejects(rejects)
			return err
		}
		sm.moveLastWriteOffset(int64(n))
	}
	if !sm.statuses.Equal(ss) {
		fe, err := frames.NewStatusesFrame(ss)
		if err != nil {
			// TODO: unrotate
			return err
		}
		n, err := fe.WriteTo(sm.storage.Writer(ctx, sm.lastWriteOffset))
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
	sm.lastWriteSegment = newShardStatuses(1 << sm.title.GetShardsNumberPower())
	sm.markupMap = make(map[MarkupKey]int64)
	sm.statuses.Reset()
	sm.setLastWriteOffset(0)

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

// newShardStatuses - init empty shards status.
func newShardStatuses(shardsNumber int) []uint32 {
	status := make([]uint32, shardsNumber)
	for i := range status {
		status[i] = math.MaxUint32
	}
	return status
}

// Rejects - reject statuses with rotates for concurrency.
type Rejects struct {
	mutex   *sync.Mutex
	active  []frames.Reject
	reserve []frames.Reject
}

// NewRejects - init new Rejects.
func NewRejects() *Rejects {
	return &Rejects{
		mutex:   new(sync.Mutex),
		active:  frames.NewRejectStatusesEmpty(),
		reserve: frames.NewRejectStatusesEmpty(),
	}
}

// Rotate - rotate statuses.
func (rjs *Rejects) Rotate() frames.RejectStatuses {
	rjs.mutex.Lock()
	defer rjs.mutex.Unlock()

	rjs.active, rjs.reserve = rjs.reserve[:0], rjs.active
	return rjs.reserve
}

// Unrotate refill given rejects back.
func (rjs *Rejects) Unrotate(rejects frames.RejectStatuses) {
	rjs.mutex.Lock()
	defer rjs.mutex.Unlock()

	rjs.active, rjs.reserve = append(rejects, rjs.active...), rjs.active[:0]
}

// Add - add reject.
func (rjs *Rejects) Add(nameID, segment uint32, shardID uint16) {
	rjs.mutex.Lock()
	defer rjs.mutex.Unlock()

	rjs.active = append(
		rjs.active,
		frames.Reject{
			NameID:  nameID,
			Segment: segment,
			ShardID: shardID,
		},
	)
}

// AckStatus keeps destinations list with last acked segments numbers per shard
type AckStatus struct {
	names   *frames.DestinationsNames
	status  frames.Statuses
	rejects *Rejects
	rwmx    *sync.RWMutex
}

// NewAckStatus is a constructor
func NewAckStatus(destinations []string, shardsNumberPower uint8) *AckStatus {
	dn := frames.NewDestinationsNames(destinations...)
	shardsNumber := 1 << shardsNumberPower
	status := frames.NewStatusesEmpty(shardsNumberPower, len(destinations))

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
		names:   frames.NewDestinationsNamesEmpty(),
		status:  frames.NewStatusesEmpty(shardsNumberPower, 0),
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
	if id == frames.NotFoundName {
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
	if id == frames.NotFoundName {
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
	if id == frames.NotFoundName {
		return -1, false
	}

	return int(id), true
}

// Last return last ack segment by shard and destination
func (as *AckStatus) Last(shardID uint16, dest string) uint32 {
	id := as.names.StringToID(dest)
	if id == frames.NotFoundName {
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
func (as *AckStatus) GetNames() *frames.DestinationsNames {
	return as.names
}

// GetCopyAckStatuses - retrun copy statuses.
func (as *AckStatus) GetCopyAckStatuses() frames.Statuses {
	newss := make(frames.Statuses, len(as.status))
	copy(newss, as.status)
	return newss
}

// RotateRejects - rotate rejects statuses..
func (as *AckStatus) RotateRejects() frames.RejectStatuses {
	return as.rejects.Rotate()
}

// UnrotateRejects - unrotate rejects statuses..
func (as *AckStatus) UnrotateRejects(rejects frames.RejectStatuses) {
	as.rejects.Unrotate(rejects)
}

// ReadDestinationsNames - read body frame to DestinationsNames.
func (as *AckStatus) ReadDestinationsNames(ctx context.Context, r io.ReaderAt, off int64, size int) error {
	if err := as.names.ReadAt(ctx, r, off, size); err != nil {
		return err
	}

	as.status = make(frames.Statuses, as.names.Len()*len(as.status))
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
