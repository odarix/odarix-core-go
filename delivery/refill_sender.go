package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/util"
	"github.com/prometheus/client_golang/prometheus"
)

// errRefillLimitExceeded - error if refill limit exceeded.
var (
	errRefillLimitExceeded = errors.New("refill limit exceeded")
	// ErrCorruptedFile - error if the file is corrupted.
	ErrCorruptedFile = errors.New("corrupted file")
)

const (
	// DefaultScanInterval - default scan refill file interval.
	DefaultScanInterval = 300 * time.Second
	// DefaultMaxRefillSize - default max refill files size in bytes.
	DefaultMaxRefillSize = 300 << 20 // 300mb
)

// RefillSendManagerConfig - config for RefillSendManagerConfig.
//
// Dir - current refill dir.
// ScanInterval - refill file scan interval.
// MaxRefillSize - max refill files size in bytes.
type RefillSendManagerConfig struct {
	ScanInterval  time.Duration `validate:"required"`
	MaxRefillSize int64         `validate:"required"`
}

// DefaultRefillSendManagerConfig - generate default RefillSendManagerConfig.
func DefaultRefillSendManagerConfig() RefillSendManagerConfig {
	return RefillSendManagerConfig{
		ScanInterval:  DefaultScanInterval,
		MaxRefillSize: DefaultMaxRefillSize,
	}
}

// RefillSendManager - manager  for send refill to server.
type RefillSendManager struct {
	rsmCfg       RefillSendManagerConfig
	dir          string
	dialers      map[string]Dialer
	errorHandler ErrorHandler
	clock        clockwork.Clock
	stop         chan struct{}
	done         chan struct{}
	// stat
	registerer      prometheus.Registerer
	fileSize        prometheus.Gauge
	numberFiles     prometheus.Gauge
	deletedFileSize *prometheus.HistogramVec
	errors          *prometheus.CounterVec
}

// NewRefillSendManager - init new RefillSendManger.
func NewRefillSendManager(
	rsmCfg RefillSendManagerConfig,
	workingDir string,
	dialers []Dialer,
	errorHandler ErrorHandler,
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) (*RefillSendManager, error) {
	if len(dialers) == 0 {
		return nil, ErrDestinationsRequired
	}
	dialersMap := make(map[string]Dialer, len(dialers))
	for _, dialer := range dialers {
		dialersMap[dialer.String()] = dialer
	}
	factory := util.NewUnconflictRegisterer(registerer)
	return &RefillSendManager{
		rsmCfg:       rsmCfg,
		dir:          filepath.Join(workingDir, RefillDir),
		dialers:      dialersMap,
		errorHandler: errorHandler,
		clock:        clock,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		registerer:   registerer,
		fileSize: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "odarix_core_delivery_refill_send_manager_file_bytes",
				Help: "Total files size of bytes.",
			},
		),
		numberFiles: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "odarix_core_delivery_refill_send_manager_files_count",
				Help: "Total number of files.",
			},
		),
		deletedFileSize: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_refill_send_manager_deleted_file_bytes",
				Help:    "Deleted file sizes of bytes.",
				Buckets: prometheus.ExponentialBucketsRange(50<<20, 1<<30, 10),
			},
			[]string{"cause"},
		),
		errors: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "odarix_core_delivery_refill_send_manager_errors",
				Help: "Total number errors.",
			},
			[]string{"place"},
		),
	}, nil
}

// Run - main loop for scan refill and sending to destinations.
func (rsm *RefillSendManager) Run(ctx context.Context) {
	if err := rsm.checkTmpRefill(); err != nil {
		rsm.errorHandler("fail check and rename tmp refill file", err)
	}
	loopTicker := rsm.clock.NewTicker(rsm.rsmCfg.ScanInterval)
	defer loopTicker.Stop()
	defer close(rsm.done)
	for {
		select {
		case <-loopTicker.Chan():
			// scan the folder for files to send and process these files
			if err := rsm.processing(ctx); err != nil {
				if errors.Is(err, ErrShutdown) {
					return
				}
				rsm.errors.With(prometheus.Labels{"place": "processing"}).Inc()
				rsm.errorHandler("fail scan and send loop", err)
				continue
			}

			// delete old files if the size exceeds the maximum
			if err := rsm.clearing(); err != nil {
				rsm.errors.With(prometheus.Labels{"place": "clearing"}).Inc()
				rsm.errorHandler("fail clearing", err)
				continue
			}
		case <-rsm.stop:
			return
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), ErrShutdown) {
				rsm.errorHandler("scan and send loop context canceled", context.Cause(ctx))
			}
			return
		}
	}
}

// checkTmpRefill - checks refile files with temporary extensions and renames them to stateful.
func (rsm *RefillSendManager) checkTmpRefill() error {
	files, err := os.ReadDir(rsm.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), refillIntermediateFileExtension) {
			continue
		}

		oldName := file.Name()
		newName := oldName[:len(oldName)-len(refillIntermediateFileExtension)] + refillFileExtension
		if err := os.Rename(
			filepath.Join(rsm.dir, oldName),
			filepath.Join(rsm.dir, newName),
		); err != nil {
			return err
		}
	}

	return nil
}

// processing - scan the folder for files to send and process these files.
func (rsm *RefillSendManager) processing(ctx context.Context) error {
	refillFiles, err := rsm.scanFolder()
	if err != nil {
		return fmt.Errorf("fail scan folder: %s: %w", rsm.dir, err)
	}

	for _, fileInfo := range refillFiles {
		select {
		case <-rsm.stop:
			return ErrShutdown
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		// read, prepare to send, send
		if err = rsm.fileProcessing(ctx, fileInfo); err != nil {
			rsm.errorHandler(fmt.Sprintf("fail send file: %s", fileInfo.Name()), err)
			if IsPermanent(err) {
				// delete bad refill file
				_ = os.Remove(filepath.Join(rsm.dir, fileInfo.Name()))
				rsm.deletedFileSize.With(prometheus.Labels{"cause": "error"}).Observe(float64(fileInfo.Size()))
			}
		}
	}

	return nil
}

// scanFolder - check folder for refill files.
func (rsm *RefillSendManager) scanFolder() ([]fs.FileInfo, error) {
	files, err := os.ReadDir(rsm.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	isCompletedRefill := func(entry os.DirEntry) bool {
		return !entry.IsDir() &&
			strings.HasSuffix(entry.Name(), refillFileExtension) &&
			!strings.HasPrefix(entry.Name(), RefillFileName)
	}

	var fullFileSize int64
	refillFiles := make([]fs.FileInfo, 0, len(files))
	for _, file := range files {
		if isCompletedRefill(file) {
			fileInfo, err := file.Info()
			if err != nil {
				return nil, err
			}
			fullFileSize += fileInfo.Size()
			refillFiles = append(refillFiles, fileInfo)
		}
	}
	rsm.fileSize.Set(float64(fullFileSize))
	rsm.numberFiles.Set(float64(len(refillFiles)))
	return refillFiles, nil
}

// processingFile - read and preparing data and sending to destinations.
//
//revive:disable:cognitive-complexity // because there is nowhere to go
func (rsm *RefillSendManager) fileProcessing(ctx context.Context, fileInfo fs.FileInfo) error {
	reader, err := NewRefillReader(
		ctx,
		FileStorageConfig{
			Dir:      rsm.dir,
			FileName: strings.TrimSuffix(fileInfo.Name(), refillFileExtension),
		},
		rsm.errorHandler,
	)
	if err != nil {
		return err
	}
	defer reader.Close()
	// indicator that all goroutines completed successfully
	withError := new(atomic.Bool)
	wg := new(sync.WaitGroup)
	// grouping data by destinations and start send with goroutines
	groupingByDestinations := reader.MakeSendMap()
	groupingByDestinations.Range(func(dname string, shardID int, shardData []uint32) bool {
		dialer, ok := rsm.dialers[dname]
		if !ok {
			// if the dialer is not found, then we skip the data
			return true
		}

		wg.Add(1)
		go func(dr Dialer, id int, data []uint32) {
			defer wg.Done()
			if err := rsm.send(ctx, dr, reader, uint16(id), data, rsm.registerer); err != nil {
				if !IsPermanent(err) {
					withError.Store(true)
				}

				rsm.errorHandler("fail send", err)
				return
			}
		}(dialer, shardID, shardData)

		return true
	})

	wg.Wait()

	// if any of the deliveries failed, file should be saved for next attempt
	if withError.Load() {
		return nil
	}

	rsm.deletedFileSize.With(prometheus.Labels{"cause": "delivered"}).Observe(float64(fileInfo.Size()))
	// if file has been delivered to all known destinations, it doesn't required anymore and should be deleted
	return reader.DeleteFile()
}

// send - sending to destinations.
func (rsm *RefillSendManager) send(
	ctx context.Context,
	dialer Dialer,
	source *RefillReader,
	shardID uint16,
	data []uint32,
	registerer prometheus.Registerer,
) error {
	rs := NewRefillSender(
		dialer,
		source,
		rsm.errorHandler,
		shardID,
		data,
		registerer,
	)

	return rs.Send(ctx)
}

// clearing - delete old refills if the maximum allowable size is exceeded.
func (rsm *RefillSendManager) clearing() error {
	refillFiles, err := rsm.scanFolder()
	if err != nil {
		return err
	}

	if len(refillFiles) == 0 {
		return nil
	}

	var fullFileSize int64
	for _, fileInfo := range refillFiles {
		fullFileSize += fileInfo.Size()
	}

	for len(refillFiles) > 0 && fullFileSize > rsm.rsmCfg.MaxRefillSize {
		rsm.errorHandler(fmt.Sprintf("remove file: %s", refillFiles[0].Name()), errRefillLimitExceeded)
		rsm.deletedFileSize.With(prometheus.Labels{"cause": "limit"}).Observe(float64(refillFiles[0].Size()))
		if err = os.Remove(filepath.Join(rsm.dir, refillFiles[0].Name())); err != nil {
			rsm.errorHandler(fmt.Sprintf("failed to delete file: %s", refillFiles[0].Name()), err)
			refillFiles = refillFiles[1:]
			continue
		}
		fullFileSize -= refillFiles[0].Size()
		refillFiles = refillFiles[1:]
	}
	rsm.numberFiles.Set(float64(len(refillFiles)))
	rsm.fileSize.Set(float64(fullFileSize))
	return nil
}

// Shutdown - await while ScanAndSend stop send.
func (rsm *RefillSendManager) Shutdown(ctx context.Context) error {
	close(rsm.stop)

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-rsm.done:
		return nil
	}
}

// PreparedData - prepared data for send.
type PreparedData struct {
	Value     *MarkupValue
	SegmentID uint32
	MsgType   frames.TypeFrame
}

// RefillSender - sender refill to server.
type RefillSender struct {
	dialer          Dialer
	source          *RefillReader
	transport       Transport
	dataToSend      []uint32
	lastSendSegment uint32
	shardID         uint16
	done            chan struct{}
	errs            chan error
	errorHandler    ErrorHandler
	// stat
	successfulDelivery prometheus.Counter
	needSend           *prometheus.HistogramVec
}

// NewRefillSender - init new RefillSender.
func NewRefillSender(
	dialer Dialer,
	source *RefillReader,
	errorHandler ErrorHandler,
	shardID uint16,
	data []uint32,
	registerer prometheus.Registerer,
) *RefillSender {
	factory := util.NewUnconflictRegisterer(registerer)
	return &RefillSender{
		dialer:          dialer,
		source:          source,
		dataToSend:      data,
		lastSendSegment: math.MaxUint32,
		shardID:         shardID,
		done:            make(chan struct{}),
		errs:            make(chan error, 1),
		errorHandler:    errorHandler,
		successfulDelivery: factory.NewCounter(
			prometheus.CounterOpts{
				Name:        "odarix_core_delivery_refill_sender_successful_delivery",
				Help:        "Total successful delivery.",
				ConstLabels: prometheus.Labels{"host": dialer.String()},
			},
		),
		needSend: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "odarix_core_delivery_refill_sender_need_send_bytes",
				Help:        "Amount of data to send.",
				ConstLabels: prometheus.Labels{"host": dialer.String()},
				Buckets:     prometheus.ExponentialBucketsRange(1024, 120<<20, 10),
			},
			[]string{"type"},
		),
	}
}

// String - implements fmt.Stringer interface.
func (rs *RefillSender) String() string {
	return rs.dialer.String()
}

// Send - create a connection, prepare data for sending and send.
func (rs *RefillSender) Send(ctx context.Context) error {
	if err := rs.dial(ctx); err != nil {
		return fmt.Errorf("%s: fail to dial: %w", rs, err)
	}

	pData, err := rs.collectedData()
	if err != nil {
		return fmt.Errorf("%s: fail prepared data: %w", rs, err)
	}

	if err = rs.transport.Send(ctx, rs.makeRefillFrame(pData)); err != nil {
		return fmt.Errorf("%s: fail send frame refill: %w", rs, err)
	}

	if err = rs.sendData(ctx, pData); err != nil {
		return fmt.Errorf("%s: fail send refill data: %w", rs, err)
	}

	select {
	case <-ctx.Done():
		if errors.Is(context.Cause(ctx), ErrShutdown) {
			return rs.transport.Close()
		}
		return errors.Join(context.Cause(ctx), rs.transport.Close())
	case <-rs.done:
		return rs.transport.Close()
	case err := <-rs.errs:
		return errors.Join(err, rs.transport.Close())
	}
}

func (rs *RefillSender) makeRefillFrame(pData []PreparedData) *frames.WriteFrame {
	data := make([]frames.MessageData, 0, len(pData))
	for _, p := range pData {
		data = append(data, frames.MessageData{
			ID:      p.SegmentID,
			Size:    p.Value.size,
			Typemsg: p.MsgType,
		})
	}
	msg := frames.NewRefillMsg(data)
	return frames.NewWriteFrame(protocolVersion, frames.RefillType, rs.shardID, 0, msg)
}

// dial - dial and set respose parameter.
func (rs *RefillSender) dial(ctx context.Context) (err error) {
	ctxDial, cancel := context.WithTimeout(ctx, 10*time.Second)
	rs.transport, err = rs.dialer.Dial(ctxDial, rs.source.BlockID().String(), rs.shardID)
	cancel()
	if err != nil {
		return err
	}

	rs.transport.OnAck(func(_ uint32) {
		if errWrite := rs.source.WriteRefillShardEOF(ctx, rs.String(), rs.shardID); errWrite != nil {
			rs.errorHandler(fmt.Sprintf("%s: fail to write shard EOF", rs), errWrite)
		}
		rs.safeDone()
		rs.successfulDelivery.Inc()
	})

	rs.transport.OnReject(func(_ uint32) {
		// TODO add reason for rejection
		rs.safeError(errors.New("refill rejected"))
	})

	rs.transport.OnReadError(func(err error) {
		rs.safeError(err)
	})

	rs.transport.Listen(ctx)

	return nil
}

// safeDone - set safely done.
func (rs *RefillSender) safeDone() {
	select {
	case <-rs.done:
	default:
		close(rs.done)
	}
}

// safeError - handle the error safely.
func (rs *RefillSender) safeError(err error) {
	select {
	case rs.errs <- err:
	default:
		rs.errorHandler(
			fmt.Sprintf("%s: error not handled", rs),
			err,
		)
	}
}

// collectedData - collects all the necessary position data for sending.
//
//revive:disable-next-line:cyclomatic  but readable
func (rs *RefillSender) collectedData() ([]PreparedData, error) {
	pData := make([]PreparedData, 0, len(rs.dataToSend))

	for _, segment := range rs.dataToSend {
		key := common.SegmentKey{ShardID: rs.shardID, Segment: segment}
		mval := rs.source.SegmentPosition(key)
		if mval == nil {
			return nil, SegmentNotFoundInRefill(key)
		}

		pData = append(
			pData,
			PreparedData{
				MsgType:   frames.SegmentType,
				SegmentID: segment,
				Value:     mval,
			},
		)
		rs.needSend.With(prometheus.Labels{"type": "put"}).Observe(float64(mval.size))
	}

	return pData, nil
}

// sendData - sending prepared data.
func (rs *RefillSender) sendData(ctx context.Context, pData []PreparedData) error {
	for _, data := range pData {
		if data.MsgType != frames.SegmentType {
			continue
		}
		if err := rs.sendSegment(ctx, data.SegmentID, data.Value); err != nil {
			return fmt.Errorf("%s: fail send segment: %w", rs, err)
		}
	}

	return nil
}

// sendSegment - restore and send segment.
func (rs *RefillSender) sendSegment(ctx context.Context, segmentID uint32, mval *MarkupValue) error {
	segment, err := rs.source.Segment(ctx, mval)
	if err != nil {
		return fmt.Errorf("%s: fail get segment: %w", rs, err)
	}
	frame := frames.NewWriteFrame(protocolVersion, frames.SegmentType, rs.shardID, segmentID, segment)
	return rs.transport.Send(ctx, frame)
}

// MarkupValue - value for markup map.
type MarkupValue struct {
	pos  int64
	size uint32
}

// SendMap - map for send grouping by destinations.
type SendMap struct {
	m      map[string][][]uint32
	shards int
}

// NewSendMap - init new SendMap.
func NewSendMap(shards int) *SendMap {
	return &SendMap{
		m:      make(map[string][][]uint32),
		shards: shards,
	}
}

// Append - append to map segmentID by shardID for dname.
func (sm *SendMap) Append(dname string, shardID uint16, segmentID uint32) {
	if _, ok := sm.m[dname]; !ok {
		sm.m[dname] = make([][]uint32, sm.shards)
	}
	list := sm.m[dname][shardID]
	if n := len(list); n != 0 {
		if list[n-1] == segmentID {
			return
		}
		if list[n-1] > segmentID {
			// It is possible that AckStatusFrame is corrupted or lost.
			// It is exactly reason for this condition is true.
			// We choose send all segments with unknown state (their ack status is lost).
			// Rejected segments should be added too as unacked.
			list = list[:sort.Search(len(list), func(i int) bool { return list[i] >= segmentID })]
		}
	}

	sm.m[dname][shardID] = append(list, segmentID)
}

// Remove destination-shard data
func (sm *SendMap) Remove(dname string, shardID uint16) {
	sm.m[dname][shardID] = nil
}

// Range - calls f sequentially for each dname, shardID and shardData present in the map.
// If f returns false, range stops the iteration.
func (sm *SendMap) Range(fn func(dname string, shardID int, shardData []uint32) bool) {
	for d := range sm.m {
		for s := range sm.m[d] {
			if len(sm.m[d][s]) == 0 {
				continue
			}
			if !fn(d, s, sm.m[d][s]) {
				break
			}
		}
	}
}

// RefillReader - reader for refill files.
type RefillReader struct {
	// title frame
	title *frames.Title
	// last status of writers
	ackStatus *AckStatus
	// reader/writer for restore file
	storage *FileStorage
	// mutex for parallel writing
	mx *sync.RWMutex
	// marking positions of Segments
	markupMap map[MarkupKey]*MarkupValue
	// last frame for all segment send for shard
	destinationsEOF map[string][]bool
	// restored all rejects
	rejects frames.RejectStatuses
	// max written segment ID for shard
	maxWriteSegments []uint32
	// state open file
	isOpenFile bool
	// last position when writing to
	lastWriteOffset int64
	// handler for error
	errorHandler ErrorHandler
}

// NewRefillReader - init new RefillReader.
func NewRefillReader(ctx context.Context, cfg FileStorageConfig, errorHandler ErrorHandler) (*RefillReader, error) {
	storage, err := NewFileStorage(cfg)
	if err != nil {
		return nil, err
	}
	rr := &RefillReader{
		storage:      storage,
		mx:           new(sync.RWMutex),
		markupMap:    make(map[MarkupKey]*MarkupValue),
		errorHandler: errorHandler,
	}
	if err := rr.openFile(); err != nil {
		return nil, err
	}
	if err := rr.readMarkup(ctx); err != nil {
		return nil, err
	}

	return rr, nil
}

// String - implements fmt.Stringer interface.
func (rr *RefillReader) String() string {
	return rr.storage.GetPath()
}

// BlockID - return if exist blockID or nil.
func (rr *RefillReader) BlockID() uuid.UUID {
	return rr.title.GetBlockID()
}

// MakeSendMap - distribute refill by destinations.
func (rr *RefillReader) MakeSendMap() *SendMap {
	// grouping by destinations
	sm := NewSendMap(rr.ackStatus.Shards())

	// distribute rejects segment.
	rr.distributeRejects(sm)

	// distribute not ack segment
	rr.distributeNotAck(sm)

	// clearing data to sent
	rr.clearingToSent(sm)

	return sm
}

// distributeRejects - distribute rejects segment.
func (rr *RefillReader) distributeRejects(sm *SendMap) {
	dnames := rr.ackStatus.GetNames()

	for _, rj := range rr.rejects {
		sm.Append(
			dnames.IDToString(int32(rj.NameID)),
			rj.ShardID,
			rj.Segment,
		)
	}
}

// distributeNotAck - distribute not ack segment.
func (rr *RefillReader) distributeNotAck(sm *SendMap) {
	shards := uint16(rr.ackStatus.Shards())

	for _, dname := range rr.ackStatus.GetNames().ToString() {
		for shardID := uint16(0); shardID < shards; shardID++ {
			// we need to check if at least some segments have been recorded
			if n := rr.maxWriteSegments[shardID]; n != math.MaxUint32 {
				for sid := rr.ackStatus.Last(shardID, dname) + 1; sid <= n; sid++ {
					sm.Append(dname, shardID, sid)
				}
			}
		}
	}
}

func (rr *RefillReader) clearingToSent(sm *SendMap) {
	for d := range rr.destinationsEOF {
		for s, eof := range rr.destinationsEOF[d] {
			if eof {
				sm.Remove(d, uint16(s))
			}
		}
	}
}

// readMarkup - read MarkupMap from storage.
//
//revive:disable-next-line:cyclomatic  but readable
func (rr *RefillReader) readMarkup(ctx context.Context) error {
	var off int64
	fsize, err := rr.storage.Size()
	if err != nil {
		return err
	}
	for {
		h, err := frames.ReadHeader(ctx, util.NewOffsetReader(rr.storage, off))
		if errors.Is(err, io.EOF) || errors.Is(err, frames.ErrUnknownFrameType) {
			break
		}
		if err != nil {
			return err
		}
		if fsize < off+int64(h.FullSize()) {
			rr.errorHandler("truncated file by frame", ErrCorruptedFile)
			if err = rr.storage.Truncate(off); err != nil {
				return err
			}
			break
		}
		off += int64(h.SizeOf())
		// read data from body
		if err = rr.readFromBody(ctx, h, off, int(h.GetSize())); err != nil {
			return err
		}
		// move cursor position
		off += int64(h.GetSize())
		if fsize > off+int64(h.SizeOf()) {
			continue
		}
		if fsize != off {
			rr.errorHandler("truncated file by header", ErrCorruptedFile)
			if err = rr.storage.Truncate(off); err != nil {
				return err
			}
		}
		break
	}
	rr.lastWriteOffset = off
	if !rr.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}
	return rr.restoreStatuses()
}

// openFile - open file refill for read/write.
func (rr *RefillReader) openFile() error {
	if rr.isOpenFile {
		return nil
	}
	// open file
	if err := rr.storage.OpenFile(); err != nil {
		return err
	}
	rr.isOpenFile = true

	return nil
}

// restoreFromBody - restore from body frame.
func (rr *RefillReader) readFromBody(ctx context.Context, h *frames.Header, off int64, size int) error {
	switch h.GetType() {
	case frames.TitleType:
		return rr.readTitle(ctx, off, size)
	case frames.DestinationNamesType:
		return rr.readDestinationsNames(ctx, off, size)
	case frames.SegmentType:
		return rr.setMarkupSegment(h, off)
	case frames.StatusType:
		return rr.setMarkupStatus(h, off)
	case frames.RejectStatusType:
		return rr.restoreRejectStatuses(h, off)
	case frames.RefillShardEOFType:
		return rr.restoreRefillShardEOF(h, off)
	}

	return nil
}

// readTitle - read title from file.
func (rr *RefillReader) readTitle(ctx context.Context, off int64, size int) error {
	var err error
	rr.title, err = frames.ReadAtTitle(ctx, rr.storage, off, size)
	if err != nil {
		return err
	}

	// init maxWriteSegments for future reference and not to panic
	rr.maxWriteSegments = newShardStatuses(1 << rr.title.GetShardsNumberPower())

	return nil
}

// readDestinationsNames - restore Destinations Names from file.
func (rr *RefillReader) readDestinationsNames(ctx context.Context, off int64, size int) error {
	// init lastWriteSegment for future reference and not to panic
	// init with shardsNumberPower to init statuses for null values
	rr.ackStatus = NewAckStatusEmpty(rr.title.GetShardsNumberPower())
	err := rr.ackStatus.ReadDestinationsNames(ctx, rr.storage, off, size)
	if err != nil {
		return err
	}

	rr.makeDestinationsEOF()

	return nil
}

// makeDestinationsEOF - make DestinationsEOF.
func (rr *RefillReader) makeDestinationsEOF() {
	shards := rr.ackStatus.Shards()
	dnames := rr.ackStatus.GetNames()

	rr.destinationsEOF = make(map[string][]bool, dnames.Len())
	for _, dname := range dnames.ToString() {
		rr.destinationsEOF[dname] = make([]bool, shards)
	}
}

// setMarkupSegment - fill position and size Segment
func (rr *RefillReader) setMarkupSegment(h *frames.Header, off int64) error {
	if !rr.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	segKey := common.SegmentKey{
		ShardID: h.GetShardID(),
		Segment: h.GetSegmentID(),
	}
	mk := MarkupKey{
		typeFrame:  frames.SegmentType,
		SegmentKey: segKey,
	}
	rr.markupMap[mk] = &MarkupValue{
		pos:  off - int64(h.SizeOf()),
		size: h.GetSize(),
	}

	if rr.maxWriteSegments[segKey.ShardID] == math.MaxUint32 ||
		segKey.Segment > rr.maxWriteSegments[segKey.ShardID] {
		rr.maxWriteSegments[segKey.ShardID] = segKey.Segment
	}

	return nil
}

// we just save status frame position until the EOF and then read the last one
func (rr *RefillReader) setMarkupStatus(h *frames.Header, off int64) error {
	key := MarkupKey{typeFrame: frames.StatusType}
	rr.markupMap[key] = &MarkupValue{
		pos:  off,
		size: h.GetSize(),
	}
	return nil
}

// restoreStatuses - restore states of writers from last status frame
func (rr *RefillReader) restoreStatuses() error {
	key := MarkupKey{typeFrame: frames.StatusType}
	val, ok := rr.markupMap[key]
	if !ok {
		return nil
	}

	buf := make([]byte, val.size)
	if _, err := rr.storage.ReadAt(buf, val.pos); err != nil {
		return err
	}

	return rr.ackStatus.status.UnmarshalBinary(buf)
}

// restoreRejectStatues - restore reject statues from file.
func (rr *RefillReader) restoreRejectStatuses(h *frames.Header, off int64) error {
	if !rr.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	buf := make([]byte, h.GetSize())
	if _, err := rr.storage.ReadAt(buf, off); err != nil {
		return err
	}

	var rs frames.RejectStatuses
	if err := rs.UnmarshalBinary(buf); err != nil {
		return err
	}

	rr.rejects = append(rr.rejects, rs...)

	return nil
}

// restoreRefillShardEOF - restore refill shard EOF from file.
func (rr *RefillReader) restoreRefillShardEOF(h *frames.Header, off int64) error {
	if !rr.checkRestoredServiceData() {
		return ErrServiceDataNotRestored{}
	}

	buf := make([]byte, h.GetSize())
	if _, err := rr.storage.ReadAt(buf, off); err != nil {
		return err
	}

	rs := frames.NewRefillShardEOFEmpty()
	if err := rs.UnmarshalBinary(buf); err != nil {
		return err
	}

	dname := rr.ackStatus.names.IDToString(int32(rs.NameID))
	if dname == "" {
		return nil
	}

	rr.destinationsEOF[dname][rs.ShardID] = true

	return nil
}

// checkRestoredServiceData - check restored service data(title, destinations names),
// these data are required to be restored, without them you cant read the rest
func (rr *RefillReader) checkRestoredServiceData() bool {
	return rr.title != nil && rr.ackStatus != nil
}

// Segment - return segment from storage.
func (rr *RefillReader) Segment(ctx context.Context, mval *MarkupValue) (*frames.BinaryBody, error) {
	rr.mx.RLock()
	defer rr.mx.RUnlock()

	return frames.ReadFrameSegment(ctx, util.NewOffsetReader(rr.storage, mval.pos))
}

// SegmentPosition - return position in storage.
func (rr *RefillReader) SegmentPosition(segKey common.SegmentKey) *MarkupValue {
	rr.mx.RLock()
	defer rr.mx.RUnlock()

	return rr.getSegmentPosition(segKey)
}

// GetSegment - return segment from storage.
func (rr *RefillReader) GetSegment(ctx context.Context, segKey common.SegmentKey) (*frames.BinaryBody, error) {
	rr.mx.RLock()
	defer rr.mx.RUnlock()

	// get position
	mval := rr.getSegmentPosition(segKey)
	if mval == nil {
		return nil, SegmentNotFoundInRefill(segKey)
	}

	return frames.ReadFrameSegment(ctx, util.NewOffsetReader(rr.storage, mval.pos))
}

// getSegmentPosition - return position in storage.
func (rr *RefillReader) getSegmentPosition(segKey common.SegmentKey) *MarkupValue {
	mk := MarkupKey{
		typeFrame:  frames.SegmentType,
		SegmentKey: segKey,
	}
	mval, ok := rr.markupMap[mk]
	if ok {
		return mval
	}

	return nil
}

// WriteRefillShardEOF - write message to mark that all segments have been sent to storage.
func (rr *RefillReader) WriteRefillShardEOF(ctx context.Context, dname string, shardID uint16) error {
	rr.mx.Lock()
	defer rr.mx.Unlock()

	// check open file
	if !rr.isOpenFile {
		if err := rr.openFile(); err != nil {
			return err
		}
	}

	dnameID := rr.ackStatus.names.StringToID(dname)
	if dnameID == frames.NotFoundName {
		return fmt.Errorf(
			"StringToID: unknown name %s",
			dname,
		)
	}

	// create frame
	fe, err := frames.NewRefillShardEOFFrame(uint32(dnameID), shardID)
	if err != nil {
		return err
	}

	// write in storage
	n, err := fe.WriteTo(rr.storage.Writer(ctx, rr.lastWriteOffset))
	if err != nil {
		return err
	}

	// move position
	rr.lastWriteOffset += n

	// set last frame
	rr.destinationsEOF[dname][shardID] = true

	return nil
}

// DeleteFile - close and delete current file.
func (rr *RefillReader) DeleteFile() error {
	rr.mx.Lock()
	defer rr.mx.Unlock()

	if rr.isOpenFile {
		err := rr.storage.Close()
		if err != nil {
			return err
		}
	}

	// set flag and delete
	rr.isOpenFile = false

	return rr.storage.DeleteCurrentFile()
}

// Close - close reader.
func (rr *RefillReader) Close() error {
	rr.mx.Lock()
	defer rr.mx.Unlock()

	rr.isOpenFile = false
	return rr.storage.Close()
}
