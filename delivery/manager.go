package delivery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/common"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrDestinationsRequired - error no destinations found in config
	ErrDestinationsRequired = errors.New("no destinations found in config")
	// ErrNotContinuableRefill - error refill contains not continuable data
	ErrNotContinuableRefill = errors.New("refill contains not continuable data")
	// ErrShutdown - error shutdown
	ErrShutdown = errors.New("shutdown")
)

// Manager - general circuit manager.
type Manager struct {
	dialers map[string]Dialer
	// refillInterval must not exceed the shutdown timeout,
	// or the shutdown time must be greater than refillInterval.
	// Otherwise, the data will be lost (not included in the refill file).
	refillInterval time.Duration
	errorHandler   ErrorHandler
	clock          clockwork.Clock

	destinations []string
	blockID      uuid.UUID
	encodersLock *sync.Mutex
	hashdexCtor  HashdexCtor
	encoders     []ManagerEncoder
	exchange     *Exchange
	refill       ManagerRefill
	refillSignal chan struct{}
	cancelRefill context.CancelCauseFunc
	refillDone   chan struct{}
	senders      []*Sender
	haTracker    HATracker
	// stat
	registerer       prometheus.Registerer
	encodeDuration   prometheus.Histogram
	promiseDuration  prometheus.Histogram
	segmentSize      prometheus.Histogram
	snapshotSize     prometheus.Histogram
	lagDuration      *prometheus.HistogramVec
	segmentSeries    prometheus.Histogram
	segmentSamples   prometheus.Histogram
	rejects          prometheus.Counter
	shutdownDuration prometheus.Histogram
}

type (
	// ManagerEncoder - interface for encoder manager.
	ManagerEncoder interface {
		LastEncodedSegment() uint32
		Encode(context.Context, common.ShardedData) (common.SegmentKey, common.Segment, common.Redundant, error)
		Snapshot(context.Context, []common.Redundant) (common.Snapshot, error)
		Destroy()
	}

	// ManagerEncoderCtor - func-constuctor for ManagerEncoder.
	ManagerEncoderCtor func(
		blockID uuid.UUID,
		shardID uint16,
		shardsNumberPower uint8,
	) (ManagerEncoder, error)

	// ManagerRefill - interface for refill manager.
	ManagerRefill interface {
		IsContinuable() bool
		BlockID() uuid.UUID
		Shards() int
		Destinations() int
		LastSegment(uint16, string) uint32
		Get(context.Context, common.SegmentKey) (Segment, error)
		Ack(common.SegmentKey, string)
		Reject(common.SegmentKey, string)
		Restore(context.Context, common.SegmentKey) (Snapshot, []Segment, error)
		WriteSegment(context.Context, common.SegmentKey, Segment) error
		WriteSnapshot(context.Context, common.SegmentKey, Snapshot) error
		WriteAckStatus(context.Context) error
		IntermediateRename() error
		Shutdown(context.Context) error
	}

	// ManagerRefillCtor - func-constuctor for ManagerRefill.
	ManagerRefillCtor func(
		ctx context.Context,
		blockID uuid.UUID,
		destinations []string,
		shardsNumberPower uint8,
		registerer prometheus.Registerer,
	) (ManagerRefill, error)

	// HashdexCtor - func-constuctor for Hashdex.
	HashdexCtor func(protoData []byte) common.ShardedData

	// HATracker - interface for High Availability Tracker.
	HATracker interface {
		IsDrop(cluster, replica string) bool
		Destroy()
	}
)

// NewManager - init new Manager.
//
//revive:disable-next-line:function-length long but readable
//revive:disable-next-line:cyclomatic  but readable
func NewManager(
	ctx context.Context,
	dialers []Dialer,
	hashdexCtor HashdexCtor,
	encoderCtor ManagerEncoderCtor,
	refillCtor ManagerRefillCtor,
	shardsNumberPower uint8,
	refillInterval time.Duration,
	haTracker HATracker,
	errorHandler ErrorHandler,
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) (*Manager, error) {
	if len(dialers) == 0 {
		return nil, ErrDestinationsRequired
	}
	dialersMap := make(map[string]Dialer, len(dialers))
	destinations := make([]string, len(dialers))
	for i, dialer := range dialers {
		dialersMap[dialer.String()] = dialer
		destinations[i] = dialer.String()
	}

	blockID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate block id: %w", err)
	}
	refill, err := refillCtor(ctx, blockID, destinations, shardsNumberPower, registerer)
	if err != nil {
		return nil, fmt.Errorf("create refill: %w", err)
	}
	if !refill.IsContinuable() {
		if err = refill.Shutdown(ctx); err != nil {
			return nil, fmt.Errorf("rename refill: %w", err)
		}

		refill, err = refillCtor(ctx, blockID, destinations, shardsNumberPower, registerer)
		if err != nil {
			return nil, fmt.Errorf("create refill: %w", err)
		}
	}
	blockID = refill.BlockID()
	exchange := NewExchange(refill.Shards(), refill.Destinations(), registerer)

	encoders := make([]ManagerEncoder, refill.Shards())
	for i := range encoders {
		encoder, err := encoderCtor(blockID, uint16(i), shardsNumberPower)
		if err != nil {
			return nil, fmt.Errorf("create encoder: %w", err)
		}
		encoders[i] = encoder
	}
	if errorHandler == nil {
		errorHandler = func(string, error) {}
	}

	factory := NewConflictRegisterer(registerer)
	mgr := &Manager{
		dialers:        dialersMap,
		refillInterval: refillInterval,
		errorHandler:   errorHandler,
		clock:          clock,

		destinations: destinations,
		blockID:      blockID,
		encodersLock: new(sync.Mutex),
		hashdexCtor:  hashdexCtor,
		encoders:     encoders,
		exchange:     exchange,
		refill:       refill,
		refillSignal: make(chan struct{}, 1),
		refillDone:   make(chan struct{}),
		haTracker:    haTracker,
		registerer:   registerer,
		encodeDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_encode_duration_milliseconds",
				Help:    "Duration of encode data(ms).",
				Buckets: prometheus.ExponentialBucketsRange(0.9, 20, 10),
			},
		),
		promiseDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_promise_duration_seconds",
				Help:    "Duration of promise await.",
				Buckets: prometheus.ExponentialBucketsRange(0.1, 20, 10),
			},
		),
		segmentSize: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_segment_bytes",
				Help:    "Size of encoded segment.",
				Buckets: prometheus.ExponentialBucketsRange(1024, 1<<20, 10),
			},
		),
		snapshotSize: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_snapshot_bytes",
				Help:    "Size of encoded snapshot.",
				Buckets: prometheus.ExponentialBucketsRange(1<<20, 120<<20, 10),
			},
		),
		lagDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_lag_duration_seconds",
				Help:    "Lag of duration.",
				Buckets: prometheus.ExponentialBucketsRange(60, 7200, 10),
			},
			[]string{"type"},
		),
		segmentSeries: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_segment_series",
				Help:    "Number of timeseries.",
				Buckets: prometheus.ExponentialBucketsRange(500, 5000, 10),
			},
		),
		segmentSamples: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_segment_samples",
				Help:    "Number of samples.",
				Buckets: prometheus.ExponentialBucketsRange(500, 5000, 10),
			},
		),
		rejects: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "odarix_core_delivery_manager_rejects",
				Help: "Number of rejects.",
			},
		),
		shutdownDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_shutdown_duration_seconds",
				Help:    "Duration of shutdown manager(s).",
				Buckets: prometheus.ExponentialBucketsRange(0.1, 20, 10),
			},
		),
	}

	refillCtx, cancel := context.WithCancelCause(ctx)
	mgr.cancelRefill = cancel
	go mgr.refillLoop(refillCtx)

	return mgr, nil
}

// Send - send data to encoders.
func (mgr *Manager) Send(ctx context.Context, data ProtoData) (ack bool, err error) {
	hx := mgr.hashdexCtor(data.Bytes())
	if mgr.haTracker.IsDrop(hx.Cluster(), hx.Replica()) {
		hx.Destroy()
		data.Destroy()
		return true, nil
	}
	result := NewSendPromise(len(mgr.encoders))
	expiredAt := mgr.clock.Now().Add(mgr.refillInterval)
	group, gCtx := errgroup.WithContext(ctx)
	mgr.encodersLock.Lock()
	start := time.Now()
	for i := range mgr.encoders {
		i := i
		group.Go(func() error {
			key, segment, redundant, errEnc := mgr.encoders[i].Encode(gCtx, hx)
			if errEnc != nil {
				return errEnc
			}
			mgr.segmentSize.Observe(float64(len(segment.Bytes())))
			mgr.segmentSeries.Observe(float64(segment.Series()))
			mgr.segmentSamples.Observe(float64(segment.Samples()))
			tsNow := time.Now().UnixMilli()
			maxTS := segment.Latest()
			if maxTS != 0 {
				mgr.lagDuration.With(prometheus.Labels{"type": "max"}).Observe(float64((tsNow - maxTS) / 1000))
			}
			minTS := segment.Earliest()
			if minTS != math.MaxInt64 {
				mgr.lagDuration.With(prometheus.Labels{"type": "min"}).Observe(float64((tsNow - minTS) / 1000))
			}
			mgr.exchange.Put(key, segment, redundant, result, expiredAt)
			return nil
		})
	}
	err = group.Wait()
	mgr.encodersLock.Unlock()
	mgr.encodeDuration.Observe(float64(time.Since(start).Milliseconds()))
	hx.Destroy()
	data.Destroy()
	if err != nil {
		// TODO: is encoder recoverable?
		return false, err
	}
	defer func(start time.Time) {
		mgr.promiseDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	return result.Await(ctx)
}

// Open run senders and refill loops
func (mgr *Manager) Open(ctx context.Context) {
	mgr.senders = make([]*Sender, 0, len(mgr.dialers)*len(mgr.encoders))
	for name, dialer := range mgr.dialers {
		for shardID := range mgr.encoders {
			lastAck := mgr.refill.LastSegment(uint16(shardID), name)
			sender := NewSender(
				ctx,
				mgr.blockID,
				uint16(shardID),
				dialer,
				lastAck,
				mgr,
				mgr.errorHandler,
				mgr.registerer,
			)
			mgr.senders = append(mgr.senders, sender)
		}
	}
}

// Get - get segment for key.
func (mgr *Manager) Get(ctx context.Context, key common.SegmentKey) (Segment, error) {
	segment, err := mgr.exchange.Get(ctx, key)
	if errors.Is(err, ErrSegmentGone) {
		return mgr.refill.Get(ctx, key)
	}
	return segment, err
}

// Ack - mark ack segment for key and destanition.
func (mgr *Manager) Ack(key common.SegmentKey, dest string) {
	mgr.refill.Ack(key, dest)
	mgr.exchange.Ack(key)
}

// Reject - mark reject segment for key and destanition and send to refill.
func (mgr *Manager) Reject(key common.SegmentKey, dest string) {
	mgr.refill.Reject(key, dest)
	if mgr.exchange.Reject(key) {
		select {
		case mgr.refillSignal <- struct{}{}:
		default:
		}
	}
	mgr.rejects.Inc()
}

// Restore - get data for restore state from refill.
func (mgr *Manager) Restore(ctx context.Context, key common.SegmentKey) (Snapshot, []Segment) {
	snapshot, segments, err := mgr.refill.Restore(ctx, key)
	if err == nil {
		return snapshot, segments
	}

	// refill may not contain snapshot yet
	snapshot, err = mgr.snapshot(ctx, key)
	if err == nil {
		return snapshot, nil
	}

	// redundants may be dropped from exchange during refill-loop
	snapshot, segments, err = mgr.refill.Restore(ctx, key)
	if err != nil {
		panic("unrestorable state. Impossible")
	}
	return snapshot, segments
}

// Close - rename refill file for close file and shutdown manager.
func (mgr *Manager) Close() error {
	return mgr.refill.IntermediateRename()
}

// Shutdown - safe shutdown manager with clearing queue and shutdown senders.
//
// refillInterval must not exceed the shutdown timeout,
// or the shutdown time must be greater than refillInterval.
// Otherwise, the data will be lost (not included in the refill file).
func (mgr *Manager) Shutdown(ctx context.Context) error {
	defer func(start time.Time) {
		mgr.shutdownDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	mgr.exchange.Shutdown(ctx)
	mgr.cancelRefill(ErrShutdown)
	<-mgr.refillDone
	// very dangerous solution, can send the CPU into space
	//revive:disable-next-line:empty-block work performed in condition
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for ctx.Err() == nil && mgr.collectSegmentsToRefill(ctx) {
		<-tick.C
	}
	wg := new(sync.WaitGroup)
	wg.Add(len(mgr.senders))
	var errs error
	m := new(sync.Mutex)
	for _, sender := range mgr.senders {
		go func(sender *Sender) {
			defer wg.Done()
			if err := sender.Shutdown(ctx); err != nil {
				m.Lock()
				errs = multierr.Append(errs, fmt.Errorf("%s: %w", sender, err))
				m.Unlock()
			}
		}(sender)
	}
	wg.Wait()

	// delete all segments because they are not required and reject all state
	mgr.exchange.RemoveAll()

	mgr.encodersLock.Lock()
	for i := range mgr.encoders {
		mgr.encoders[i].Destroy()
	}
	mgr.encodersLock.Unlock()

	return multierr.Append(errs, mgr.refill.Shutdown(ctx))
}

func (mgr *Manager) refillLoop(ctx context.Context) {
	ticker := mgr.clock.NewTicker(mgr.refillInterval)
	defer ticker.Stop()
	defer close(mgr.refillDone)
	for {
		select {
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), ErrShutdown) {
				mgr.errorHandler("refill loop context canceled", context.Cause(ctx))
			}
			return
		case <-ticker.Chan():
			mgr.collectSegmentsToRefill(ctx)
		case <-mgr.refillSignal:
			mgr.collectSegmentsToRefill(ctx)
		}
	}
}

func (mgr *Manager) collectSegmentsToRefill(ctx context.Context) bool {
	rejected, empty := mgr.exchange.RejectedOrExpired(mgr.clock.Now())
	if len(rejected) > 0 {
		less := func(a, b common.SegmentKey) bool {
			return a.ShardID < b.ShardID || (a.ShardID == b.ShardID && a.Segment < b.Segment)
		}
		sort.Slice(rejected, func(i, j int) bool { return less(rejected[i], rejected[j]) })
	}

	removeRestOfShard := func(keys []common.SegmentKey, i int) []common.SegmentKey {
		j := i + 1
		for j < len(keys) && keys[j].ShardID == keys[i].ShardID {
			j++
		}
		return append(keys[:i], keys[j:]...)
	}
	for i := 0; i < len(rejected); i++ {
		if err := mgr.writeSegmentToRefill(ctx, rejected[i]); err != nil {
			mgr.errorHandler("fail to write segment in refill", err)
			rejected = removeRestOfShard(rejected, i)
			i--
		}
	}
	if err := mgr.refill.WriteAckStatus(ctx); err != nil {
		mgr.errorHandler("fail write ack status in refill", err)
	}
	mgr.exchange.Remove(rejected)
	return !empty
}

func (mgr *Manager) writeSegmentToRefill(ctx context.Context, key common.SegmentKey) error {
	segment, err := mgr.exchange.Get(ctx, key)
	switch {
	case errors.Is(err, ErrSegmentGone):
		// it is possible that segment may be ack and removed from exchange
		return nil
	case err != nil:
		return err
	}
	err = mgr.refill.WriteSegment(ctx, key, segment)
	if err == nil || !errors.Is(err, ErrSnapshotRequired) {
		return err
	}
	snapshot, err := mgr.snapshot(ctx, key)
	if err != nil {
		return err
	}
	err = mgr.refill.WriteSnapshot(ctx, key, snapshot)
	snapshot.Destroy()
	if err != nil {
		return err
	}
	return mgr.refill.WriteSegment(ctx, key, segment)
}

func (mgr *Manager) snapshot(ctx context.Context, key common.SegmentKey) (common.Snapshot, error) {
	mgr.encodersLock.Lock()
	defer mgr.encodersLock.Unlock()

	lastSegment := mgr.encoders[key.ShardID].LastEncodedSegment()

	if key.Segment > lastSegment+1 {
		panic("invalid segment key: more last segment +1")
	}

	redundants := make([]common.Redundant, 0, lastSegment-key.Segment+1)
	for segment := key.Segment; segment <= lastSegment; segment++ {
		key = common.SegmentKey{ShardID: key.ShardID, Segment: segment}
		redundant, err := mgr.exchange.Redundant(ctx, key)
		if err != nil {
			return nil, err
		}
		redundants = append(redundants, redundant)
	}

	snapshot, err := mgr.encoders[key.ShardID].Snapshot(ctx, redundants)
	mgr.snapshotSize.Observe(float64(len(snapshot.Bytes())))
	return snapshot, err
}

// DefaultShardsNumberPower - default shards number power.
const DefaultShardsNumberPower = 0

// CalculateRequiredShardsNumberPower - get need shards number power.
func (*Manager) CalculateRequiredShardsNumberPower() uint8 {
	return DefaultShardsNumberPower
}
