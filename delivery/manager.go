package delivery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
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
	// uncommittedTimeWindow must not exceed the shutdown timeout,
	// or the shutdown time must be greater than uncommittedTimeWindow.
	// Otherwise, the data will be lost (not included in the refill file).
	uncommittedTimeWindow time.Duration
	limits                Limits
	errorHandler          ErrorHandler
	clock                 clockwork.Clock

	destinations   []string
	blockID        uuid.UUID
	encodersLock   *sync.Mutex
	hashdexCtor    HashdexCtor
	encoders       []ManagerEncoder
	ohCreatedAt    time.Time
	ohPromise      *OpenHeadPromise
	exchange       *Exchange
	refill         ManagerRefill
	refillSignal   chan struct{}
	cancelRefill   context.CancelCauseFunc
	refillDone     chan struct{}
	senders        []*Sender
	rejectNotifyer RejectNotifyer
	haTracker      HATracker
	walsSizes      []int64
	// stat
	registerer               prometheus.Registerer
	encodeDuration           prometheus.Histogram
	promiseDuration          prometheus.Histogram
	segmentSize              prometheus.Histogram
	snapshotSize             prometheus.Histogram
	lagDuration              *prometheus.HistogramVec
	segmentSeries            prometheus.Histogram
	segmentSamples           prometheus.Histogram
	rejects                  prometheus.Counter
	shutdownDuration         prometheus.Histogram
	promiseStopRace          prometheus.Counter
	ohPromiseFinalize        prometheus.Counter
	ohPromiseDuration        prometheus.Histogram
	currentShardsNumberPower prometheus.Gauge
}

type (
	// ManagerEncoder - interface for encoder manager.
	ManagerEncoder interface {
		LastEncodedSegment() uint32
		Encode(context.Context, common.ShardedData) (common.SegmentKey, common.Segment, common.Redundant, error)
		Add(context.Context, common.ShardedData) (common.Segment, error)
		Finalize(context.Context) (common.SegmentKey, common.Segment, common.Redundant, error)
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
		workingDir string,
		blockID uuid.UUID,
		destinations []string,
		shardsNumberPower uint8,
		alwaysToRefill bool,
		registerer prometheus.Registerer,
	) (ManagerRefill, error)

	// HashdexCtor - func-constuctor for Hashdex.
	HashdexCtor func(protoData []byte, limits common.HashdexLimits) (common.ShardedData, error)

	// HATracker - interface for High Availability Tracker.
	HATracker interface {
		IsDrop(cluster, replica string) bool
		Destroy()
	}

	// RejectNotifyer - notify on reject.
	RejectNotifyer interface {
		NotifyOnReject()
	}
)

// NewManager - init new Manager.
//
//revive:disable-next-line:function-length long but readable
//revive:disable-next-line:cyclomatic  but readable
//revive:disable-next-line:argument-limit  but readable and convenient
func NewManager(
	ctx context.Context,
	dialers []Dialer,
	hashdexCtor HashdexCtor,
	encoderCtor ManagerEncoderCtor,
	refillCtor ManagerRefillCtor,
	shardsNumberPower uint8,
	uncommittedTimeWindow time.Duration,
	workingDir string,
	limits Limits,
	rejectNotifyer RejectNotifyer,
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
	alwaysToRefill := uncommittedTimeWindow == AlwaysToRefill
	refill, err := refillCtor(workingDir, blockID, destinations, shardsNumberPower, alwaysToRefill, registerer)
	if err != nil {
		return nil, fmt.Errorf("create refill: %w", err)
	}
	if !refill.IsContinuable() {
		if err = refill.Shutdown(ctx); err != nil {
			return nil, fmt.Errorf("rename refill: %w", err)
		}

		refill, err = refillCtor(workingDir, blockID, destinations, shardsNumberPower, alwaysToRefill, registerer)
		if err != nil {
			return nil, fmt.Errorf("create refill: %w", err)
		}
	}
	blockID = refill.BlockID()
	exchange := NewExchange(refill.Shards(), refill.Destinations(), alwaysToRefill, registerer)

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
		dialers:               dialersMap,
		uncommittedTimeWindow: uncommittedTimeWindow,
		limits:                limits,

		errorHandler: errorHandler,
		clock:        clock,

		destinations:   destinations,
		blockID:        blockID,
		encodersLock:   new(sync.Mutex),
		hashdexCtor:    hashdexCtor,
		encoders:       encoders,
		exchange:       exchange,
		refill:         refill,
		refillSignal:   make(chan struct{}, 1),
		refillDone:     make(chan struct{}),
		rejectNotifyer: rejectNotifyer,
		haTracker:      haTracker,
		walsSizes:      make([]int64, 1<<shardsNumberPower),
		registerer:     registerer,
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
				Buckets: prometheus.ExponentialBucketsRange(500, 20000, 10),
			},
		),
		segmentSamples: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_segment_samples",
				Help:    "Number of samples.",
				Buckets: prometheus.ExponentialBucketsRange(500, 20000, 10),
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
		promiseStopRace: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "odarix_core_delivery_manager_promise_stop_race_total",
				Help: "Counter of race error on open head promise finalization.",
			},
		),
		ohPromiseFinalize: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "odarix_core_delivery_manager_open_head_promise_finalize_total",
				Help: "Counter of finalized segments in open head mode.",
			},
		),
		ohPromiseDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "odarix_core_delivery_manager_open_head_promise_duration_seconds",
				Help:    "Duration of open head collect data before finalize.",
				Buckets: prometheus.ExponentialBucketsRange(0.001, 1, 10),
			},
		),
		currentShardsNumberPower: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "odarix_core_delivery_manager_shards_number_power",
				Help: "Current value shards number power.",
			},
		),
	}
	mgr.currentShardsNumberPower.Set(float64(shardsNumberPower))

	refillCtx, cancel := context.WithCancelCause(ctx)
	mgr.cancelRefill = cancel
	go mgr.refillLoop(refillCtx)

	return mgr, nil
}

// Send - send data to encoders.
func (mgr *Manager) Send(ctx context.Context, data ProtoData) (ack bool, err error) {
	hx, err := mgr.hashdexCtor(data.Bytes(), mgr.limits.Hashdex)
	if err != nil {
		data.Destroy()
		return false, err
	}

	if mgr.haTracker.IsDrop(hx.Cluster(), hx.Replica()) {
		data.Destroy()
		return true, nil
	}
	result := NewSendPromise(len(mgr.encoders))
	expiredAt := mgr.clock.Now().Add(mgr.uncommittedTimeWindow / 2)
	group, gCtx := errgroup.WithContext(ctx)
	mgr.encodersLock.Lock()
	start := time.Now()
	errs := make([]error, len(mgr.encoders))
	for i := range mgr.encoders {
		i := i
		group.Go(func() error {
			key, segment, redundant, errEnc := mgr.encoders[i].Encode(gCtx, hx)
			if errEnc != nil {
				if isUnhandledEncoderError(errEnc) {
					errEnc = markAsCorruptedEncoderError(errEnc)
				}
				errs[i] = errEnc
				return errEnc
			}
			mgr.observeSegmentMetrics(segment)
			mgr.exchange.Put(key, segment, redundant, result, expiredAt)
			if mgr.uncommittedTimeWindow == AlwaysToRefill {
				mgr.triggeredRefill()
			}
			return nil
		})
	}
	_ = group.Wait()
	err = errors.Join(errs...)
	mgr.encodersLock.Unlock()
	mgr.encodeDuration.Observe(float64(time.Since(start).Milliseconds()))
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

// SendOpenHead adds data to encoders to send it later when limits reached
func (mgr *Manager) SendOpenHead(ctx context.Context, data ProtoData) (ack bool, err error) {
	hx, err := mgr.hashdexCtor(data.Bytes(), mgr.limits.Hashdex)
	if err != nil {
		data.Destroy()
		return false, err
	}

	if mgr.haTracker.IsDrop(hx.Cluster(), hx.Replica()) {
		data.Destroy()
		return true, nil
	}

	mgr.encodersLock.Lock()
	segments, err := mgr.addData(ctx, hx)
	data.Destroy()

	promise := mgr.getPromise()
	limitsReached, afterFinish := promise.Add(segments)
	if limitsReached {
		mgr.finalizePromise()
		afterFinish()
	}
	mgr.encodersLock.Unlock()
	if err != nil {
		// TODO: is encoder recoverable?
		return false, err
	}

	defer func(start time.Time) {
		mgr.promiseDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	return promise.Await(ctx)
}

func (mgr *Manager) addData(ctx context.Context, hx common.ShardedData) ([]common.Segment, error) {
	defer func(start time.Time) {
		mgr.encodeDuration.Observe(float64(time.Since(start).Milliseconds()))
	}(time.Now())

	segments := make([]common.Segment, len(mgr.encoders))
	errs := make([]error, len(mgr.encoders))
	group, gCtx := errgroup.WithContext(ctx)
	for i := range mgr.encoders {
		i := i
		group.Go(func() error {
			segment, err := mgr.encoders[i].Add(gCtx, hx)
			if err != nil {
				if isUnhandledEncoderError(err) {
					err = markAsCorruptedEncoderError(err)
				}
			}
			segments[i] = segment
			errs[i] = err
			return err
		})
	}
	_ = group.Wait()

	return segments, errors.Join(errs...)
}

func (mgr *Manager) getPromise() *OpenHeadPromise {
	if mgr.ohPromise == nil {
		var promise *OpenHeadPromise
		promise = NewOpenHeadPromise(len(mgr.encoders), mgr.limits.OpenHead, mgr.clock, func() {
			mgr.encodersLock.Lock()
			defer mgr.encodersLock.Unlock()

			// It is possible that this function called concurrent with SendOpenHead method.
			// In this case finalizePromise will be called directly from SendOpenHead.
			// To avoid finalizing different promise we check here that promise didn't changed.
			if mgr.ohPromise == promise {
				mgr.finalizePromise()
			}
		}, mgr.promiseStopRace)

		mgr.ohCreatedAt = mgr.clock.Now()
		mgr.ohPromise = promise
	}
	return mgr.ohPromise
}

func (mgr *Manager) finalizePromise() {
	ctx := context.Background()
	group, gCtx := errgroup.WithContext(ctx)
	expiredAt := mgr.clock.Now().Add(mgr.uncommittedTimeWindow / 2)
	for i := range mgr.encoders {
		i := i
		group.Go(func() error {
			key, segment, redundant, errEnc := mgr.encoders[i].Finalize(gCtx)
			if errEnc != nil {
				return errEnc
			}
			atomic.AddInt64(&mgr.walsSizes[i], segment.Size())
			mgr.observeSegmentMetrics(segment)
			mgr.exchange.Put(key, segment, redundant, mgr.ohPromise.SendPromise, expiredAt)
			if mgr.uncommittedTimeWindow == AlwaysToRefill {
				mgr.triggeredRefill()
			}
			return nil
		})
	}
	err := group.Wait()
	if err != nil {
		mgr.ohPromise.Error(err)
	}
	mgr.ohPromiseFinalize.Inc()
	mgr.ohPromiseDuration.Observe(mgr.clock.Since(mgr.ohCreatedAt).Seconds())
	mgr.ohPromise = nil
}

func (mgr *Manager) observeSegmentMetrics(segment common.Segment) {
	mgr.segmentSize.Observe(float64(segment.Size()))
	mgr.segmentSeries.Observe(float64(segment.Series()))
	mgr.segmentSamples.Observe(float64(segment.Samples()))
	tsNow := time.Now().UnixMilli()
	maxTS := segment.Latest()
	if maxTS != 0 {
		mgr.lagDuration.With(
			prometheus.Labels{"type": "max"},
		).Observe(float64((tsNow - maxTS) / 1000))
	}
	minTS := segment.Earliest()
	if minTS != math.MaxInt64 {
		mgr.lagDuration.With(
			prometheus.Labels{"type": "min"},
		).Observe(float64((tsNow - minTS) / 1000))
	}
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
		mgr.triggeredRefill()
	}
	mgr.rejectNotifyer.NotifyOnReject()
	mgr.rejects.Inc()
}

func (mgr *Manager) triggeredRefill() {
	select {
	case mgr.refillSignal <- struct{}{}:
	default:
	}
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
// uncommittedTimeWindow must not exceed the shutdown timeout,
// or the shutdown time must be greater than uncommittedTimeWindow.
// Otherwise, the data will be lost (not included in the refill file).
//
//revive:disable-next-line:function-length long but readable
func (mgr *Manager) Shutdown(ctx context.Context) error {
	defer func(start time.Time) {
		mgr.shutdownDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	mgr.encodersLock.Lock()
	promise := mgr.ohPromise
	mgr.encodersLock.Unlock()
	if promise != nil {
		<-promise.Finalized()
	}

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
	var ticker clockwork.Ticker
	if mgr.uncommittedTimeWindow != AlwaysToRefill {
		ticker = mgr.clock.NewTicker(mgr.uncommittedTimeWindow / 2)
	} else {
		ticker = NewDoNothingTicker()
	}

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
	mgr.snapshotSize.Observe(float64(snapshot.Size()))
	return snapshot, err
}

// MaxBlockBytes - maximum block size among all shards.
func (mgr *Manager) MaxBlockBytes() int64 {
	var maxSize int64
	for i := range mgr.walsSizes {
		s := atomic.LoadInt64(&mgr.walsSizes[i])
		if s > maxSize {
			maxSize = s
		}
	}
	return maxSize
}

// OpenHeadLimits - current config for open head.
func (mgr *Manager) OpenHeadLimits() OpenHeadLimits {
	return mgr.limits.OpenHead
}

// Limits - current limits.
func (mgr *Manager) Limits() Limits {
	return mgr.limits
}

// DoNothingTicker - imitation of a ticker, but only one that does nothing
type DoNothingTicker struct {
	c chan time.Time
}

// NewDoNothingTicker - init new DoNothingTicker.
func NewDoNothingTicker() *DoNothingTicker {
	return &DoNothingTicker{c: make(chan time.Time)}
}

// Chan - implement clockwork.Ticker.
func (t *DoNothingTicker) Chan() <-chan time.Time {
	return t.c
}

// Reset - implement clockwork.Ticker.
func (*DoNothingTicker) Reset(_ time.Duration) {}

// Stop - implement clockwork.Ticker.
func (*DoNothingTicker) Stop() {}
