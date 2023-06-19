package delivery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
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
	dialers        map[string]Dialer
	refillInterval time.Duration
	errorHandler   ErrorHandler
	clock          clockwork.Clock

	destinations []string
	blockID      uuid.UUID
	encodersLock *sync.Mutex
	encoders     []ManagerEncoder
	exchange     *Exchange
	refill       ManagerRefill
	refillSignal chan struct{}
	cancelRefill context.CancelCauseFunc
	refillDone   chan struct{}
	senders      []*Sender
}

type (
	// ManagerEncoder - interface for encoder manager.
	ManagerEncoder interface {
		LastEncodedSegment() uint32
		Encode(context.Context, ShardedData) (SegmentKey, Segment, Redundant, error)
		Snapshot(context.Context, []Redundant) (Snapshot, error)
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
		Get(context.Context, SegmentKey) (Segment, error)
		Ack(SegmentKey, string)
		Reject(SegmentKey, string)
		Restore(context.Context, SegmentKey) (Snapshot, []Segment, error)
		WriteSegment(context.Context, SegmentKey, Segment) error
		WriteSnapshot(context.Context, SegmentKey, Snapshot) error
		WriteAckStatus(context.Context) error
		TemporarilyRename() error
		Shutdown(context.Context) error
	}

	// ManagerRefillCtor - func-constuctor for ManagerRefill.
	ManagerRefillCtor func(
		ctx context.Context,
		blockID uuid.UUID,
		destinations []string,
		shardsNumberPower uint8,
	) (ManagerRefill, error)
)

// NewManager - init new Manager.
//
//revive:disable-next-line:function-length long but readable
func NewManager(
	ctx context.Context,
	dialers []Dialer,
	encoderCtor ManagerEncoderCtor,
	refillCtor ManagerRefillCtor,
	shardsNumberPower uint8,
	refillInterval time.Duration,
	errorHandler ErrorHandler,
	clock clockwork.Clock,
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
	refill, err := refillCtor(ctx, blockID, destinations, shardsNumberPower)
	if err != nil {
		return nil, fmt.Errorf("create refill: %w", err)
	}
	if !refill.IsContinuable() {
		if err = refill.TemporarilyRename(); err != nil {
			return nil, fmt.Errorf("rename refill: %w", err)
		}

		if err = refill.Shutdown(ctx); err != nil {
			return nil, fmt.Errorf("shutdown refill: %w", err)
		}

		refill, err = refillCtor(ctx, blockID, destinations, shardsNumberPower)
		if err != nil {
			return nil, fmt.Errorf("create refill: %w", err)
		}
	}
	blockID = refill.BlockID()
	exchange := NewExchange(refill.Shards(), refill.Destinations())

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

	mgr := &Manager{
		dialers:        dialersMap,
		refillInterval: refillInterval,
		errorHandler:   errorHandler,
		clock:          clock,

		destinations: destinations,
		blockID:      blockID,
		encodersLock: new(sync.Mutex),
		encoders:     encoders,
		exchange:     exchange,
		refill:       refill,
		refillSignal: make(chan struct{}, 1),
		refillDone:   make(chan struct{}),
	}

	refillCtx, cancel := context.WithCancelCause(ctx)
	mgr.cancelRefill = cancel
	go mgr.refillLoop(refillCtx)

	return mgr, nil
}

// Send - send data to encoders.
func (mgr *Manager) Send(ctx context.Context, data ShardedData) (ack bool, err error) {
	result := NewSendPromise(len(mgr.encoders))
	expiredAt := mgr.clock.Now().Add(mgr.refillInterval)
	group, gCtx := errgroup.WithContext(ctx)
	mgr.encodersLock.Lock()
	for i := range mgr.encoders {
		i := i
		group.Go(func() error {
			key, segment, redundant, errEnc := mgr.encoders[i].Encode(gCtx, data)
			if errEnc != nil {
				return errEnc
			}
			mgr.exchange.Put(key, segment, redundant, result, expiredAt)
			return nil
		})
	}
	err = group.Wait()
	mgr.encodersLock.Unlock()
	if err != nil {
		// TODO: is encoder recoverable?
		return false, err
	}

	return result.Await(ctx)
}

// Open run senders and refill loops
func (mgr *Manager) Open(ctx context.Context) {
	mgr.senders = make([]*Sender, 0, len(mgr.dialers)*len(mgr.encoders))
	for name, dialer := range mgr.dialers {
		for shardID := range mgr.encoders {
			lastAck := mgr.refill.LastSegment(uint16(shardID), name)
			sender := NewSender(ctx, mgr.blockID, uint16(shardID), dialer, lastAck, mgr, mgr.errorHandler)
			mgr.senders = append(mgr.senders, sender)
		}
	}
}

// Get - get segment for key.
func (mgr *Manager) Get(ctx context.Context, key SegmentKey) (Segment, error) {
	segment, err := mgr.exchange.Get(ctx, key)
	if errors.Is(err, ErrSegmentGone) {
		return mgr.refill.Get(ctx, key)
	}
	return segment, err
}

// Ack - mark ack segment for key and destanition.
func (mgr *Manager) Ack(key SegmentKey, dest string) {
	mgr.refill.Ack(key, dest)
	mgr.exchange.Ack(key)
}

// Reject - mark reject segment for key and destanition and send to refill.
func (mgr *Manager) Reject(key SegmentKey, dest string) {
	mgr.refill.Reject(key, dest)
	if mgr.exchange.Reject(key) {
		select {
		case mgr.refillSignal <- struct{}{}:
		default:
		}
	}
}

// Restore - get data for restore state from refill.
func (mgr *Manager) Restore(ctx context.Context, key SegmentKey) (Snapshot, []Segment) {
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
	return mgr.refill.TemporarilyRename()
}

// Shutdown - safe shutdown manager with clearing queue and shutdown senders.
func (mgr *Manager) Shutdown(ctx context.Context) error {
	mgr.exchange.Shutdown(ctx)
	mgr.cancelRefill(ErrShutdown)
	<-mgr.refillDone
	// TODO: very dangerous solution, can send the CPU into space
	//revive:disable-next-line:empty-block work performed in condition
	for mgr.collectSegmentsToRefill(ctx) {
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
		less := func(a, b SegmentKey) bool {
			return a.ShardID < b.ShardID || (a.ShardID == b.ShardID && a.Segment < b.Segment)
		}
		sort.Slice(rejected, func(i, j int) bool { return less(rejected[i], rejected[j]) })
	}

	removeRestOfShard := func(keys []SegmentKey, i int) []SegmentKey {
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

func (mgr *Manager) writeSegmentToRefill(ctx context.Context, key SegmentKey) error {
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
	if err = mgr.refill.WriteSnapshot(ctx, key, snapshot); err != nil {
		return err
	}
	return mgr.refill.WriteSegment(ctx, key, segment)
}

func (mgr *Manager) snapshot(ctx context.Context, key SegmentKey) (Snapshot, error) {
	mgr.encodersLock.Lock()
	defer mgr.encodersLock.Unlock()

	lastSegment := mgr.encoders[key.ShardID].LastEncodedSegment()

	if key.Segment > lastSegment+1 {
		panic("invalid segment key: more last segment +1")
	}

	redundants := make([]Redundant, 0, lastSegment-key.Segment+1)
	for segment := key.Segment; segment <= lastSegment; segment++ {
		key = SegmentKey{ShardID: key.ShardID, Segment: segment}
		redundant, err := mgr.exchange.Redundant(ctx, key)
		if err != nil {
			return nil, err
		}
		redundants = append(redundants, redundant)
	}

	return mgr.encoders[key.ShardID].Snapshot(ctx, redundants)
}

// DefaultShardsNumberPower - default shards number power.
const DefaultShardsNumberPower = 0

// CalculateRequiredShardsNumberPower - get need shards number power.
func (*Manager) CalculateRequiredShardsNumberPower() uint8 {
	return DefaultShardsNumberPower
}
