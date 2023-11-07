package delivery

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrPromiseCanceled - error for promise is canceled
	ErrPromiseCanceled = errors.New("promise is canceled")
	// ErrSegmentGone - error for segment gone from exchange
	ErrSegmentGone = errors.New("segment gone from exchange")
)

// Exchange holds and coordinate segment-redundants flow
type Exchange struct {
	locked         uint32
	destinations   int
	records        *sync.Map // map[common.SegmentKey]*exchangeRecord
	lastSegments   []uint32  // max segment id per shard
	rejects        []uint32  // 0-1 marks that shard has rejected segment
	alwaysToRefill bool
	// stat
	puts    prometheus.Counter
	removes prometheus.Counter
}

// NewExchange is a constructor
//
// We rely on the fact that destinations is a correct index name-serial.
func NewExchange(shards, destinations int, alwaysToRefill bool, registerer prometheus.Registerer) *Exchange {
	lastSegments := make([]uint32, shards)
	for i := range lastSegments {
		lastSegments[i] = math.MaxUint32
	}
	factory := util.NewUnconflictRegisterer(registerer)
	return &Exchange{
		locked:         0,
		destinations:   destinations,
		records:        new(sync.Map),
		lastSegments:   lastSegments,
		rejects:        make([]uint32, shards),
		alwaysToRefill: alwaysToRefill,
		puts: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "odarix_core_delivery_exchange_puts",
				Help: "Number of put in exchange.",
			},
		),
		removes: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "odarix_core_delivery_exchange_removes",
				Help: "Number of remove in exchange.",
			},
		),
	}
}

// deleteRecord - delete record from map.
func (ex *Exchange) deleteRecord(key any) {
	record, ok := ex.records.LoadAndDelete(key)
	if ok {
		exr := record.(*exchangeRecord)
		if exr.Resolved() && !errors.Is(exr.err, ErrPromiseCanceled) {
			ex.removes.Inc()
		}
	}
}

// Get returns segment
//
// If it's already gone, return ErrSegmentGone, if it stil not in exchange
// await and return.
//
// If exchange locked (after Shutdown), and there is not putted segment,
// it returns ErrPromiseCanceled.
func (ex *Exchange) Get(ctx context.Context, key common.SegmentKey) (common.Segment, error) {
	lastSegment := atomic.LoadUint32(&ex.lastSegments[key.ShardID])
	record, ok := ex.records.Load(key)
	if ok {
		return record.(*exchangeRecord).Segment(ctx)
	}

	if lastSegment != math.MaxUint32 && lastSegment >= key.Segment {
		return nil, ErrSegmentGone
	}
	record, ok = ex.records.LoadOrStore(key, newExchangeRecord())
	if !ok && atomic.LoadUint32(&ex.locked) != 0 {
		record.(*exchangeRecord).CancelIfNotResolved()
		ex.deleteRecord(key)
	}
	return record.(*exchangeRecord).Segment(ctx)
}

// Put resolve promise associated with the key
//
// Result of delivery acks will be stored in sendPromise when record will be delete from exchange.
func (ex *Exchange) Put(
	key common.SegmentKey,
	segment common.Segment,
	redundant common.Redundant,
	sendPromise *SendPromise,
	expiredAt time.Time,
) {
	if atomic.LoadUint32(&ex.locked) != 0 {
		panic("put data in locked exchange")
	}

	ex.puts.Inc()
	record, _ := ex.records.LoadOrStore(key, newExchangeRecord())
	record.(*exchangeRecord).Resolve(segment, redundant, ex.destinations, sendPromise, expiredAt)
	if !atomic.CompareAndSwapUint32(&ex.lastSegments[key.ShardID], key.Segment-1, key.Segment) {
		panic("invalid segment putted in exchange")
	}
}

// Ack segment by key
func (ex *Exchange) Ack(key common.SegmentKey) {
	record, ok := ex.records.Load(key)
	if !ok {
		return
	}
	if !record.(*exchangeRecord).Ack() {
		return
	}
	if ex.isSafeForDelete(key) {
		ex.deleteRecord(key)
	}
}

// isSafeForDelete checks conditions that segment can be removed safely
//
// If shard has any rejected segment, than we should preserve all segments in this shard
// for minimize snapshots count.
func (ex *Exchange) isSafeForDelete(key common.SegmentKey) bool {
	return atomic.LoadUint32(&ex.rejects[key.ShardID]) == 0 && !ex.alwaysToRefill
}

// Reject segment by key
func (ex *Exchange) Reject(key common.SegmentKey) bool {
	atomic.StoreUint32(&ex.rejects[key.ShardID], 1)
	record, ok := ex.records.Load(key)
	if ok {
		record.(*exchangeRecord).Reject()
		return true
	}
	return false
}

// RejectedOrExpired returns slice of keys which was rejected
func (ex *Exchange) RejectedOrExpired(now time.Time) (keys []common.SegmentKey, empty bool) {
	empty = true
	ex.records.Range(func(k, value any) bool {
		empty = false
		key := k.(common.SegmentKey)
		record := value.(*exchangeRecord)
		if !record.Resolved() {
			return true
		}
		if record.Rejected() || record.Expired(now) || atomic.LoadUint32(&ex.rejects[key.ShardID]) != 0 {
			keys = append(keys, key)
		}
		return true
	})
	return keys, empty
}

// Remove keys from exchange because them writted in refill
func (ex *Exchange) Remove(keys []common.SegmentKey) {
	if len(keys) == 0 {
		return
	}

	for _, key := range keys {
		if value, ok := ex.records.Load(key); ok {
			ex.deleteRecord(key)
			record := value.(*exchangeRecord)
			if !record.Delivered() {
				record.sendPromise.Refill()
			}
		}
	}
}

// RemoveAll - remove all keys from exchange.
func (ex *Exchange) RemoveAll() {
	ex.records.Range(
		func(key, value any) bool {
			value.(*exchangeRecord).sendPromise.Abort()
			ex.deleteRecord(key)
			return true
		},
	)
}

// Redundant returns redundant by key
func (ex *Exchange) Redundant(ctx context.Context, key common.SegmentKey) (common.Redundant, error) {
	record, ok := ex.records.Load(key)
	if ok {
		return record.(*exchangeRecord).Redundant(ctx)
	}
	if atomic.LoadUint32(&ex.lastSegments[key.ShardID]) >= key.Segment {
		return nil, ErrSegmentGone
	}
	record, ok = ex.records.LoadOrStore(key, newExchangeRecord)
	if !ok && atomic.LoadUint32(&ex.locked) != 0 {
		record.(*exchangeRecord).CancelIfNotResolved()
		ex.deleteRecord(key)
	}
	return record.(*exchangeRecord).Redundant(ctx)
}

// Shutdown locks exchange and await until it will be empty
//
// Shutdown also cancels all unresolved promises, cause put is forbidden after lock.
func (ex *Exchange) Shutdown(_ context.Context) {
	atomic.StoreUint32(&ex.locked, 1)

	ex.records.Range(func(key, value any) bool {
		record := value.(*exchangeRecord)
		if record.CancelIfNotResolved() {
			ex.deleteRecord(key)
		}
		return true
	})
}

type exchangeRecord struct {
	*segmentPromise
	*sendStatus
	sendPromise *SendPromise
	expiredAt   time.Time
}

func newExchangeRecord() *exchangeRecord {
	return &exchangeRecord{
		segmentPromise: newSegmentPromise(),
	}
}

func (record *exchangeRecord) Resolve(
	segment common.Segment,
	redundant common.Redundant,
	destinations int,
	sendPromise *SendPromise,
	expiredAt time.Time,
) {
	record.sendStatus = newSendStatus(destinations)
	record.sendPromise = sendPromise
	record.expiredAt = expiredAt

	record.segmentPromise.Resolve(segment, redundant)
}

func (record *exchangeRecord) Ack() bool {
	if record.sendStatus.Ack() {
		record.sendPromise.Ack()
		return true
	}
	return false
}

func (record *exchangeRecord) Delivered() bool {
	return record.segmentPromise.Resolved() && record.sendStatus.Delivered()
}

func (record *exchangeRecord) Rejected() bool {
	return record.segmentPromise.Resolved() && record.sendStatus.Rejected()
}

func (record *exchangeRecord) Expired(now time.Time) bool {
	return record.segmentPromise.Resolved() && now.After(record.expiredAt)
}

type segmentPromise struct {
	segment   common.Segment
	redundant common.Redundant
	err       error
	resolve   chan struct{}
}

func newSegmentPromise() *segmentPromise {
	return &segmentPromise{
		resolve: make(chan struct{}),
	}
}

func (promise *segmentPromise) Segment(ctx context.Context) (common.Segment, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-promise.resolve:
		return promise.segment, promise.err
	}
}

func (promise *segmentPromise) Redundant(ctx context.Context) (common.Redundant, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-promise.resolve:
		return promise.redundant, promise.err
	}
}

func (promise *segmentPromise) Resolve(segment common.Segment, redundant common.Redundant) {
	promise.segment = segment
	promise.redundant = redundant
	close(promise.resolve)
}

func (promise *segmentPromise) Resolved() bool {
	select {
	case <-promise.resolve:
		return true
	default:
		return false
	}
}

func (promise *segmentPromise) CancelIfNotResolved() bool {
	select {
	case <-promise.resolve:
		return false
	default:
	}
	promise.err = ErrPromiseCanceled
	close(promise.resolve)
	return true
}

type sendStatus struct {
	counter, rejects int32
}

func newSendStatus(destinations int) *sendStatus {
	return &sendStatus{
		counter: int32(destinations),
	}
}

func (status *sendStatus) Ack() bool {
	if atomic.AddInt32(&status.counter, -1) == 0 {
		return status.rejects == 0
	}
	return false
}

func (status *sendStatus) Reject() {
	atomic.AddInt32(&status.rejects, 1)
	atomic.AddInt32(&status.counter, -1)
}

func (status *sendStatus) Delivered() bool {
	return atomic.LoadInt32(&status.counter) == 0 && atomic.LoadInt32(&status.rejects) == 0
}

func (status *sendStatus) Rejected() bool {
	return status != nil && atomic.LoadInt32(&status.rejects) > 0
}
