package delivery

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPromiseCanceled - error for promise is canceled
	ErrPromiseCanceled = errors.New("promise is canceled")
	// ErrSegmentGone - error for segment gone from exchange
	ErrSegmentGone = errors.New("segment gone from exchange")
	// ErrSegmentNotFound - error for segment was not putted in exchange
	ErrSegmentNotFound = errors.New("segment was not putted in exchange")
)

// Exchange holds and coordinate segment-redundants flow
type Exchange struct {
	locked       uint32
	destinations int
	// rwmutex      *sync.RWMutex
	// records      map[SegmentKey]*exchangeRecord
	records      *sync.Map // map[SegmentKey]*exchangeRecord
	lastSegments []uint32
}

// NewExchange is a constructor
//
// We rely on the fact that destinations is a correct index name-serial.
func NewExchange(shards, destinations int) *Exchange {
	lastSegments := make([]uint32, shards)
	for i := range lastSegments {
		lastSegments[i] = math.MaxUint32
	}
	return &Exchange{
		locked:       0,
		destinations: destinations,
		// rwmutex:      new(sync.RWMutex),
		// records:      make(map[SegmentKey]*exchangeRecord),
		records:      new(sync.Map),
		lastSegments: lastSegments,
	}
}

// deleteRecord - delete record from map.
func (ex *Exchange) deleteRecord(key any) {
	record, ok := ex.records.LoadAndDelete(key)
	if ok {
		record.(*exchangeRecord).Destroy()
	}
}

// Get returns segment
//
// If it's already gone, return ErrSegmentGone, if it stil not in exchange
// await and return.
//
// If exchange locked (after Shutdown), and there is not putted segment,
// it returns ErrPromiseCanceled.
func (ex *Exchange) Get(ctx context.Context, key SegmentKey) (Segment, error) {
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
	key SegmentKey,
	segment Segment,
	redundant Redundant,
	sendPromise *SendPromise,
	expiredAt time.Time,
) {
	if atomic.LoadUint32(&ex.locked) != 0 {
		panic("put data in locked exchange")
	}

	record, _ := ex.records.LoadOrStore(key, newExchangeRecord())
	record.(*exchangeRecord).Resolve(segment, redundant, ex.destinations, sendPromise, expiredAt)
	if !atomic.CompareAndSwapUint32(&ex.lastSegments[key.ShardID], key.Segment-1, key.Segment) {
		panic("invalid segment putted in exchange")
	}
}

// Ack segment by key
func (ex *Exchange) Ack(key SegmentKey) {
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

// If ancestor still in exchange, then there is a rejected ancestor.
// We should preserve segments after rejected segment until it will be written in refill
// because we probably will need to make a snapshot for it (require all redundants after).
// If some segment not safe for delete it will be deleted later, on remove rejected ancestor.
func (ex *Exchange) isSafeForDelete(key SegmentKey) bool {
	ancestorKey := SegmentKey{ShardID: key.ShardID, Segment: key.Segment - 1}
	_, ancestorInExchange := ex.records.Load(ancestorKey)
	return !ancestorInExchange
}

// Reject segment by key
func (ex *Exchange) Reject(key SegmentKey) bool {
	record, ok := ex.records.Load(key)
	if ok {
		record.(*exchangeRecord).Reject()
		return true
	}
	return false
}

// RejectedOrExpired returns slice of keys which was rejected
func (ex *Exchange) RejectedOrExpired(now time.Time) (keys []SegmentKey, empty bool) {
	empty = true
	ex.records.Range(func(key, value any) bool {
		empty = false
		record := value.(*exchangeRecord)
		if record.Rejected() || record.Expired(now) {
			keys = append(keys, key.(SegmentKey))
		}
		return true
	})
	return keys, empty
}

// Remove keys from exchange because them writted in refill
func (ex *Exchange) Remove(keys []SegmentKey) {
	if len(keys) == 0 {
		return
	}

	for _, key := range keys {
		if record, ok := ex.records.Load(key); ok {
			ex.removeWithAckFollowers(key)
			record.(*exchangeRecord).sendPromise.Refill()
		}
	}
}

// It is possible situation when follow segments already delivered
// but still in exchange because have rejected ancestor.
// We should delete it here. (See Exchange.Ack method.)
func (ex *Exchange) removeWithAckFollowers(key SegmentKey) {
	ex.deleteRecord(key)
	for {
		key.Segment++
		record, ok := ex.records.Load(key)
		if !ok || !record.(*exchangeRecord).Delivered() {
			return
		}
		ex.deleteRecord(key)
	}
}

// Redundant returns redundant by key
func (ex *Exchange) Redundant(ctx context.Context, key SegmentKey) (Redundant, error) {
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
	segment Segment,
	redundant Redundant,
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
	segment   Segment
	redundant Redundant
	err       error
	resolve   chan struct{}
}

func newSegmentPromise() *segmentPromise {
	return &segmentPromise{
		resolve: make(chan struct{}),
	}
}

func (promise *segmentPromise) Segment(ctx context.Context) (Segment, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-promise.resolve:
		return promise.segment, promise.err
	}
}

func (promise *segmentPromise) Destroy() {
	if promise.segment != nil {
		promise.segment.Destroy()
	}
	if promise.redundant != nil {
		promise.redundant.Destroy()
	}
}

func (promise *segmentPromise) Redundant(ctx context.Context) (Redundant, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-promise.resolve:
		return promise.redundant, promise.err
	}
}

func (promise *segmentPromise) Resolve(segment Segment, redundant Redundant) {
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
	return atomic.LoadInt32(&status.rejects) > 0
}
