package delivery

import (
	"context"
	"errors"
	"sync/atomic"
	"unsafe"
)

// ErrorHandler useful for logging errors caused in delivery box
type ErrorHandler func(msg string, err error)

// Data - data to be sent.
type Data interface{}

// ShardedData - array of structures (*LabelSet, timestamp, value, LSHash)
type ShardedData interface{}

// Segment - encoded data segment
type Segment interface {
	Bytes() []byte
	Destroy()
}

// Redundant - information to create a Snapshot
type Redundant interface {
	PointerData() unsafe.Pointer
	Destroy()
}

// Snapshot - snapshot of encoder/decoder
type Snapshot interface {
	Bytes() []byte
	Destroy()
}

// SegmentKey - key to store segment in Exchange and Refill
type SegmentKey struct {
	ShardID uint16
	Segment uint32
}

// IsFirst returns true if it is a first segment in shard
func (key SegmentKey) IsFirst() bool {
	return key.Segment == 0
}

// Prev returns key to previous segment in same sahrd
func (key SegmentKey) Prev() SegmentKey {
	return SegmentKey{
		ShardID: key.ShardID,
		Segment: key.Segment - 1,
	}
}

// SendPromise is a status aggregator
//
// Promise is created for batch of data and aggregate statuses of all segments
// produced from this data (segment per shard).
// Promise resolved when all statuses has been changed.
type SendPromise struct {
	counter int32
	refills int32
	done    chan struct{}
}

// NewSendPromise is a constructor
func NewSendPromise(shardsNumber int) *SendPromise {
	return &SendPromise{
		counter: int32(shardsNumber),
		refills: 0,
		done:    make(chan struct{}),
	}
}

// Ack marks that one of shards has been ack
func (promise *SendPromise) Ack() {
	if atomic.AddInt32(&promise.counter, -1) == 0 {
		close(promise.done)
	}
}

// Refill marks that one of shards has been refill
func (promise *SendPromise) Refill() {
	atomic.AddInt32(&promise.refills, 1)
	counter := atomic.AddInt32(&promise.counter, -1)
	if counter == 0 {
		close(promise.done)
	}
}

// Await concurrently waits until all shard statuses changed to not initial state
// and returns true if all segments in ack-state or false otherwise
//
// It's thread-safe and context-canceled operation. It returns error only if context done.
func (promise *SendPromise) Await(ctx context.Context) (ack bool, err error) {
	select {
	case <-ctx.Done():
		return false, context.Cause(ctx)
	case <-promise.done:
		return promise.refills == 0, nil
	}
}

// IsPermanent - check if the error is permanent.
func IsPermanent(err error) bool {
	var p interface {
		Permanent() bool
	}
	if errors.As(err, &p) {
		return p.Permanent()
	}
	return false
}

// ErrSegmentNotFoundRefill - error segment not found in refill.
type ErrSegmentNotFoundRefill struct{}

// Error - implements error.
func (ErrSegmentNotFoundRefill) Error() string {
	return "segment not found"
}

// Unwrap - implements errors.Unwrap.
func (e ErrSegmentNotFoundRefill) Unwrap() error {
	return e
}

// Is - implements errors.Is.
func (ErrSegmentNotFoundRefill) Is(target error) bool {
	_, ok := target.(ErrSegmentNotFoundRefill)
	return ok
}

// Permanent - sign of a permanent error.
func (ErrSegmentNotFoundRefill) Permanent() bool {
	return true
}

// ErrServiceDataNotRestored - error if service data not recovered(title, destinations names).
type ErrServiceDataNotRestored struct{}

// Error - implements error.
func (ErrServiceDataNotRestored) Error() string {
	return "service data not recovered"
}

// Unwrap - implements errors.Unwrap.
func (e ErrServiceDataNotRestored) Unwrap() error {
	return e
}

// Is - implements errors.Is.
func (ErrServiceDataNotRestored) Is(target error) bool {
	_, ok := target.(ErrServiceDataNotRestored)
	return ok
}

// Permanent - sign of a permanent error.
func (ErrServiceDataNotRestored) Permanent() bool {
	return true
}
