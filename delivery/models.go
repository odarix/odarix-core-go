package delivery

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/odarix/odarix-core-go/common"
)

// ErrorHandler useful for logging errors caused in delivery box
type ErrorHandler func(msg string, err error)

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

// ErrSegmentNotFoundInRefill - error segment not found in refill.
type ErrSegmentNotFoundInRefill struct {
	key common.SegmentKey
}

// SegmentNotFoundInRefill create ErrSegmentNotFoundInRefill error
func SegmentNotFoundInRefill(key common.SegmentKey) ErrSegmentNotFoundInRefill {
	return ErrSegmentNotFoundInRefill{key}
}

// Error - implements error.
func (err ErrSegmentNotFoundInRefill) Error() string {
	return fmt.Sprintf("segment %s not found", err.key)
}

// Permanent - sign of a permanent error.
func (ErrSegmentNotFoundInRefill) Permanent() bool {
	return true
}

// ErrServiceDataNotRestored - error if service data not recovered(title, destinations names).
type ErrServiceDataNotRestored struct{}

// Error - implements error.
func (ErrServiceDataNotRestored) Error() string {
	return "service data not recovered"
}

// Permanent - sign of a permanent error.
func (ErrServiceDataNotRestored) Permanent() bool {
	return true
}
