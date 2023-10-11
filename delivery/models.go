package delivery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrAborted - error for promise is aborted
	ErrAborted = errors.New("promise is aborted")
)

// Segment is an universal interface for blob segment data
type Segment interface {
	frames.WritePayload
}

// Snapshot is an universal interface for blob snapshot data
type Snapshot interface {
	frames.WritePayload
}

// ProtoData is an universal interface for blob protobuf data
type ProtoData interface {
	Bytes() []byte
	Destroy()
}

// ErrorHandler useful for logging errors caused in delivery box
type ErrorHandler func(msg string, err error)

// SendPromise is a status aggregator
//
// Promise is created for batch of data and aggregate statuses of all segments
// produced from this data (segment per shard).
// Promise resolved when all statuses has been changed.
type SendPromise struct {
	done    chan struct{}
	err     error
	counter int32
	refills int32
}

// NewSendPromise is a constructor
func NewSendPromise(shardsNumber int) *SendPromise {
	return &SendPromise{
		done:    make(chan struct{}),
		counter: int32(shardsNumber),
		refills: 0,
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

// Abort - marks that one of shards has been aborted.
func (promise *SendPromise) Abort() {
	counter := atomic.SwapInt32(&promise.counter, 0)
	if counter > 0 {
		promise.err = ErrAborted
		close(promise.done)
	}
}

// Error - promise resolve with error.
func (promise *SendPromise) Error(err error) {
	counter := atomic.SwapInt32(&promise.counter, 0)
	if counter > 0 {
		promise.err = err
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
		if promise.err != nil {
			return false, promise.err
		}
		return promise.refills == 0, nil
	}
}

const (
	// DefaultMaxDuration - default max duration open head.
	DefaultMaxDuration = 5 * time.Second
	// DefaultMaxSamples - default max samples open head.
	DefaultMaxSamples = 40e3
	// DefaultLastAddTimeout - default last add timeout open head.
	DefaultLastAddTimeout = 200 * time.Millisecond
)

// OpenHeadLimits configure how long encoder should accumulate data for one segment
type OpenHeadLimits struct {
	MaxDuration    time.Duration `validate:"required"`
	LastAddTimeout time.Duration `validate:"required"`
	MaxSamples     uint32        `validate:"min=5000"`
}

// DefaultOpenHeadLimits - generate default OpenHeadLimits.
func DefaultOpenHeadLimits() OpenHeadLimits {
	return OpenHeadLimits{
		MaxDuration:    DefaultMaxDuration,
		LastAddTimeout: DefaultLastAddTimeout,
		MaxSamples:     DefaultMaxSamples,
	}
}

// MarshalBinary - encoding to byte.
func (l *OpenHeadLimits) MarshalBinary() ([]byte, error) {
	//revive:disable-next-line:add-constant sum 8+4+8
	buf := make([]byte, 0, 20)

	buf = binary.AppendUvarint(buf, uint64(l.MaxDuration))
	buf = binary.AppendUvarint(buf, uint64(l.LastAddTimeout))
	buf = binary.AppendUvarint(buf, uint64(l.MaxSamples))
	return buf, nil
}

// UnmarshalBinary - decoding from byte.
func (l *OpenHeadLimits) UnmarshalBinary(data []byte) error {
	var offset int

	maxDuration, n := binary.Uvarint(data[offset:])
	l.MaxDuration = time.Duration(maxDuration)
	offset += n

	lastAddTimeout, n := binary.Uvarint(data[offset:])
	l.LastAddTimeout = time.Duration(lastAddTimeout)
	offset += n

	maxSamples, _ := binary.Uvarint(data[offset:])
	l.MaxSamples = uint32(maxSamples)

	return nil
}

// OpenHeadPromise is a SendPromise wrapper to combine several Sends in one segment
type OpenHeadPromise struct {
	*SendPromise
	done     chan struct{}
	timer    clockwork.Timer
	deadline time.Time
	samples  []uint32 // limit to decrease
	clock    clockwork.Clock
	timeout  time.Duration
}

// NewOpenHeadPromise is a constructor
func NewOpenHeadPromise(
	shards int,
	limits OpenHeadLimits,
	clock clockwork.Clock,
	callback func(),
	raceCounter prometheus.Counter,
) *OpenHeadPromise {
	done := make(chan struct{})
	samples := make([]uint32, shards)
	for i := range samples {
		samples[i] = limits.MaxSamples
	}
	return &OpenHeadPromise{
		SendPromise: NewSendPromise(shards),
		done:        done,
		timer: clock.AfterFunc(limits.MaxDuration, func() {
			callback()
			// We catch bug when timer.Stop return true even if function is called.
			// In this case we have double close channel. It's not clear how reproduce it in tests
			// or why it's happen. So here is a crutch
			// FIXME: double close channel on timer.Stop race
			select {
			case <-done:
				raceCounter.Inc()
			default:
				close(done)
			}
		}),
		deadline: clock.Now().Add(limits.MaxDuration),
		samples:  samples,
		clock:    clock,
		timeout:  limits.LastAddTimeout,
	}
}

// Add appends data to promise and checks limits. It returns true if limits reached.
func (promise *OpenHeadPromise) Add(segments []common.Segment) (limitsReached bool, afterFinish func()) {
	select {
	case <-promise.done:
		panic("Attempt to add data in finalized promise")
	default:
	}
	stopAndReturnCloser := func() func() {
		if promise.timer.Stop() {
			return func() { close(promise.done) }
		}
		return func() {}
	}
	for i, segment := range segments {
		if segment == nil {
			continue
		}
		if promise.samples[i] < segment.Samples() {
			return true, stopAndReturnCloser()
		}
		promise.samples[i] -= segment.Samples()
	}
	timeout := -promise.clock.Since(promise.deadline)
	if timeout > promise.timeout {
		timeout = promise.timeout
	}
	if timeout <= 0 {
		return true, stopAndReturnCloser()
	}
	promise.timer.Reset(timeout)
	return false, func() {}
}

// Finalized wait until Close method call
func (promise *OpenHeadPromise) Finalized() <-chan struct{} {
	return promise.done
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

// CorruptedEncoderError - error for currepted error.
type CorruptedEncoderError struct {
	err error
}

func isUnhandledEncoderError(err error) bool {
	return !common.IsRemoteWriteLimitsExceedsError(err) &&
		!common.IsRemoteWriteParsingError(err)
}

func markAsCorruptedEncoderError(err error) error {
	return CorruptedEncoderError{err: err}
}

// Error implements error interface
func (err CorruptedEncoderError) Error() string {
	return err.err.Error()
}

// Unwrap implements errors.Unwrapper interface
func (err CorruptedEncoderError) Unwrap() error {
	return err.err
}
