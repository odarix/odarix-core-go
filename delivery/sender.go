package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/prometheus/client_golang/prometheus"
)

// Source is a manager
type Source interface {
	Get(ctx context.Context, key common.SegmentKey) (Segment, error)
	Ack(key common.SegmentKey, dest string)
	Reject(key common.SegmentKey, dest string)
	Restore(ctx context.Context, key common.SegmentKey) (Snapshot, []Segment)
}

// Sender is a transport adapter for manager
type Sender struct {
	dialer        Dialer
	source        Source
	blockID       uuid.UUID
	shardID       uint16
	lastDelivered uint32
	done          chan struct{}
	errorHandler  ErrorHandler
	cancelCause   context.CancelCauseFunc
	// stat
	sentSegment      prometheus.Gauge
	responsedSegment *prometheus.GaugeVec
}

// NewSender is a constructor
func NewSender(
	ctx context.Context,
	blockID uuid.UUID,
	shardID uint16,
	dialer Dialer,
	lastAck uint32,
	source Source,
	errorHandler ErrorHandler,
	registerer prometheus.Registerer,
) *Sender {
	factory := NewConflictRegisterer(registerer)
	sender := &Sender{
		dialer:        dialer,
		source:        source,
		blockID:       blockID,
		shardID:       shardID,
		lastDelivered: lastAck,
		done:          make(chan struct{}),
		errorHandler:  errorHandler,
		sentSegment: factory.NewGauge(
			prometheus.GaugeOpts{
				Name:        "odarix_core_delivery_sender_sent_segment",
				Help:        "Sent segment ID.",
				ConstLabels: prometheus.Labels{"host": dialer.String()},
			},
		),
		responsedSegment: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "odarix_core_delivery_sender_responsed_segment",
				Help:        "Responsed segment ID.",
				ConstLabels: prometheus.Labels{"host": dialer.String()},
			},
			[]string{"state"},
		),
	}
	sender.sentSegment.Set(0)
	sender.responsedSegment.With(prometheus.Labels{"state": "ack"}).Set(0)
	sender.responsedSegment.With(prometheus.Labels{"state": "reject"}).Set(0)
	ctx, cancel := context.WithCancelCause(ctx)
	sender.cancelCause = cancel
	go sender.mainLoop(ctx)
	return sender
}

// String implements fmt.Stringer interface
func (sender *Sender) String() string {
	return sender.dialer.String()
}

// Shutdown await while write receive ErrPromiseCanceled and then ack on last sent
func (sender *Sender) Shutdown(ctx context.Context) error {
	sender.cancelCause(ErrShutdown)

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-sender.done:
		return nil
	}
}

//revive:disable-next-line:cognitive-complexity function is not complicated
func (sender *Sender) mainLoop(ctx context.Context) {
	writeDone := new(atomic.Bool)
	errRead := errors.New("read error")
	for ctx.Err() == nil {
		transport, closeTransport, err := sender.dial(ctx)
		if err != nil {
			continue
		}
		lastSent := sender.lastDelivered
		onResponse := func(id uint32) {
			if !atomic.CompareAndSwapUint32(&sender.lastDelivered, id-1, id) {
				panic(fmt.Sprintf("%s: unexpected segment %d (lastDelivered %d)", sender, id, sender.lastDelivered))
			}
			if writeDone.Load() && lastSent == id {
				closeTransport()
				close(sender.done)
			}
		}
		transport.OnAck(func(id uint32) {
			sender.responsedSegment.With(prometheus.Labels{"state": "ack"}).Set(float64(id))
			sender.source.Ack(common.SegmentKey{ShardID: sender.shardID, Segment: id}, sender.String())
			onResponse(id)
		})
		transport.OnReject(func(id uint32) {
			sender.responsedSegment.With(prometheus.Labels{"state": "reject"}).Set(float64(id))
			sender.source.Reject(common.SegmentKey{ShardID: sender.shardID, Segment: id}, sender.String())
			onResponse(id)
		})
		writeCtx, cancel := context.WithCancelCause(ctx)
		transport.OnReadError(func(err error) {
			if !errors.Is(err, io.EOF) {
				sender.errorHandler(fmt.Sprintf("%s: fail to read response", sender), err)
			}
			cancel(errRead)
		})
		transport.Listen(ctx)
		lastSent, err = sender.writeLoop(writeCtx, transport, lastSent)
		if err != nil {
			if !errors.Is(err, errRead) {
				sender.errorHandler(fmt.Sprintf("%s: fail to send segment", sender), err)
			}
			closeTransport()
			continue
		}
		// transport will be closed by reader otherwise
		if lastSent == atomic.LoadUint32(&sender.lastDelivered) {
			closeTransport()
			close(sender.done)
		}
		writeDone.Store(true)
		return
	}
	close(sender.done)
}

func (sender *Sender) dial(ctx context.Context) (transport Transport, closeFn func(), err error) {
	transport, err = sender.dialer.Dial(ctx)
	if err != nil {
		if !errors.Is(err, ErrShutdown) && !errors.Is(err, context.Canceled) {
			sender.errorHandler(fmt.Sprintf("%s: fail to dial", sender), err)
		}
		return nil, nil, err
	}

	closeFn = func() {
		if err := transport.Close(); err != nil {
			sender.errorHandler(fmt.Sprintf("%s: fail to close transport", sender), err)
		}
	}

	// restore connection state
	if sender.lastDelivered != math.MaxUint32 {
		snapshot, segments := sender.source.Restore(ctx, common.SegmentKey{
			ShardID: sender.shardID,
			Segment: sender.lastDelivered + 1,
		})
		segmentID := sender.lastDelivered + 1 - uint32(len(segments))
		if snapshot != nil {
			frame := frames.NewWriteFrame(protocolVersion, frames.SnapshotType, sender.shardID, segmentID, snapshot)
			if err := transport.Send(ctx, frame); err != nil {
				if ctx.Err() == nil {
					sender.errorHandler(fmt.Sprintf("%s: fail to send restore", sender), err)
				}
				closeFn()
				return nil, nil, err
			}
		}
		for i := range segments {
			frame := frames.NewWriteFrame(
				protocolVersion,
				frames.DrySegmentType,
				sender.shardID,
				segmentID+uint32(i),
				segments[i],
			)
			if err := transport.Send(ctx, frame); err != nil {
				if ctx.Err() == nil {
					sender.errorHandler(fmt.Sprintf("%s: fail to send restore", sender), err)
				}
				closeFn()
				return nil, nil, err
			}
		}
	}

	return transport, closeFn, nil
}

func (sender *Sender) writeLoop(ctx context.Context, transport Transport, from uint32) (uint32, error) {
	id := from + 1
	for ; ctx.Err() == nil; id++ {
		segment, err := sender.getSegment(ctx, id)
		if segment == nil {
			return id - 1, err
		}
		frame := frames.NewWriteFrame(protocolVersion, frames.SegmentType, sender.shardID, id, segment)
		if err = transport.Send(ctx, frame); err != nil {
			return id - 1, err
		}
		sender.sentSegment.Set(float64(id))
	}
	return id - 1, context.Cause(ctx)
}

// getSegment returns segment by sender shardID and given segment id
//
// It retry get segment from source with exponential backoff until one of next happened:
// - get segment without error (returns (segment, nil))
// - context canceled (returns (nil, context.Cause(ctx)))
// - get ErrPromiseCanceled (returns (nil, nil))
//
// So, it's correct to check that segment is nil, it is equivalent permanent state.
func (sender *Sender) getSegment(ctx context.Context, id uint32) (Segment, error) {
	key := common.SegmentKey{
		ShardID: sender.shardID,
		Segment: id,
	}
	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = 0 // retry until context cancel
	bo := backoff.WithContext(eb, ctx)
	segment, err := backoff.RetryWithData(func() (Segment, error) {
		segment, err := sender.source.Get(ctx, key)
		if errors.Is(err, ErrPromiseCanceled) {
			return nil, nil
		}
		return segment, err
	}, bo)
	if err != nil && err == ctx.Err() {
		err = context.Cause(ctx)
	}
	return segment, err
}
