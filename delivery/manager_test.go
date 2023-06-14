package delivery_test

//go:generate moq -out manager_moq_test.go -pkg delivery_test -rm . Dialer Transport ManagerEncoder ManagerRefill

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/odarix/odarix-core-go/delivery"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ManagerSuite struct {
	suite.Suite
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerSuite))
}

func (s *ManagerSuite) TestSendWithAck() {
	baseCtx := context.Background()

	s.T().Log("Use auto-ack transport (ack segements after ms delay)")
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination)}

	s.T().Log("Use no-op refill: assumed that it won't be touched")
	refillCtor := s.constructorForRefill(&ManagerRefillMock{
		AckFunc:            func(delivery.SegmentKey, string) {},
		WriteAckStatusFunc: func(context.Context) error { return nil },
		ShutdownFunc:       func(context.Context) error { return nil },
	})

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, time.Minute, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send and check a few parts of data")
	for i := 0; i < 10; i++ {
		data := faker.Paragraph()
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
		delivered, err := manager.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 100 ms")
		s.True(delivered, "data should be delivered in 100 ms")
		sendCancel()
		for j := 0; j < 4; j++ {
			s.Equal(data, <-destination, "data should be delivered 4 times")
		}
	}

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")
}

func (s *ManagerSuite) TestRejectToRefill() {
	baseCtx := context.Background()

	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or reject after ms")
	transportSwitcher := new(atomic.Bool)
	dialers := []delivery.Dialer{
		s.transportWithReject(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, nil),
			transportSwitcher, time.Millisecond,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, time.Minute, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	data := faker.Paragraph()
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)

	s.T().Log("Switch transport to error state and send next part of data")
	transportSwitcher.Store(true)
	data = faker.Paragraph()
	sendCtx, sendCancel := context.WithTimeout(baseCtx, time.Second)
	delivered, err = manager.Send(sendCtx, data)
	sendCancel()
	s.False(delivered)
	s.NoError(err)

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, delivery.SegmentKey{ShardID: uint16(i), Segment: 1})
		s.NoError(err, "segment should be in refill")
		parts := strings.SplitN(string(segment.Bytes()), ":", 6)
		s.Equal("segment", parts[0])
		s.Equal(strconv.Itoa(i), parts[2])
		s.Equal(data, parts[5])
	}
}

func (s *ManagerSuite) TestAckRejectRace() {
	baseCtx := context.Background()

	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or reject after ms")
	transportSwitcher := new(atomic.Bool)
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{
		s.transportWithReject(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination),
			transportSwitcher, time.Millisecond,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillWriteLock := new(sync.Mutex)
	writeSegment := refill.WriteSegmentFunc
	refill.WriteSegmentFunc = func(ctx context.Context, key delivery.SegmentKey, segment delivery.Segment) error {
		refillWriteLock.Lock()
		defer refillWriteLock.Unlock()
		return writeSegment(ctx, key, segment)
	}
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, time.Minute, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	data := faker.Paragraph()
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for j := 0; j < 4; j++ {
		s.Equal(data, <-destination, "data should be delivered 4 times")
	}

	s.T().Log("Switch transport to reject state")
	transportSwitcher.Store(true)

	s.T().Log("Lock refill to prevent writing segment in refill")
	refillWriteLock.Lock()

	s.T().Log("Send next part of data")
	rejectedData := faker.Paragraph()
	sendCtx, sendCancel := context.WithTimeout(baseCtx, 10*time.Millisecond)
	delivered, err = manager.Send(sendCtx, rejectedData)
	sendCancel()
	s.False(delivered)
	s.ErrorIs(err, context.DeadlineExceeded)

	s.T().Log("Switch transport back to auto-ack state")
	transportSwitcher.Store(false)

	s.T().Log("Send next part of data")
	data = faker.Paragraph()
	delivered, err = manager.Send(baseCtx, data)
	sendCancel()
	s.True(delivered)
	s.NoError(err)
	for j := 0; j < 4; j++ {
		s.Equal(data, <-destination, "data should be delivered 4 times")
	}

	s.T().Log("Unlock refill to prevent writing segment in refill")
	refillWriteLock.Unlock()

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, delivery.SegmentKey{ShardID: uint16(i), Segment: 1})
		s.NoError(err, "segment should be in refill")
		parts := strings.SplitN(string(segment.Bytes()), ":", 6)

		s.Equal("segment", parts[0])
		s.Equal(strconv.Itoa(i), parts[2])
		s.Equal(rejectedData, parts[5])
	}

	s.T().Log("Check that exchange is empty")
	for i := 0; i < 3; i++ {
		if i == 1 {
			continue
		}
		for j := 0; j < 4; j++ {
			_, err := manager.Get(baseCtx, delivery.SegmentKey{ShardID: uint16(j), Segment: uint32(i)})
			s.Error(err)
		}
	}
}

func (s *ManagerSuite) TestRestoreFromRefill() {
	s.T().SkipNow()
	baseCtx := context.Background()

	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or return error after ms")
	transportSwitcher := new(atomic.Bool)
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{
		s.transportWithError(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination),
			transportSwitcher, time.Millisecond,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, 5*time.Second, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	data := faker.Paragraph()
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for i := 0; i < 4; i++ {
		s.Equal(data, <-destination, "data should be delivered in all destination shards")
	}

	s.T().Log("Switch transport to error state and send next part of data")
	transportSwitcher.Store(true)
	data = faker.Paragraph()
	time.AfterFunc(time.Millisecond, func() {
		clock.Advance(5 * time.Second)
	})
	delivered, err = manager.Send(baseCtx, data)
	s.False(delivered)
	s.NoError(err)

	s.T().Log("Switch transport back to auto-ack state")
	transportSwitcher.Store(false)
	for i := 0; i < 4; i++ {
		s.Equal(data, <-destination, "data should be delivered in all destination shards")
	}

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")
}

func (s *ManagerSuite) TestRestoreWithNoRefill() {
	baseCtx := context.Background()

	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or return error after ms")
	transportSwitcher := new(atomic.Bool)
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{
		s.transportWithError(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination),
			transportSwitcher, time.Millisecond,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.corruptedRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, time.Minute, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	data := faker.Paragraph()
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for i := 0; i < 4; i++ {
		s.Equal(data, <-destination, "data should be delivered in all destination shards")
	}

	s.T().Log("Switch transport to error state and send next part of data")
	transportSwitcher.Store(true)
	time.AfterFunc(10*time.Millisecond, func() {
		s.T().Log("Switch transport back to auto-ack state")
		transportSwitcher.Store(false)
	})
	data = faker.Paragraph()
	delivered, err = manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for i := 0; i < 4; i++ {
		s.Equal(data, <-destination, "data should be delivered in all destination shards")
	}

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")
}

func (s *ManagerSuite) TestNotOpened() {
	baseCtx := context.Background()

	s.T().Log("Use nil transport because it won't be used")
	dialers := []delivery.Dialer{&DialerMock{StringFunc: s.T().Name}}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, time.Minute, s.errorHandler, clock)
	s.Require().NoError(err)

	s.T().Log("Send data will fall with timeout")
	data := faker.Paragraph()
	sendCtx, sendCancel := context.WithTimeout(baseCtx, time.Millisecond)
	_, err = manager.Send(sendCtx, data)
	s.ErrorIs(err, context.DeadlineExceeded)
	sendCancel()

	clock.Advance(time.Minute + time.Millisecond)

	s.T().Log("Shutdown manager")
	baseCtx, cancel := context.WithTimeout(baseCtx, 10*time.Millisecond)
	defer cancel()
	s.NoError(manager.Shutdown(baseCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, delivery.SegmentKey{ShardID: uint16(i), Segment: 0})
		s.NoError(err, "segment should be in refill")
		parts := strings.SplitN(string(segment.Bytes()), ":", 6)
		s.Equal("segment", parts[0])
		s.Equal(strconv.Itoa(i), parts[2])
		s.Equal(data, parts[5])
	}
}

func (s *ManagerSuite) TestLongDial() {
	baseCtx := context.Background()

	s.T().Log("Use dialer that wait until context done")
	dialers := []delivery.Dialer{&DialerMock{
		StringFunc: s.T().Name,
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			<-ctx.Done()
			return nil, context.Cause(ctx)
		},
	}}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	manager, err := delivery.NewManager(baseCtx, dialers, s.simpleEncoder(), refillCtor, 2, time.Minute, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send data will be rejected")
	data := faker.Paragraph()
	sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
	time.AfterFunc(time.Millisecond, func() {
		clock.Advance(time.Minute + time.Second)
	})
	delivered, err := manager.Send(sendCtx, data)
	s.False(delivered)
	s.NoError(err)
	sendCancel()

	s.T().Log("Shutdown manager")
	baseCtx, cancel := context.WithTimeout(baseCtx, time.Millisecond)
	defer cancel()
	s.ErrorIs(manager.Shutdown(baseCtx), context.DeadlineExceeded, "manager will try to dial til the end")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, delivery.SegmentKey{ShardID: uint16(i), Segment: 0})
		if s.NoError(err, "segment should be in refill") {
			parts := strings.SplitN(string(segment.Bytes()), ":", 6)
			s.Equal("segment", parts[0])
			s.Equal(strconv.Itoa(i), parts[2])
			s.Equal(data, parts[5])
		}
	}
}

func (*ManagerSuite) transportWithReject(dialer delivery.Dialer, switcher *atomic.Bool, delay time.Duration) delivery.Dialer {
	return &DialerMock{
		StringFunc: dialer.String,
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			transport, err := dialer.Dial(ctx)
			if err != nil {
				return nil, err
			}
			m := new(sync.Mutex)
			var reject func(uint32)
			return &TransportMock{
				OnAckFunc: transport.OnAck,
				OnRejectFunc: func(fn func(uint32)) {
					m.Lock()
					defer m.Unlock()
					reject = fn
				},
				SendRestoreFunc: func(ctx context.Context, snapshot delivery.Snapshot, segments []delivery.Segment) error {
					return transport.SendRestore(ctx, snapshot, segments)
				},
				SendSegmentFunc: func(ctx context.Context, segment delivery.Segment) error {
					if !switcher.Load() {
						return transport.SendSegment(ctx, segment)
					}
					parts := strings.SplitN(string(segment.Bytes()), ":", 6)
					segmentID, err := strconv.ParseUint(parts[4], 10, 32)
					if err != nil {
						return err
					}

					time.AfterFunc(delay, func() {
						m.Lock()
						defer m.Unlock()
						reject(uint32(segmentID))
					})
					return nil
				},
				WithReaderErrorFunc: func(contextMoqParam context.Context, fn func(context.Context) error) error {
					return fn(contextMoqParam)
				},
				CloseFunc: transport.Close,
			}, nil
		},
	}
}

func (*ManagerSuite) transportWithError(dialer delivery.Dialer, switcher *atomic.Bool, delay time.Duration) delivery.Dialer {
	return &DialerMock{
		StringFunc: dialer.String,
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			transport, err := dialer.Dial(ctx)
			if err != nil {
				return nil, err
			}
			return &TransportMock{
				OnAckFunc:    transport.OnAck,
				OnRejectFunc: transport.OnReject,
				SendRestoreFunc: func(ctx context.Context, snapshot delivery.Snapshot, segments []delivery.Segment) error {
					if switcher.Load() {
						time.Sleep(delay)
						return assert.AnError
					}
					return transport.SendRestore(ctx, snapshot, segments)
				},
				SendSegmentFunc: func(ctx context.Context, segment delivery.Segment) error {
					if switcher.Load() {
						time.Sleep(delay)
						return assert.AnError
					}
					return transport.SendSegment(ctx, segment)
				},
				WithReaderErrorFunc: func(contextMoqParam context.Context, fn func(context.Context) error) error {
					return fn(contextMoqParam)
				},
				CloseFunc: transport.Close,
			}, nil
		},
	}
}

func (*ManagerSuite) transportNewAutoAck(name string, delay time.Duration, dest chan string) delivery.Dialer {
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			m := new(sync.Mutex)
			var ack func(uint32)
			var transportShard *uint64
			transport := &TransportMock{
				OnAckFunc: func(fn func(uint32)) {
					m.Lock()
					defer m.Unlock()
					ack = fn
				},
				OnRejectFunc: func(fn func(uint32)) {},
				SendRestoreFunc: func(_ context.Context, _ delivery.Snapshot, _ []delivery.Segment) error {
					return nil
				},
				SendSegmentFunc: func(_ context.Context, segment delivery.Segment) error {
					parts := strings.SplitN(string(segment.Bytes()), ":", 6)
					shardID, err := strconv.ParseUint(parts[2], 10, 16)
					if err != nil {
						return err
					}
					if transportShard == nil {
						transportShard = &shardID
					} else if *transportShard != shardID {
						return fmt.Errorf("invalid shardID: expected %d got %d", *transportShard, shardID)
					}
					segmentID, err := strconv.ParseUint(parts[4], 10, 32)
					if err != nil {
						return err
					}
					time.AfterFunc(delay, func() {
						m.Lock()
						defer m.Unlock()
						ack(uint32(segmentID))
						select {
						case dest <- parts[5]:
						default:
						}
					})
					return nil
				},
				WithReaderErrorFunc: func(contextMoqParam context.Context, fn func(context.Context) error) error {
					return fn(contextMoqParam)
				},
				CloseFunc: func() error {
					m.Lock()
					defer m.Unlock()
					ack = func(u uint32) {}
					return nil
				},
			}

			return transport, nil
		},
	}
}

func (*ManagerSuite) inMemoryRefill() *ManagerRefillMock {
	m := new(sync.Mutex)
	data := make(map[uint16]map[uint32]interface{})
	rejects := make(map[delivery.SegmentKey]bool)
	lastSegments := make(map[uint16]uint32)
	errNotFound := errors.New("not found")

	return &ManagerRefillMock{
		GetFunc: func(_ context.Context, key delivery.SegmentKey) (delivery.Segment, error) {
			m.Lock()
			defer m.Unlock()

			blob, ok := data[key.ShardID][key.Segment]
			if !ok {
				return nil, errNotFound
			}
			if segment, ok := blob.(delivery.Segment); ok {
				if !strings.Contains(string(segment.Bytes()), "snapshot:") {
					return segment, nil
				}
			}
			return nil, errNotFound
		},
		AckFunc: func(_ delivery.SegmentKey, _ string) {},
		RejectFunc: func(key delivery.SegmentKey, _ string) {
			m.Lock()
			defer m.Unlock()

			rejects[key] = true
		},
		RestoreFunc: func(_ context.Context, key delivery.SegmentKey) (delivery.Snapshot, []delivery.Segment, error) {
			m.Lock()
			defer m.Unlock()

			shard := data[key.ShardID]
			var blobs []interface{}
			i := key.Segment - 1
			for {
				blob, ok := shard[i]
				if !ok {
					break
				}
				blobs = append(blobs, blob)
				i--
			}
			if len(blobs) == 0 {
				return nil, nil, errNotFound
			}
			var snapshot delivery.Snapshot
			if s, ok := blobs[len(blobs)-1].(delivery.Snapshot); ok {
				snapshot = s
				blobs = blobs[:len(blobs)-1]
			}
			segments := make([]delivery.Segment, 0, len(blobs))
			for i := len(blobs) - 1; i >= 0; i-- {
				segments = append(segments, blobs[i].(delivery.Segment))
			}
			return snapshot, segments, nil
		},
		WriteSegmentFunc: func(_ context.Context, key delivery.SegmentKey, segment delivery.Segment) error {
			m.Lock()
			defer m.Unlock()

			if _, ok := data[key.ShardID][key.Segment-1]; key.Segment > 0 && !ok {
				return delivery.ErrSnapshotRequired
			}
			if key.Segment == 0 {
				data[key.ShardID] = make(map[uint32]interface{})
			}

			segmentCopy := &dataTest{
				data: make([]byte, len(segment.Bytes())),
			}
			copy(segmentCopy.data, segment.Bytes())

			data[key.ShardID][key.Segment] = segmentCopy
			if lastSegment, ok := lastSegments[key.ShardID]; !ok || lastSegment < key.Segment {
				lastSegments[key.ShardID] = key.Segment
			}
			return nil
		},
		WriteSnapshotFunc: func(_ context.Context, key delivery.SegmentKey, snapshot delivery.Snapshot) error {
			m.Lock()
			defer m.Unlock()

			shard, ok := data[key.ShardID]
			if !ok {
				shard = make(map[uint32]interface{})
				data[key.ShardID] = shard
			}

			snapshotCopy := &dataTest{
				data: make([]byte, len(snapshot.Bytes())),
			}
			copy(snapshotCopy.data, snapshot.Bytes())

			shard[key.Segment-1] = snapshotCopy
			return nil
		},
		WriteAckStatusFunc: func(_ context.Context) error {
			return nil
		},
		ShutdownFunc: context.Cause,
	}
}

func (*ManagerSuite) corruptedRefill() *ManagerRefillMock {
	return &ManagerRefillMock{
		GetFunc: func(_ context.Context, key delivery.SegmentKey) (delivery.Segment, error) {
			return nil, assert.AnError
		},
		AckFunc:    func(_ delivery.SegmentKey, _ string) {},
		RejectFunc: func(_ delivery.SegmentKey, _ string) {},
		RestoreFunc: func(_ context.Context, _ delivery.SegmentKey) (delivery.Snapshot, []delivery.Segment, error) {
			return nil, nil, assert.AnError
		},
		WriteSegmentFunc: func(_ context.Context, _ delivery.SegmentKey, _ delivery.Segment) error {
			return assert.AnError
		},
		WriteSnapshotFunc: func(_ context.Context, _ delivery.SegmentKey, _ delivery.Snapshot) error {
			return assert.AnError
		},
		WriteAckStatusFunc: func(_ context.Context) error {
			// return nil
			return assert.AnError
		},
		ShutdownFunc: context.Cause,
	}
}

func (*ManagerSuite) constructorForRefill(refill *ManagerRefillMock) delivery.ManagerRefillCtor {
	return func(_ context.Context, blockID uuid.UUID, destinations []string, shardsNumberPower uint8) (delivery.ManagerRefill, error) {
		if refill.BlockIDFunc == nil {
			refill.BlockIDFunc = func() uuid.UUID { return blockID }
		}
		if refill.ShardsFunc == nil {
			refill.ShardsFunc = func() int { return 1 << shardsNumberPower }
		}
		if refill.DestinationsFunc == nil {
			refill.DestinationsFunc = func() int { return len(destinations) }
		}
		if refill.LastSegmentFunc == nil {
			refill.LastSegmentFunc = func(uint16, string) uint32 { return math.MaxUint32 }
		}
		if refill.IsContinuableFunc == nil {
			refill.IsContinuableFunc = func() bool { return true }
		}
		return refill, nil
	}
}

type RedundantTest struct {
	blockID uuid.UUID
	shardID uint16
	segment uint32
}

func (*RedundantTest) PointerData() unsafe.Pointer {
	return nil
}

func (*RedundantTest) Destroy() {
}

func (*ManagerSuite) simpleEncoder() delivery.ManagerEncoderCtor {
	return func(blockID uuid.UUID, shardID uint16, shardsNumberPower uint8) (delivery.ManagerEncoder, error) {
		var nextSegmentID uint32
		shards := 1 << shardsNumberPower

		return &ManagerEncoderMock{
			LastEncodedSegmentFunc: func() uint32 { return nextSegmentID - 1 },
			EncodeFunc: func(
				_ context.Context, data delivery.ShardedData,
			) (delivery.SegmentKey, delivery.Segment, delivery.Redundant, error) {
				key := delivery.SegmentKey{
					ShardID: shardID,
					Segment: nextSegmentID,
				}
				segment := &dataTest{
					data: []byte(fmt.Sprintf(
						"segment:%s:%d:%d:%d:%+v",
						blockID, shardID, shards, nextSegmentID, data,
					)),
				}
				redundant := &RedundantTest{
					blockID: blockID,
					shardID: shardID,
					segment: nextSegmentID,
				}
				nextSegmentID++
				return key, segment, redundant, nil
			},
			SnapshotFunc: func(_ context.Context, redundants []delivery.Redundant) (delivery.Snapshot, error) {
				var lastRedundant uint32
				firstSegment := nextSegmentID
				for _, redundant := range redundants {
					dr, ok := redundant.(*RedundantTest)
					if !ok {
						return nil, fmt.Errorf("Unknown redundant type %[1]T, %+[1]v", redundant)
					}
					if dr.blockID != blockID || dr.shardID != shardID {
						return nil, fmt.Errorf(
							"Encoder-redundant mismatch: expected %s/%d, got %s/%d",
							blockID, shardID, dr.blockID, dr.shardID)
					}
					if lastRedundant != 0 && dr.segment != lastRedundant+1 {
						return nil, fmt.Errorf("Non-monothonic redundants: expected %d, got %d", lastRedundant+1, dr.segment)
					}
					lastRedundant = dr.segment
					if dr.segment < firstSegment {
						firstSegment = dr.segment
					}
				}
				if lastRedundant != 0 && lastRedundant != nextSegmentID-1 {
					return nil, fmt.Errorf("Partial redundants: expected %d, got %d", nextSegmentID, lastRedundant)
				}
				return &dataTest{data: []byte(fmt.Sprintf(
					"snapshot:%s:%d:%d:%d",
					blockID, shardID, shards, firstSegment,
				))}, nil
			},
		}, nil
	}
}

func (s *ManagerSuite) errorHandler(msg string, err error) {
	s.T().Logf("%s: %s", msg, err)
}
