package delivery_test

//go:generate moq -out manager_moq_test.go -pkg delivery_test -rm . Dialer Transport ManagerEncoder ManagerRefill RejectNotifyer

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

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/frames/framestest"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
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
		AckFunc:            func(common.SegmentKey, string) {},
		WriteAckStatusFunc: func(context.Context) error { return nil },
		ShutdownFunc:       func(context.Context) error { return nil },
	})

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send and check a few parts of data")
	for i := 0; i < 10; i++ {
		expectedData := faker.Paragraph()
		data := newShardedDataTest(expectedData)
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
		delivered, err := manager.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 100 ms")
		s.True(delivered, "data should be delivered in 100 ms")
		sendCancel()
		for j := 0; j < 4; j++ {
			s.Equal(expectedData, <-destination, "data should be delivered 4 times")
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
			transportSwitcher, time.Millisecond, "blockID", 0,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	data := newShardedDataTest(faker.Paragraph())
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)

	s.T().Log("Switch transport to error state and send next part of data")
	transportSwitcher.Store(true)
	expectedData := faker.Paragraph()
	data = newShardedDataTest(expectedData)
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
		segment, err := refill.Get(baseCtx, common.SegmentKey{ShardID: uint16(i), Segment: 1})
		if s.NoError(err, "segment should be in refill") {
			data, err := framestest.ReadPayload(segment)
			if s.NoError(err) {
				parts := strings.SplitN(string(data), ":", 6)
				s.Equal("segment", parts[0])
				s.Equal(strconv.Itoa(i), parts[2])
				s.Equal(expectedData, parts[5])
			}
		}
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
			transportSwitcher, time.Millisecond, "blockID", 0,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillWriteLock := new(sync.Mutex)
	writeSegment := refill.WriteSegmentFunc
	refill.WriteSegmentFunc = func(ctx context.Context, key common.SegmentKey, segment delivery.Segment) error {
		refillWriteLock.Lock()
		defer refillWriteLock.Unlock()
		return writeSegment(ctx, key, segment)
	}
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	expectedData := faker.Paragraph()
	data := newShardedDataTest(expectedData)
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for j := 0; j < 4; j++ {
		s.Equal(expectedData, <-destination, "data should be delivered 4 times")
	}

	s.T().Log("Switch transport to reject state")
	transportSwitcher.Store(true)

	s.T().Log("Lock refill to prevent writing segment in refill")
	refillWriteLock.Lock()

	s.T().Log("Send next part of data")
	equalRejectedData := faker.Paragraph()
	rejectedData := newShardedDataTest(equalRejectedData)
	sendCtx, sendCancel := context.WithTimeout(baseCtx, 10*time.Millisecond)
	delivered, err = manager.Send(sendCtx, rejectedData)
	sendCancel()
	s.False(delivered)
	s.ErrorIs(err, context.DeadlineExceeded)

	s.T().Log("Switch transport back to auto-ack state")
	transportSwitcher.Store(false)

	s.T().Log("Send next part of data")
	expectedData = faker.Paragraph()
	data = newShardedDataTest(expectedData)
	delivered, err = manager.Send(baseCtx, data)
	sendCancel()
	s.True(delivered)
	s.NoError(err)
	for j := 0; j < 4; j++ {
		s.Equal(expectedData, <-destination, "data should be delivered 4 times")
	}

	s.T().Log("Unlock refill to prevent writing segment in refill")
	refillWriteLock.Unlock()

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, 20*time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected and followed data is in refill")
	for i := 1; i < 3; i++ {
		for j := 0; j < 4; j++ {
			segment, err := refill.Get(baseCtx, common.SegmentKey{ShardID: uint16(j), Segment: uint32(i)})
			if s.NoError(err, "segment should be in refill") {
				data, err := framestest.ReadPayload(segment)
				if s.NoError(err) {
					parts := strings.SplitN(string(data), ":", 6)
					s.Equal("segment", parts[0], "%q")
					s.Equal(strconv.Itoa(j), parts[2])
				}
			}
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
			transportSwitcher, time.Millisecond, "blockID", 0,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		5*time.Second,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	expectedData := faker.Paragraph()
	data := newShardedDataTest(expectedData)
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for i := 0; i < 4; i++ {
		s.Equal(expectedData, <-destination, "data should be delivered in all destination shards")
	}

	s.T().Log("Switch transport to error state and send next part of data")
	transportSwitcher.Store(true)
	expectedData = faker.Paragraph()
	data = newShardedDataTest(expectedData)
	time.AfterFunc(time.Millisecond, func() {
		clock.Advance(5 * time.Second)
	})
	delivered, err = manager.Send(baseCtx, data)
	s.False(delivered)
	s.NoError(err)

	s.T().Log("Switch transport back to auto-ack state")
	transportSwitcher.Store(false)
	for i := 0; i < 4; i++ {
		s.Equal(expectedData, <-destination, "data should be delivered in all destination shards")
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
			transportSwitcher, time.Millisecond, "blockID", 0,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.corruptedRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send first part of data without an error to pre-heat manager")
	expectedData := faker.Paragraph()
	data := newShardedDataTest(expectedData)
	delivered, err := manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for i := 0; i < 4; i++ {
		s.Equal(expectedData, <-destination, "data should be delivered in all destination shards")
	}

	s.T().Log("Switch transport to error state and send next part of data")
	transportSwitcher.Store(true)
	time.AfterFunc(10*time.Millisecond, func() {
		s.T().Log("Switch transport back to auto-ack state")
		transportSwitcher.Store(false)
	})
	expectedData = faker.Paragraph()
	data = newShardedDataTest(expectedData)
	delivered, err = manager.Send(baseCtx, data)
	s.True(delivered)
	s.NoError(err)
	for i := 0; i < 4; i++ {
		s.Equal(expectedData, <-destination, "data should be delivered in all destination shards")
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
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)

	s.T().Log("Send data will fall with timeout")
	expectedData := faker.Paragraph()
	data := newShardedDataTest(expectedData)
	sendCtx, sendCancel := context.WithTimeout(baseCtx, time.Millisecond)
	_, err = manager.Send(sendCtx, data)
	s.ErrorIs(err, context.DeadlineExceeded)
	sendCancel()

	clock.Advance(time.Minute + time.Millisecond)

	s.T().Log("Shutdown manager")
	baseCtx, cancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
	defer cancel()
	s.NoError(manager.Shutdown(baseCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, common.SegmentKey{ShardID: uint16(i), Segment: 0})
		if s.NoError(err, "segment should be in refill") {
			data, err := framestest.ReadPayload(segment)
			if s.NoError(err) {
				parts := strings.SplitN(string(data), ":", 6)
				s.Equal("segment", parts[0])
				s.Equal(strconv.Itoa(i), parts[2])
				s.Equal(expectedData, parts[5])
			}
		}
	}
}

func (s *ManagerSuite) TestLongDial() {
	baseCtx := context.Background()

	s.T().Log("Use dialer that wait until context done")
	dialers := []delivery.Dialer{&DialerMock{
		StringFunc: s.T().Name,
		DialFunc: func(ctx context.Context, s string, v uint16) (delivery.Transport, error) {
			<-ctx.Done()
			return nil, context.Cause(ctx)
		},
	}}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send data will be rejected")
	expectedData := faker.Paragraph()
	data := newShardedDataTest(expectedData)
	sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
	time.AfterFunc(time.Millisecond, func() {
		clock.Advance(time.Minute + time.Second)
	})
	delivered, err := manager.Send(sendCtx, data)
	s.False(delivered)
	s.NoError(err)
	sendCancel()

	s.T().Log("Shutdown manager")
	baseCtx, cancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
	defer cancel()
	s.NoError(manager.Shutdown(baseCtx))

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, common.SegmentKey{ShardID: uint16(i), Segment: 0})
		if s.NoError(err, "segment should be in refill") {
			data, err := framestest.ReadPayload(segment)
			if s.NoError(err) {
				parts := strings.SplitN(string(data), ":", 6)
				s.Equal("segment", parts[0])
				s.Equal(strconv.Itoa(i), parts[2])
				s.Equal(expectedData, parts[5])
			}
		}
	}
}

func (s *ManagerSuite) TestAlwaysToRefill() {
	baseCtx := context.Background()

	s.T().Log("Use auto-ack transport (ack segements after ms delay)")
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{s.transportNewAutoAck(s.T().Name(), 10*time.Millisecond, destination)}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillCtor := s.constructorForRefill(refill)

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	var uncommittedTimeWindow time.Duration = delivery.AlwaysToRefill
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		uncommittedTimeWindow,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("Send and check a few parts of data")
	for i := 0; i < 10; i++ {
		expectedData := faker.Paragraph()
		data := newShardedDataTest(expectedData)
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
		delivered, err := manager.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 100 ms")
		s.False(delivered, "data should be delivered in 100 ms")
		sendCancel()
		for j := 0; j < 4; j++ {
			s.Equal(expectedData, <-destination, "data should be delivered 4 times")
		}
	}

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected and followed data is in refill")
	for i := 0; i < 3; i++ {
		for j := 0; j < 4; j++ {
			segment, err := refill.Get(baseCtx, common.SegmentKey{ShardID: uint16(j), Segment: uint32(i)})
			if s.NoError(err, "segment should be in refill") {
				data, err := framestest.ReadPayload(segment)
				if s.NoError(err) {
					parts := strings.SplitN(string(data), ":", 6)
					s.Equal("segment", parts[0], "%q")
					s.Equal(strconv.Itoa(j), parts[2])
				}
			}
		}
	}
}

func (*ManagerSuite) transportWithReject(dialer delivery.Dialer, switcher *atomic.Bool, delay time.Duration, blockID string, shardID uint16) delivery.Dialer {
	return &DialerMock{
		StringFunc: dialer.String,
		DialFunc: func(ctx context.Context, s string, v uint16) (delivery.Transport, error) {
			transport, err := dialer.Dial(ctx, blockID, shardID)
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
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame *frames.WriteFrame) error {
					if !switcher.Load() {
						return transport.Send(ctx, frame)
					}
					rf, err := framestest.ReadFrame(ctx, frame)
					if err != nil {
						return err
					}
					parts := strings.SplitN(string(rf.GetBody()), ":", 6)
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
				ListenFunc: func(ctx context.Context) {},
				CloseFunc:  transport.Close,
			}, nil
		},
	}
}

func (*ManagerSuite) transportWithError(dialer delivery.Dialer, switcher *atomic.Bool, delay time.Duration, blockID string, shardID uint16) delivery.Dialer {
	return &DialerMock{
		StringFunc: dialer.String,
		DialFunc: func(ctx context.Context, s string, v uint16) (delivery.Transport, error) {
			transport, err := dialer.Dial(ctx, blockID, shardID)
			if err != nil {
				return nil, err
			}
			return &TransportMock{
				OnAckFunc:       transport.OnAck,
				OnRejectFunc:    transport.OnReject,
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame *frames.WriteFrame) error {
					if switcher.Load() {
						time.Sleep(delay)
						return assert.AnError
					}
					return transport.Send(ctx, frame)
				},
				ListenFunc: func(ctx context.Context) {},
				CloseFunc:  transport.Close,
			}, nil
		},
	}
}

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerSuite) transportNewAutoAck(name string, delay time.Duration, dest chan string) delivery.Dialer {
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context, s string, v uint16) (delivery.Transport, error) {
			m := new(sync.Mutex)
			var ack func(uint32)
			var transportShard *uint64
			transport := &TransportMock{
				OnAckFunc: func(fn func(uint32)) {
					m.Lock()
					defer m.Unlock()
					ack = fn
				},
				OnRejectFunc:    func(fn func(uint32)) {},
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame *frames.WriteFrame) error {
					rf, err := framestest.ReadFrame(ctx, frame)
					if err != nil {
						return err
					}
					parts := strings.SplitN(string(rf.GetBody()), ":", 6)
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
					if rf.GetType() == frames.SegmentType {
						time.AfterFunc(delay, func() {
							m.Lock()
							defer m.Unlock()
							ack(uint32(segmentID))
							select {
							case dest <- parts[5]:
							default:
							}
						})
					}
					return nil
				},
				ListenFunc: func(ctx context.Context) {},
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

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerSuite) inMemoryRefill() *ManagerRefillMock {
	m := new(sync.Mutex)
	data := make(map[uint16]map[uint32]interface{})
	rejects := make(map[common.SegmentKey]bool)
	lastSegments := make(map[uint16]uint32)
	errNotFound := errors.New("not found")

	return &ManagerRefillMock{
		GetFunc: func(_ context.Context, key common.SegmentKey) (delivery.Segment, error) {
			m.Lock()
			defer m.Unlock()

			blob, ok := data[key.ShardID][key.Segment]
			if !ok {
				return nil, errNotFound
			}
			if segment, ok := blob.(common.Segment); ok {
				return segment, nil
			}
			return nil, errNotFound
		},
		AckFunc: func(_ common.SegmentKey, _ string) {},
		RejectFunc: func(key common.SegmentKey, _ string) {
			m.Lock()
			defer m.Unlock()

			rejects[key] = true
		},
		RestoreFunc: func(_ context.Context, key common.SegmentKey) (delivery.Snapshot, []delivery.Segment, error) {
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
			var snapshot common.Snapshot
			if s, ok := blobs[len(blobs)-1].(common.Snapshot); ok {
				snapshot = s
				blobs = blobs[:len(blobs)-1]
			}
			segments := make([]delivery.Segment, 0, len(blobs))
			for i := len(blobs) - 1; i >= 0; i-- {
				segments = append(segments, blobs[i].(common.Segment))
			}
			return snapshot, segments, nil
		},
		WriteSegmentFunc: func(_ context.Context, key common.SegmentKey, segment delivery.Segment) error {
			m.Lock()
			defer m.Unlock()

			if _, ok := data[key.ShardID][key.Segment-1]; key.Segment > 0 && !ok {
				return delivery.ErrSnapshotRequired
			}
			if key.Segment == 0 {
				data[key.ShardID] = make(map[uint32]interface{})
			}

			buf, err := framestest.ReadPayload(segment)
			if err != nil {
				return err
			}

			data[key.ShardID][key.Segment] = &dataTest{
				data: buf,
			}
			if lastSegment, ok := lastSegments[key.ShardID]; !ok || lastSegment < key.Segment {
				lastSegments[key.ShardID] = key.Segment
			}
			return nil
		},
		WriteSnapshotFunc: func(_ context.Context, key common.SegmentKey, snapshot delivery.Snapshot) error {
			m.Lock()
			defer m.Unlock()

			shard, ok := data[key.ShardID]
			if !ok {
				shard = make(map[uint32]interface{})
				data[key.ShardID] = shard
			}

			buf, err := framestest.ReadPayload(snapshot)
			if err != nil {
				return err
			}

			shard[key.Segment-1] = &dataTest{
				data: buf,
			}
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
		GetFunc: func(_ context.Context, key common.SegmentKey) (delivery.Segment, error) {
			return nil, assert.AnError
		},
		AckFunc:    func(_ common.SegmentKey, _ string) {},
		RejectFunc: func(_ common.SegmentKey, _ string) {},
		RestoreFunc: func(_ context.Context, _ common.SegmentKey) (delivery.Snapshot, []delivery.Segment, error) {
			return nil, nil, assert.AnError
		},
		WriteSegmentFunc: func(_ context.Context, _ common.SegmentKey, _ delivery.Segment) error {
			return assert.AnError
		},
		WriteSnapshotFunc: func(_ context.Context, _ common.SegmentKey, _ delivery.Snapshot) error {
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
	return func(_ string, blockID uuid.UUID, destinations []string, shardsNumberPower uint8, alwaysToRefill bool, registerer prometheus.Registerer) (delivery.ManagerRefill, error) {
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

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerSuite) simpleEncoder() delivery.ManagerEncoderCtor {
	return func(blockID uuid.UUID, shardID uint16, shardsNumberPower uint8) (delivery.ManagerEncoder, error) {
		var nextSegmentID uint32
		shards := 1 << shardsNumberPower
		headData := ""
		mx := new(sync.Mutex)

		return &ManagerEncoderMock{
			LastEncodedSegmentFunc: func() uint32 { return nextSegmentID - 1 },
			EncodeFunc: func(
				_ context.Context, data common.ShardedData,
			) (common.SegmentKey, common.Segment, common.Redundant, error) {
				key := common.SegmentKey{
					ShardID: shardID,
					Segment: nextSegmentID,
				}
				segment := &dataTest{
					data: []byte(fmt.Sprintf(
						"segment:%s:%d:%d:%d:%+v",
						blockID, shardID, shards, nextSegmentID, data.(*shardedDataTest).data,
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
			SnapshotFunc: func(_ context.Context, redundants []common.Redundant) (common.Snapshot, error) {
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
			DestroyFunc: func() {},
			AddFunc: func(_ context.Context, shardedData common.ShardedData) (common.Segment, error) {
				mx.Lock()
				defer mx.Unlock()
				headData += shardedData.(*shardedDataTest).data

				return &dataTest{}, nil
			},
			FinalizeFunc: func(_ context.Context) (common.SegmentKey, common.Segment, common.Redundant, error) {
				key := common.SegmentKey{
					ShardID: shardID,
					Segment: nextSegmentID,
				}
				mx.Lock()
				data := headData
				headData = ""
				mx.Unlock()
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
		}, nil
	}
}

func (s *ManagerSuite) errorHandler(msg string, err error) {
	s.T().Logf("%s: %s", msg, err)
}

func (s *ManagerSuite) TestSend2WithAck() {
	baseCtx := context.Background()

	s.T().Log("Use auto-ack transport (ack segements after ms delay)")
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination)}

	s.T().Log("Use no-op refill: assumed that it won't be touched")
	refillCtor := s.constructorForRefill(&ManagerRefillMock{
		AckFunc:            func(common.SegmentKey, string) {},
		WriteAckStatusFunc: func(context.Context) error { return nil },
		ShutdownFunc:       func(context.Context) error { return nil },
	})

	s.T().Log("Instance and open manager")
	clock := clockwork.NewFakeClock()
	rejectNotifyer := &RejectNotifyerMock{NotifyOnRejectFunc: func() {}}
	haTracker := delivery.NewHighAvailabilityTracker(baseCtx, nil, clock)
	defer haTracker.Destroy()
	manager, err := delivery.NewManager(
		baseCtx,
		dialers,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		2,
		time.Minute,
		"workingDir",
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)
	manager.Open(baseCtx)
	s.T().Log("Send and check a few parts of data")
	group, gCtx := errgroup.WithContext(baseCtx)
	for i := 0; i < 10; i++ {
		group.Go(func() error {
			data := newShardedDataTest(faker.Paragraph())
			sendCtx, sendCancel := context.WithTimeout(gCtx, 4000*time.Millisecond)
			delivered, errSend := manager.SendOpenHead(sendCtx, data)
			sendCancel()
			if errSend != nil {
				return errSend
			}
			s.True(delivered, "data should be delivered in 2600 ms")
			return nil
		})
	}
	time.AfterFunc(
		100*time.Millisecond,
		func() {
			clock.Advance(2500 * time.Millisecond)
		},
	)
	err = group.Wait()
	s.NoError(err, "data should be delivered in 2600 ms")
	j := 0
	for ; j < 4; j++ {
		select {
		case <-destination:
		default:
		}
	}
	s.Equal(4, j)

	s.T().Log("Shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")
}
