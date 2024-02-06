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

	"github.com/odarix/odarix-core-go/cppbridge"
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
		AckFunc:            func(cppbridge.SegmentKey, string) {},
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")
}

func (s *ManagerSuite) TestRejectToRefill() {
	baseCtx := context.Background()

	shardMeta := delivery.ShardMeta{
		BlockID:                uuid.New(),
		ShardID:                0,
		ShardsLog:              0,
		SegmentEncodingVersion: 1,
	}
	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or reject after ms")
	transportSwitcher := new(atomic.Bool)
	dialers := []delivery.Dialer{
		s.transportWithReject(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, nil),
			transportSwitcher, time.Millisecond, shardMeta,
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, cppbridge.SegmentKey{ShardID: uint16(i), Segment: 1})
		if s.NoError(err, "segment should be in refill") {
			data, err := framestest.ReadPayload(segment)
			if s.NoError(err) {
				parts := strings.SplitN(string(data), ":", 5)
				s.Equal("segment", parts[0])
				s.Equal(strconv.Itoa(i), parts[1])
				s.Equal(expectedData, parts[4])
			}
		}
	}
}

func (s *ManagerSuite) TestAckRejectRace() {
	baseCtx := context.Background()

	shardMeta := delivery.ShardMeta{
		BlockID:                uuid.New(),
		ShardID:                0,
		ShardsLog:              0,
		SegmentEncodingVersion: 1,
	}
	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or reject after ms")
	transportSwitcher := new(atomic.Bool)
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{
		s.transportWithReject(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination),
			transportSwitcher, time.Millisecond, shardMeta,
		),
	}

	s.T().Log("Use full-implemented refill in memory")
	refill := s.inMemoryRefill()
	refillWriteLock := new(sync.Mutex)
	writeSegment := refill.WriteSegmentFunc
	refill.WriteSegmentFunc = func(ctx context.Context, key cppbridge.SegmentKey, segment delivery.Segment) error {
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected and followed data is in refill")
	for i := 1; i < 3; i++ {
		for j := 0; j < 4; j++ {
			segment, err := refill.Get(baseCtx, cppbridge.SegmentKey{ShardID: uint16(j), Segment: uint32(i)})
			if s.NoError(err, "segment should be in refill") {
				data, err := framestest.ReadPayload(segment)
				if s.NoError(err) {
					parts := strings.SplitN(string(data), ":", 5)
					s.Equal("segment", parts[0], "%q")
					s.Equal(strconv.Itoa(j), parts[1])
				}
			}
		}
	}
}

func (s *ManagerSuite) TestRestoreFromRefill() {
	s.T().SkipNow()
	baseCtx := context.Background()

	shardMeta := delivery.ShardMeta{
		BlockID:                uuid.New(),
		ShardID:                0,
		ShardsLog:              0,
		SegmentEncodingVersion: 1,
	}
	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or return error after ms")
	transportSwitcher := new(atomic.Bool)
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{
		s.transportWithError(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination),
			transportSwitcher, time.Millisecond, shardMeta,
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.Equal("final", <-destination, "failed final frame")
}

func (s *ManagerSuite) TestRestoreWithNoRefill() {
	baseCtx := context.Background()

	shardMeta := delivery.ShardMeta{
		BlockID:                uuid.New(),
		ShardID:                0,
		ShardsLog:              0,
		SegmentEncodingVersion: 1,
	}
	s.T().Log("Use transport that may be in 2 states: auto-ack after ms or return error after ms")
	transportSwitcher := new(atomic.Bool)
	destination := make(chan string, 4)
	dialers := []delivery.Dialer{
		s.transportWithError(
			s.transportNewAutoAck(s.T().Name(), time.Millisecond, destination),
			transportSwitcher, time.Millisecond, shardMeta,
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.Equal("final", <-destination, "failed final frame")
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(baseCtx), "manager should be gracefully stopped")

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, cppbridge.SegmentKey{ShardID: uint16(i), Segment: 0})
		if s.NoError(err, "segment should be in refill") {
			data, err := framestest.ReadPayload(segment)
			if s.NoError(err) {
				parts := strings.SplitN(string(data), ":", 5)
				s.Equal("segment", parts[0])
				s.Equal(strconv.Itoa(i), parts[1])
				s.Equal(expectedData, parts[4])
			}
		}
	}
}

func (s *ManagerSuite) TestLongDial() {
	baseCtx := context.Background()

	s.T().Log("Use dialer that wait until context done")
	dialers := []delivery.Dialer{&DialerMock{
		StringFunc: s.T().Name,
		DialFunc: func(ctx context.Context, shardMeta delivery.ShardMeta) (delivery.Transport, error) {
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(baseCtx))

	s.T().Log("Check that rejected data is in refill")
	for i := 0; i < 4; i++ {
		segment, err := refill.Get(baseCtx, cppbridge.SegmentKey{ShardID: uint16(i), Segment: 0})
		if s.NoError(err, "segment should be in refill") {
			data, err := framestest.ReadPayload(segment)
			if s.NoError(err) {
				parts := strings.SplitN(string(data), ":", 5)
				s.Equal("segment", parts[0])
				s.Equal(strconv.Itoa(i), parts[1])
				s.Equal(expectedData, parts[4])
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
		testHashdexFactory{},
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")

	s.Equal("final", <-destination, "failed final frame")

	s.T().Log("Check that rejected and followed data is in refill")
	for i := 0; i < 3; i++ {
		for j := 0; j < 4; j++ {
			segment, err := refill.Get(baseCtx, cppbridge.SegmentKey{ShardID: uint16(j), Segment: uint32(i)})
			if s.NoError(err, "segment should be in refill") {
				data, err := framestest.ReadPayload(segment)
				if s.NoError(err) {
					parts := strings.SplitN(string(data), ":", 5)
					s.Equal("segment", parts[0], "%q")
					s.Equal(strconv.Itoa(j), parts[1])
				}
			}
		}
	}
}

func (*ManagerSuite) transportWithReject(
	dialer delivery.Dialer,
	switcher *atomic.Bool,
	delay time.Duration,
	shardMeta delivery.ShardMeta,
) delivery.Dialer {
	return &DialerMock{
		StringFunc: dialer.String,
		DialFunc: func(ctx context.Context, _ delivery.ShardMeta) (delivery.Transport, error) {
			transport, err := dialer.Dial(ctx, shardMeta)
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
				SendFunc: func(ctx context.Context, frame frames.FrameWriter) error {
					if !switcher.Load() {
						return transport.Send(ctx, frame)
					}
					rs, err := framestest.ReadSegment(ctx, frame)
					if err != nil {
						return err
					}
					if rs.GetSize() == 0 {
						// Final
						return nil
					}

					parts := strings.SplitN(string(rs.GetBody()), ":", 5)
					segmentID, err := strconv.ParseUint(parts[3], 10, 32)
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

func (*ManagerSuite) transportWithError(
	dialer delivery.Dialer,
	switcher *atomic.Bool,
	delay time.Duration,
	shardMeta delivery.ShardMeta,
) delivery.Dialer {
	return &DialerMock{
		StringFunc: dialer.String,
		DialFunc: func(ctx context.Context, _ delivery.ShardMeta) (delivery.Transport, error) {
			transport, err := dialer.Dial(ctx, shardMeta)
			if err != nil {
				return nil, err
			}
			return &TransportMock{
				OnAckFunc:       transport.OnAck,
				OnRejectFunc:    transport.OnReject,
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame frames.FrameWriter) error {
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
		DialFunc: func(ctx context.Context, shardMeta delivery.ShardMeta) (delivery.Transport, error) {
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
				SendFunc: func(ctx context.Context, frame frames.FrameWriter) error {
					rs, err := framestest.ReadSegment(ctx, frame)
					if err != nil {
						return err
					}
					if rs.GetSize() == 0 {
						dest <- "final"
						return nil
					}
					parts := strings.SplitN(string(rs.GetBody()), ":", 5)
					shardID, err := strconv.ParseUint(parts[1], 10, 16)
					if err != nil {
						return err
					}
					if transportShard == nil {
						transportShard = &shardID
					} else if *transportShard != shardID {
						return fmt.Errorf("invalid shardID: expected %d got %d", *transportShard, shardID)
					}
					segmentID, err := strconv.ParseUint(parts[3], 10, 32)
					if err != nil {
						return err
					}

					time.AfterFunc(delay, func() {
						m.Lock()
						defer m.Unlock()
						ack(uint32(segmentID))
						select {
						case dest <- parts[4]:
						default:
						}
					})
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
	rejects := make(map[cppbridge.SegmentKey]bool)
	lastSegments := make(map[uint16]uint32)
	errNotFound := errors.New("not found")

	return &ManagerRefillMock{
		GetFunc: func(_ context.Context, key cppbridge.SegmentKey) (delivery.Segment, error) {
			m.Lock()
			defer m.Unlock()

			blob, ok := data[key.ShardID][key.Segment]
			if !ok {
				return nil, errNotFound
			}
			if segment, ok := blob.(cppbridge.Segment); ok {
				return segment, nil
			}
			return nil, errNotFound
		},
		AckFunc: func(_ cppbridge.SegmentKey, _ string) {},
		RejectFunc: func(key cppbridge.SegmentKey, _ string) {
			m.Lock()
			defer m.Unlock()

			rejects[key] = true
		},
		WriteSegmentFunc: func(_ context.Context, key cppbridge.SegmentKey, segment delivery.Segment) error {
			m.Lock()
			defer m.Unlock()

			if data[key.ShardID] == nil {
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
		WriteAckStatusFunc: func(_ context.Context) error {
			return nil
		},
		IntermediateRenameFunc: func() error { return nil },
		ShutdownFunc:           context.Cause,
	}
}

func (*ManagerSuite) corruptedRefill() *ManagerRefillMock {
	return &ManagerRefillMock{
		GetFunc: func(_ context.Context, key cppbridge.SegmentKey) (delivery.Segment, error) {
			return nil, assert.AnError
		},
		AckFunc:    func(_ cppbridge.SegmentKey, _ string) {},
		RejectFunc: func(_ cppbridge.SegmentKey, _ string) {},
		WriteSegmentFunc: func(_ context.Context, _ cppbridge.SegmentKey, _ delivery.Segment) error {
			return assert.AnError
		},
		WriteAckStatusFunc: func(_ context.Context) error {
			// return nil
			return assert.AnError
		},
		ShutdownFunc: context.Cause,
	}
}

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerSuite) constructorForRefill(refill *ManagerRefillMock) delivery.ManagerRefillCtor {
	return func(_ string, blockID uuid.UUID, destinations []string, shardsNumberPower uint8, segmentEncodingVersion uint8, alwaysToRefill bool, registerer prometheus.Registerer) (delivery.ManagerRefill, error) {
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
		if refill.IntermediateRenameFunc == nil {
			refill.IntermediateRenameFunc = func() error { return nil }
		}
		return refill, nil
	}
}

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerSuite) simpleEncoder() delivery.ManagerEncoderCtor {
	return func(shardID uint16, shardsNumberPower uint8) delivery.ManagerEncoder {
		var nextSegmentID uint32
		shards := 1 << shardsNumberPower
		headData := ""
		mx := new(sync.Mutex)

		return &ManagerEncoderMock{
			LastEncodedSegmentFunc: func() uint32 { return nextSegmentID - 1 },
			EncodeFunc: func(
				_ context.Context, data cppbridge.ShardedData,
			) (cppbridge.SegmentKey, cppbridge.Segment, error) {
				key := cppbridge.SegmentKey{
					ShardID: shardID,
					Segment: nextSegmentID,
				}
				segment := &dataTest{
					data: []byte(fmt.Sprintf(
						"segment:%d:%d:%d:%+v",
						shardID, shards, nextSegmentID, data.(*shardedDataTest).data,
					)),
				}
				nextSegmentID++
				return key, segment, nil
			},
			AddFunc: func(_ context.Context, shardedData cppbridge.ShardedData) (cppbridge.SegmentStats, error) {
				mx.Lock()
				defer mx.Unlock()
				headData += shardedData.(*shardedDataTest).data

				return &dataTest{}, nil
			},
			FinalizeFunc: func(_ context.Context) (cppbridge.SegmentKey, cppbridge.Segment, error) {
				key := cppbridge.SegmentKey{
					ShardID: shardID,
					Segment: nextSegmentID,
				}
				mx.Lock()
				data := headData
				headData = ""
				mx.Unlock()
				segment := &dataTest{
					data: []byte(fmt.Sprintf(
						"segment:%d:%d:%d:%+v",
						shardID, shards, nextSegmentID, data,
					)),
				}
				nextSegmentID++
				return key, segment, nil
			},
		}
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
		AckFunc:            func(cppbridge.SegmentKey, string) {},
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
		testHashdexFactory{},
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
			delivered, errSend := manager.SendOpenHeadProtobuf(sendCtx, data)
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
	s.NoError(manager.Close(), "manager should be gracefully close")
	s.NoError(manager.Shutdown(shutdownCtx), "manager should be gracefully stopped")
}
