package delivery_test

//go:generate moq -out delivery_moq_test.go -pkg delivery_test -rm . ManagerRefillSender

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/delivery"
)

type ManagerKeeperSuite struct {
	suite.Suite
}

func TestManagerKeeper(t *testing.T) {
	suite.Run(t, new(ManagerKeeperSuite))
}

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerKeeperSuite) transportNewAutoAck(name string, delay time.Duration, dest chan string) delivery.Dialer {
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
				OnRejectFunc:    func(fn func(uint32)) {},
				OnReadErrorFunc: func(fn func(error)) {},
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

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerKeeperSuite) transportWithReject(name string, delay time.Duration, dest chan string) delivery.Dialer {
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			m := new(sync.Mutex)
			var ack func(uint32)
			var reject func(uint32)
			switcher := func(segmentID uint32) {
				if segmentID%2 != 0 {
					reject(segmentID)
					return
				}
				ack(segmentID)
			}

			var transportShard *uint64
			transport := &TransportMock{
				OnAckFunc: func(fn func(uint32)) {
					m.Lock()
					defer m.Unlock()
					ack = fn
				},
				OnRejectFunc: func(fn func(uint32)) {
					m.Lock()
					defer m.Unlock()
					reject = fn
				},
				OnReadErrorFunc: func(fn func(err error)) {},
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
						switcher(uint32(segmentID))
						select {
						case dest <- parts[5]:
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
					reject = func(u uint32) {}
					return nil
				},
			}

			return transport, nil
		},
	}
}

func (*ManagerKeeperSuite) constructorForRefill(refill *ManagerRefillMock) delivery.ManagerRefillCtor {
	return func(_ context.Context, blockID uuid.UUID, destinations []string, shardsNumberPower uint8, registerer prometheus.Registerer) (delivery.ManagerRefill, error) {
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

func (*ManagerKeeperSuite) constructorForRefillSender(mrs *ManagerRefillSenderMock) delivery.MangerRefillSenderCtor {
	return func(cfg *delivery.RefillSendManagerConfig, dialers []delivery.Dialer, errorHandler delivery.ErrorHandler, clock clockwork.Clock, registerer prometheus.Registerer) (delivery.ManagerRefillSender, error) {
		if mrs.RunFunc == nil {
			mrs.RunFunc = func(ctx context.Context) {
				<-ctx.Done()
				if !errors.Is(context.Cause(ctx), delivery.ErrShutdown) {
					errorHandler("scan and send loop context canceled", context.Cause(ctx))
				}
			}
		}
		if mrs.ShutdownFunc == nil {
			mrs.ShutdownFunc = func(ctx context.Context) error {
				if ctx.Err() != nil && !errors.Is(context.Cause(ctx), delivery.ErrShutdown) {
					errorHandler("scan and send loop context canceled", context.Cause(ctx))
					return context.Cause(ctx)
				}

				return nil
			}
		}
		return mrs, nil
	}
}

//revive:disable-next-line:cognitive-complexity this is test
func (*ManagerKeeperSuite) simpleEncoder() delivery.ManagerEncoderCtor {
	return func(blockID uuid.UUID, shardID uint16, shardsNumberPower uint8) (delivery.ManagerEncoder, error) {
		var nextSegmentID uint32
		shards := 1 << shardsNumberPower

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
						return nil, fmt.Errorf("Unknown redundant type %[1]T, %[1]v", redundant)
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
		}, nil
	}
}

//revive:disable-next-line:cognitive-complexity this is test
//revive:disable-next-line:cyclomatic this is test
func (*ManagerKeeperSuite) inMemoryRefill() *ManagerRefillMock {
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
				if !strings.Contains(string(segment.Bytes()), "snapshot:") {
					return segment, nil
				}
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
			var snapshot delivery.Snapshot
			if s, ok := blobs[len(blobs)-1].(delivery.Snapshot); ok {
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
		WriteSnapshotFunc: func(_ context.Context, key common.SegmentKey, snapshot delivery.Snapshot) error {
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
		IsContinuableFunc:      func() bool { return false },
		IntermediateRenameFunc: func() error { return nil },
		ShutdownFunc:           context.Cause,
	}
}

func (s *ManagerKeeperSuite) errorHandler(msg string, err error) {
	s.T().Logf("%s: %s: %s", s.T().Name(), msg, err)
}

func (s *ManagerKeeperSuite) TestSendHappyPath() {
	baseCtx := context.Background()

	s.T().Log("use auto-ack transport (ack segements after ms delay), default 1 shards")
	destination := make(chan string, 1)
	dialers := []delivery.Dialer{s.transportNewAutoAck(s.T().Name(), 50*time.Millisecond, destination)}

	s.T().Log("use no-op refill: assumed that it won't be touched")
	refillCtor := s.constructorForRefill(&ManagerRefillMock{
		AckFunc:                func(common.SegmentKey, string) {},
		WriteAckStatusFunc:     func(context.Context) error { return nil },
		IntermediateRenameFunc: func() error { return nil },
		ShutdownFunc:           func(context.Context) error { return nil },
	})

	mangerRefillSenderCtor := s.constructorForRefillSender(&ManagerRefillSenderMock{})

	cfg := &delivery.ManagerKeeperConfig{
		RotateInterval:       5 * time.Second,
		RefillInterval:       5 * time.Second,
		RejectRotateInterval: 2 * time.Second,
		ShutdownTimeout:      time.Second,
		RefillSenderManager: &delivery.RefillSendManagerConfig{
			Dir:           "/tmp/refill",
			ScanInterval:  3 * time.Second,
			MaxRefillSize: 10000,
		},
	}

	clock := clockwork.NewFakeClock()

	s.T().Log("instance manager keeper")
	managerKeeper, err := delivery.NewManagerKeeper(
		baseCtx,
		cfg,
		delivery.NewManager,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		mangerRefillSenderCtor,
		clock,
		dialers,
		s.errorHandler,
		nil,
	)
	s.Require().NoError(err)

	s.T().Log("send and check a few parts of data")
	var delivered bool
	for i := 0; i < 10; i++ {
		expectedData := faker.Paragraph()
		data := newShardedDataTest(expectedData)
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
		delivered, err = managerKeeper.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 100 ms")
		s.True(delivered, "data should be delivered in 100 ms")
		sendCancel()
		s.Equal(expectedData, <-destination, "data should be delivered 1 times(1 shard)")
	}

	s.T().Log("shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, time.Second)
	defer shutdownCancel()

	err = managerKeeper.Shutdown(shutdownCtx)
	s.NoError(err)
}

func (s *ManagerKeeperSuite) TestSendWithRotate() {
	baseCtx := context.Background()

	s.T().Log("use auto-ack transport (ack segements after ms delay), default 1 shards")
	destination := make(chan string, 1)
	dialers := []delivery.Dialer{s.transportNewAutoAck(s.T().Name(), 50*time.Millisecond, destination)}

	s.T().Log("use no-op refill: assumed that it won't be touched")
	refillCtor := s.constructorForRefill(&ManagerRefillMock{
		AckFunc:                func(common.SegmentKey, string) {},
		WriteAckStatusFunc:     func(context.Context) error { return nil },
		IntermediateRenameFunc: func() error { return nil },
		ShutdownFunc:           func(context.Context) error { return nil },
	})

	mangerRefillSenderCtor := s.constructorForRefillSender(&ManagerRefillSenderMock{})

	cfg := &delivery.ManagerKeeperConfig{
		RotateInterval:       2 * time.Second,
		RefillInterval:       5 * time.Second,
		RejectRotateInterval: 2 * time.Second,
		ShutdownTimeout:      time.Second,
		RefillSenderManager: &delivery.RefillSendManagerConfig{
			Dir:           "/tmp/refill",
			ScanInterval:  3 * time.Second,
			MaxRefillSize: 10000,
		},
	}

	clock := clockwork.NewFakeClock()

	s.T().Log("instance manager keeper")
	managerKeeper, err := delivery.NewManagerKeeper(
		baseCtx,
		cfg,
		delivery.NewManager,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		mangerRefillSenderCtor,
		clock,
		dialers,
		s.errorHandler,
		nil,
	)
	s.Require().NoError(err)

	time.AfterFunc(
		300*time.Millisecond,
		func() {
			s.T().Log("first rotate")
			clock.Advance(2 * time.Second)
		},
	)

	time.AfterFunc(
		600*time.Millisecond,
		func() {
			s.T().Log("second rotate")
			clock.Advance(2 * time.Second)
		},
	)

	s.T().Log("send and check a few parts of data")
	var delivered bool
	for i := 0; i < 15; i++ {
		expectedData := faker.Paragraph()
		data := newShardedDataTest(expectedData)
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
		delivered, err = managerKeeper.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 100 ms")
		check := s.True(delivered, "data should be delivered in 100 ms")
		sendCancel()
		if check {
			s.Equal(expectedData, <-destination, "data should be delivered 1 times(1 shard)")
		}
	}

	s.T().Log("shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer shutdownCancel()

	err = managerKeeper.Shutdown(shutdownCtx)
	s.NoError(err)
}

func (s *ManagerKeeperSuite) TestSendWithReject() {
	baseCtx := context.Background()

	s.T().Log("use auto-ack transport (ack segements after ms delay), default 1 shards")
	destination := make(chan string, 1)
	dialers := []delivery.Dialer{s.transportWithReject(s.T().Name(), 50*time.Millisecond, destination)}

	s.T().Log("Use full-implemented refill in memory")
	refillCtor := s.constructorForRefill(s.inMemoryRefill())

	mangerRefillSenderCtor := s.constructorForRefillSender(&ManagerRefillSenderMock{})

	cfg := &delivery.ManagerKeeperConfig{
		RotateInterval:       2 * time.Second,
		RefillInterval:       5 * time.Second,
		RejectRotateInterval: 2 * time.Second,
		ShutdownTimeout:      time.Second,
		RefillSenderManager: &delivery.RefillSendManagerConfig{
			Dir:           "/tmp/refill",
			ScanInterval:  3 * time.Second,
			MaxRefillSize: 10000,
		},
	}

	clock := clockwork.NewFakeClock()

	s.T().Log("instance manager keeper")
	managerKeeper, err := delivery.NewManagerKeeper(
		baseCtx,
		cfg,
		delivery.NewManager,
		newByteShardedDataTest,
		s.simpleEncoder(),
		refillCtor,
		mangerRefillSenderCtor,
		clock,
		dialers,
		s.errorHandler,
		nil,
	)
	s.Require().NoError(err)

	time.AfterFunc(
		300*time.Millisecond,
		func() {
			s.T().Log("first rotate")
			clock.Advance(2 * time.Second)
		},
	)

	time.AfterFunc(
		600*time.Millisecond,
		func() {
			s.T().Log("second rotate")
			clock.Advance(2 * time.Second)
		},
	)

	s.T().Log("send and check a few parts of data")
	for i := 0; i < 15; i++ {
		expectedData := faker.Paragraph()
		data := newShardedDataTest(expectedData)
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 100*time.Millisecond)
		_, err = managerKeeper.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 100 ms")
		sendCancel()
		s.Equal(expectedData, <-destination, "data should be delivered 1 times(1 shard)")
	}

	s.T().Log("shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer shutdownCancel()

	err = managerKeeper.Shutdown(shutdownCtx)
	s.NoError(err)
}

func TestRotateTimer(t *testing.T) {
	delayAfterNotify := 4 * time.Second
	durationBlock := 10 * time.Second
	clock := clockwork.NewFakeClock()
	rt := delivery.NewRotateTimer(clock, durationBlock, delayAfterNotify)

	t.Log("notify tick")
	rt.NotifyOnReject()
	clock.Advance(delayAfterNotify)
	loop := <-rt.Chan()
	assert.Zero(t, clock.Since(loop))

	t.Log("main tick")
	rt.Reset()
	clock.Advance(durationBlock)
	loop = <-rt.Chan()
	assert.Zero(t, clock.Since(loop))

	t.Log("2 notify tick")
	rt.Reset()
	rt.NotifyOnReject()
	clock.Advance(delayAfterNotify / 2)
	rt.NotifyOnReject()
	clock.Advance(delayAfterNotify)
	loop = <-rt.Chan()
	assert.Zero(t, clock.Since(loop))

	t.Log("main tick")
	rt.Reset()
	var stop bool
	i := 0
	for ; i < 20 && !stop; i++ {
		rt.NotifyOnReject()
		clock.Advance(delayAfterNotify / 2)
		select {
		case loop = <-rt.Chan():
			stop = true
		default:
		}
	}
	assert.Equal(t, 5, i)
	assert.Zero(t, clock.Since(loop))

	t.Log("main tick stopped")
	rt.Reset()
	rt.Stop()
	clock.Advance(durationBlock)
	select {
	case loop = <-rt.Chan():
	default:
	}
	assert.NotZero(t, clock.Since(loop))
}
