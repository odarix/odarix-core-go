package delivery_test

//go:generate moq -out manager_keeper_moq_test.go -pkg delivery_test -rm . ManagerRefillSender

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/frames/framestest"
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
		DialFunc: func(ctx context.Context, s string, v uint16) (delivery.Transport, error) {
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
	return func(_ string, blockID uuid.UUID, destinations []string, shardsNumberPower uint8, registerer prometheus.Registerer) (delivery.ManagerRefill, error) {
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
	return func(cfg delivery.RefillSendManagerConfig, workingDir string, dialers []delivery.Dialer, errorHandler delivery.ErrorHandler, clock clockwork.Clock, registerer prometheus.Registerer) (delivery.ManagerRefillSender, error) {
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
				buf, _ := framestest.ReadPayload(segment)
				if !strings.Contains(string(buf), "snapshot:") {
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
		IsContinuableFunc:      func() bool { return false },
		IntermediateRenameFunc: func() error { return nil },
		ShutdownFunc:           context.Cause,
	}
}

func (s *ManagerKeeperSuite) errorHandler(msg string, err error) {
	s.T().Logf("%s: %s: %s", s.T().Name(), msg, err)
}

func (*ManagerKeeperSuite) mkDir() (string, error) {
	return os.MkdirTemp("", filepath.Clean("refill-"))
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
	dir, err := s.mkDir()
	s.Require().NoError(err)

	cfg := delivery.ManagerKeeperConfig{
		ShutdownTimeout: 6 * time.Second,
		RefillInterval:  5 * time.Second,
		WorkingDir:      dir,
		RefillSenderManager: delivery.RefillSendManagerConfig{
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

	err = os.RemoveAll(filepath.Clean(dir))
	s.Require().NoError(err)
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
	dir, err := s.mkDir()
	s.Require().NoError(err)

	cfg := delivery.ManagerKeeperConfig{
		ShutdownTimeout: 6 * time.Second,
		RefillInterval:  5 * time.Second,
		WorkingDir:      dir,
		RefillSenderManager: delivery.RefillSendManagerConfig{
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
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 200*time.Millisecond)
		delivered, err = managerKeeper.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 200 ms")
		check := s.True(delivered, "data should be delivered in 200 ms")
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

	err = os.RemoveAll(filepath.Clean(dir))
	s.Require().NoError(err)
}

func (s *ManagerKeeperSuite) TestSendWithReject() {
	baseCtx := context.Background()

	s.T().Log("use auto-ack transport (ack segements after ms delay), default 1 shards")
	destination := make(chan string, 1)
	dialers := []delivery.Dialer{s.transportWithReject(s.T().Name(), 50*time.Millisecond, destination)}

	s.T().Log("Use full-implemented refill in memory")
	refillCtor := s.constructorForRefill(s.inMemoryRefill())

	mangerRefillSenderCtor := s.constructorForRefillSender(&ManagerRefillSenderMock{})
	dir, err := s.mkDir()
	s.Require().NoError(err)

	cfg := delivery.ManagerKeeperConfig{
		ShutdownTimeout: 6 * time.Second,
		RefillInterval:  5 * time.Second,
		WorkingDir:      dir,
		RefillSenderManager: delivery.RefillSendManagerConfig{
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
		sendCtx, sendCancel := context.WithTimeout(baseCtx, 300*time.Millisecond)
		_, err = managerKeeper.Send(sendCtx, data)
		s.NoError(err, "data should be delivered in 300 ms")
		sendCancel()
		s.Equal(expectedData, <-destination, "data should be delivered 1 times(1 shard)")
	}

	s.T().Log("shutdown manager")
	shutdownCtx, shutdownCancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer shutdownCancel()

	err = managerKeeper.Shutdown(shutdownCtx)
	s.NoError(err)

	err = os.RemoveAll(filepath.Clean(dir))
	s.Require().NoError(err)
}

type RotateTimerSuite struct {
	suite.Suite

	cfg delivery.BlockLimits
}

func TestRotateTimer(t *testing.T) {
	suite.Run(t, new(RotateTimerSuite))
}

func (s *RotateTimerSuite) SetupSuite() {
	s.cfg = delivery.BlockLimits{
		DesiredBlockFormationDuration: 10 * time.Second,
		DelayAfterNotify:              4 * time.Second,
	}
}

func (s *RotateTimerSuite) TestMainTickWithNotify() {
	clock := clockwork.NewFakeClock()
	rt := delivery.NewRotateTimer(clock, s.cfg)
	s.T().Log("notify tick")
	rt.NotifyOnReject()
	clock.Advance(s.cfg.DelayAfterNotify)
	loop := <-rt.Chan()
	s.Zero(clock.Since(loop))

	s.T().Log("main tick")
	rt.Reset()
	clock.Advance(s.cfg.DesiredBlockFormationDuration)
	loop = <-rt.Chan()
	s.Zero(clock.Since(loop))
}

func (s *RotateTimerSuite) TestMainTickWith2Notify() {
	clock := clockwork.NewFakeClock()
	rt := delivery.NewRotateTimer(clock, s.cfg)
	s.T().Log("2 notify tick")
	rt.NotifyOnReject()
	clock.Advance(s.cfg.DelayAfterNotify / 2)
	rt.NotifyOnReject()
	clock.Advance(s.cfg.DelayAfterNotify)
	loop := <-rt.Chan()
	s.Zero(clock.Since(loop))
}

func (s *RotateTimerSuite) TestMainTick() {
	clock := clockwork.NewFakeClock()
	rt := delivery.NewRotateTimer(clock, s.cfg)
	s.T().Log("main tick")
	stop := atomic.Bool{}
	go func() {
		for i := 0; i < 20 && !stop.Load(); i++ {
			time.Sleep(time.Millisecond)
			rt.NotifyOnReject()
			clock.Advance(s.cfg.DelayAfterNotify / 2)
		}
	}()
	loop := <-rt.Chan()
	stop.Store(true)
	s.Zero(clock.Since(loop))
}

func (s *RotateTimerSuite) TestMainTickStopped() {
	clock := clockwork.NewFakeClock()
	rt := delivery.NewRotateTimer(clock, s.cfg)
	s.T().Log("main tick stopped")
	rt.Stop()
	clock.Advance(s.cfg.DesiredBlockFormationDuration)
	var loop time.Time
	select {
	case loop = <-rt.Chan():
	default:
	}
	s.NotZero(clock.Since(loop))
}

type CurrentStateSuite struct {
	suite.Suite

	dir string
}

func TestCurrentState(t *testing.T) {
	suite.Run(t, new(CurrentStateSuite))
}

func (s *CurrentStateSuite) SetupSuite() {
	var err error
	s.dir, err = os.MkdirTemp("", filepath.Clean("current_state-"))
	s.NoError(err)
}

func (s *CurrentStateSuite) TearDownSuite() {
	_ = os.RemoveAll(filepath.Clean(s.dir)) // delete the created dir
}

func (s *CurrentStateSuite) TestEmptyFile() {
	cs := delivery.NewCurrentState(s.dir)

	s.T().Log("read empty file and check empty values")
	err := cs.Read()
	s.Error(err)

	snp := cs.ShardsNumberPower()
	s.EqualValues(0, snp)

	limits := cs.Limits()
	s.Equal(delivery.DefaultLimits(), limits)

	blockLimits := cs.Block()
	s.Equal(delivery.DefaultBlockLimits(), blockLimits)
}

func (s *CurrentStateSuite) TestWrite() {
	cs := delivery.NewCurrentState(s.dir)

	s.T().Log("set new number power, write file and read with check values")
	var expectedMaxSamples uint32 = 20e3
	limits := delivery.DefaultLimits()
	limits.OpenHead.MaxSamples = expectedMaxSamples

	err := cs.Write(2, &limits)
	s.Require().NoError(err)

	err = cs.Read()
	s.Require().NoError(err)

	snp := cs.ShardsNumberPower()
	s.EqualValues(2, snp)

	blimits := cs.Block()
	s.EqualValues(delivery.DefaultDesiredBlockSizeBytes, blimits.DesiredBlockSizeBytes)

	limits = cs.Limits()
	s.Equal(expectedMaxSamples, limits.OpenHead.MaxSamples)
}

func (s *CurrentStateSuite) TestValdate() {
	cs := delivery.NewCurrentState(s.dir)

	s.T().Log("set new invalid MaxSamples and DesiredBlockSizeBytes")
	limits := delivery.DefaultLimits()
	limits.OpenHead.MaxSamples = 0
	limits.Block.DesiredBlockSizeBytes = 0

	err := cs.Write(2, &limits)
	s.Require().NoError(err)

	err = cs.Read()
	s.Require().NoError(err)

	snp := cs.ShardsNumberPower()
	s.EqualValues(2, snp)

	blimits := cs.Block()
	s.EqualValues(delivery.DefaultDesiredBlockSizeBytes, blimits.DesiredBlockSizeBytes)

	limits = cs.Limits()
	s.EqualValues(delivery.DefaultMaxSamples, limits.OpenHead.MaxSamples)
}

func (s *CurrentStateSuite) TestError() {
	cs := delivery.NewCurrentState(s.dir)

	s.T().Log("set new number power, write file and read with check values")
	var expectedMaxSamples uint32 = 20e3
	limits := delivery.DefaultLimits()
	limits.OpenHead.MaxSamples = expectedMaxSamples

	err := cs.Write(2, &limits)
	s.Require().NoError(err)

	s.T().Log(cs.Limits().Hashdex)

	fi, err := os.Stat(filepath.Join(s.dir, "state.db"))
	s.Require().NoError(err)

	s.T().Log("check sum on file")
	err = os.Truncate(filepath.Join(s.dir, "state.db"), fi.Size()-1)
	s.Require().NoError(err)
	err = cs.Read()
	s.Require().ErrorIs(err, delivery.ErrCorruptedFile)

	s.T().Log("check sum on hashdex limits")
	err = os.Truncate(filepath.Join(s.dir, "state.db"), fi.Size()-7)
	s.Require().NoError(err)
	err = cs.Read()
	s.Require().ErrorIs(err, delivery.ErrCorruptedFile)

	s.T().Log("check sum on block limits")
	err = os.Truncate(filepath.Join(s.dir, "state.db"), fi.Size()-25)
	s.Require().NoError(err)
	err = cs.Read()
	s.Require().ErrorIs(err, delivery.ErrCorruptedFile)

	s.T().Log("check sum on open head limits")
	err = os.Truncate(filepath.Join(s.dir, "state.db"), fi.Size()-48)
	s.Require().NoError(err)
	err = cs.Read()
	s.Require().ErrorIs(err, delivery.ErrCorruptedFile)

	s.T().Log("check sum on shards number power")
	err = os.Truncate(filepath.Join(s.dir, "state.db"), fi.Size()-66)
	s.Require().NoError(err)
	err = cs.Read()
	s.Require().ErrorIs(err, delivery.ErrCorruptedFile)

	s.T().Log("check magic byte")
	err = os.Truncate(filepath.Join(s.dir, "state.db"), 0)
	s.Require().NoError(err)
	err = cs.Read()
	s.Require().ErrorIs(err, delivery.ErrCorruptedFile)
}

type AutosharderSuite struct {
	suite.Suite
}

func TestAutosharder(t *testing.T) {
	suite.Run(t, new(AutosharderSuite))
}

func (s *AutosharderSuite) TestAutosharderOnLimitTime() {
	clock := clockwork.NewFakeClock()
	dur := 2 * time.Hour
	cfg := delivery.BlockLimits{
		DesiredBlockSizeBytes:                   64 << 20,
		DesiredBlockFormationDuration:           dur,
		DelayAfterNotify:                        300 * time.Second,
		BlockSizePercentThresholdForDownscaling: 10,
	}
	as := delivery.NewAutosharder(clock, cfg, 5)

	s.T().Log("on reject")
	clock.Advance(30 * time.Minute)
	s.EqualValues(5, as.ShardsNumberPower(29<<20))

	s.T().Log("on reject")
	clock.Advance(30 * time.Minute)
	s.EqualValues(5, as.ShardsNumberPower(29<<20))

	s.T().Log("on reject")
	clock.Advance(30 * time.Minute)
	s.EqualValues(5, as.ShardsNumberPower(29<<20))

	s.T().Log("on reject")
	clock.Advance(15 * time.Minute)
	s.EqualValues(5, as.ShardsNumberPower(29<<20))

	clock.Advance(15 * time.Minute)
	s.T().Log("time is over, size block percent 0-3.125")
	s.EqualValues(0, as.ShardsNumberPower(1<<20))

	s.T().Log("time is over, size block percent 3.125-6.25")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(120 * time.Minute)
	s.EqualValues(1, as.ShardsNumberPower(2<<20))

	s.T().Log("time is over, size block percent 6.25-12.5")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(120 * time.Minute)
	s.EqualValues(2, as.ShardsNumberPower(5<<20))

	s.T().Log("time is over, size block percent 12.5-25")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(120 * time.Minute)
	s.EqualValues(3, as.ShardsNumberPower(10<<20))

	s.T().Log("time is over, size block percent 25-50")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(120 * time.Minute)
	s.EqualValues(4, as.ShardsNumberPower(29<<20))

	s.T().Log("time is over, size block percent 50-100")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(120 * time.Minute)
	s.EqualValues(5, as.ShardsNumberPower(30<<20))

	s.T().Log("reset, size block percent 50-100")
	as.Reset(cfg)
	clock.Advance(120 * time.Minute)
	s.EqualValues(1, as.ShardsNumberPower(2<<20))
}

func (s *AutosharderSuite) TestAutosharderOnLimitSize() {
	clock := clockwork.NewFakeClock()
	dur := 2 * time.Hour
	cfg := delivery.BlockLimits{
		DesiredBlockSizeBytes:                   64 << 20,
		DesiredBlockFormationDuration:           dur,
		DelayAfterNotify:                        300 * time.Second,
		BlockSizePercentThresholdForDownscaling: 10,
	}
	as := delivery.NewAutosharder(clock, cfg, 5)

	s.T().Log("on reject")
	clock.Advance(5 * time.Minute)
	s.EqualValues(5, as.ShardsNumberPower(32<<20))

	s.T().Log("on size limit, time block percent 0-6.25")
	s.EqualValues(10, as.ShardsNumberPower(65<<20))

	s.T().Log("on size limit, time block percent 6.25-12.5")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(15 * time.Minute)
	s.EqualValues(9, as.ShardsNumberPower(65<<20))

	s.T().Log("on size limit, time block percent 12.5-25")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(25 * time.Minute)
	s.EqualValues(8, as.ShardsNumberPower(65<<20))

	s.T().Log("on size limit, time block percent 25-50")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(35 * time.Minute)
	s.EqualValues(7, as.ShardsNumberPower(65<<20))

	s.T().Log("on size limit, time block percent 50-100")
	as = delivery.NewAutosharder(clock, cfg, 5)
	clock.Advance(65 * time.Minute)
	s.EqualValues(6, as.ShardsNumberPower(65<<20))

	s.T().Log("reset, size block percent 50-100")
	as.Reset(cfg)
	s.EqualValues(6, as.ShardsNumberPower(30<<20))

	s.T().Log("overhead, size block percent 50-100")
	as = delivery.NewAutosharder(clock, cfg, 253)
	clock.Advance(31 * time.Minute)
	s.EqualValues(255, as.ShardsNumberPower(65<<20))
}

type ManagerKeeperConfigSuite struct {
	suite.Suite
}

func TestManagerKeeperConfig(t *testing.T) {
	suite.Run(t, new(ManagerKeeperConfigSuite))
}

func (s *ManagerKeeperConfigSuite) TestDefaultManagerKeeperConfig() {
	cfg := delivery.DefaultManagerKeeperConfig()
	err := cfg.Validate()
	s.NoError(err)
}

func (s *ManagerKeeperConfigSuite) TestShutdownTimeoutNil() {
	cfg := delivery.DefaultManagerKeeperConfig()
	cfg.ShutdownTimeout = 0

	err := cfg.Validate()
	s.Error(err)
}

func (s *ManagerKeeperConfigSuite) TestShutdownTimeoutOverRefillInterval() {
	cfg := delivery.DefaultManagerKeeperConfig()
	cfg.ShutdownTimeout = 10

	err := cfg.Validate()
	s.Error(err)
}

func (s *ManagerKeeperConfigSuite) TestRefillIntervalNil() {
	cfg := delivery.DefaultManagerKeeperConfig()
	cfg.RefillInterval = 0

	err := cfg.Validate()
	s.Error(err)
}

func (s *ManagerKeeperConfigSuite) TestRefillSenderManager_ScanInterval_Nil() {
	cfg := delivery.DefaultManagerKeeperConfig()
	cfg.RefillSenderManager.ScanInterval = 0

	err := cfg.Validate()
	s.Error(err)
}

func (s *ManagerKeeperConfigSuite) TestRefillSenderManager_MaxRefillSize_Nil() {
	cfg := delivery.DefaultManagerKeeperConfig()
	cfg.RefillSenderManager.MaxRefillSize = 0

	err := cfg.Validate()
	s.Error(err)
}

type BlockLimitsSuite struct {
	suite.Suite
}

func TestBlockLimits(t *testing.T) {
	suite.Run(t, new(BlockLimitsSuite))
}

func (s *BlockLimitsSuite) TestMarshalBinaryUnmarshalBinary() {
	blm := delivery.DefaultBlockLimits()

	b, err := blm.MarshalBinary()
	s.NoError(err)

	blu := delivery.BlockLimits{}
	err = blu.UnmarshalBinary(b)
	s.NoError(err)

	s.Equal(blm, blu)
}

func (s *BlockLimitsSuite) TestMarshalBinaryUnmarshalBinary_Quick() {
	f := func(
		desiredBlockSizeBytes, blockSizePercentThresholdForDownscaling int64,
		desiredBlockFormationDuration, delayAfterNotify time.Duration,
	) bool {
		blm := delivery.BlockLimits{
			DesiredBlockSizeBytes:                   desiredBlockSizeBytes,
			BlockSizePercentThresholdForDownscaling: blockSizePercentThresholdForDownscaling,
			DesiredBlockFormationDuration:           desiredBlockFormationDuration,
			DelayAfterNotify:                        delayAfterNotify,
		}

		b, err := blm.MarshalBinary()
		s.NoError(err)

		blu := delivery.BlockLimits{}
		err = blu.UnmarshalBinary(b)
		s.NoError(err)

		return s.Equal(blm, blu)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}
