package delivery_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
)

type RefillSenderSuite struct {
	suite.Suite

	workDir           string
	refillDir         string
	rcfg              delivery.RefillSendManagerConfig
	destinationsNames []string
	baseCtx           context.Context
}

func TestRefillSenderSuite(t *testing.T) {
	suite.Run(t, new(RefillSenderSuite))
}

func (s *RefillSenderSuite) SetupSuite() {
	dir, err := os.MkdirTemp("", filepath.Clean("refill-"))
	s.Require().NoError(err)

	s.workDir = dir
	s.refillDir = filepath.Join(s.workDir, delivery.RefillDir)

	s.rcfg = delivery.RefillSendManagerConfig{
		ScanInterval: 1 * time.Second,
	}

	s.destinationsNames = []string{
		"www.collector.com",
		"www.collector-dev.com",
		"www.collector-prod.com",
		"www.collector-replica.com",
	}

	s.baseCtx = context.Background()
}

func (s *RefillSenderSuite) SetupTest() {
	s.rcfg.MaxRefillSize = 10000
}

func (s *RefillSenderSuite) errorHandler(msg string, err error) {
	s.T().Logf("%s: %s", msg, err)
}

//revive:disable-next-line:cognitive-complexity this is test
func (s *RefillSenderSuite) createDialerHappyPath(name string, recv chan struct{}) delivery.Dialer {
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context, shardMeta delivery.ShardMeta) (delivery.Transport, error) {
			transport := &TransportMock{
				OnAckFunc:       func(fn func(uint32)) {},
				OnRejectFunc:    func(fn func(uint32)) {},
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame frames.FrameWriter) error {
					return nil
				},
				ListenFunc: func(ctx context.Context) {},
				CloseFunc: func() error {
					return nil
				},
			}

			return transport, nil
		},
		SendRefillFunc: func(ctx context.Context, r io.Reader, shardMeta delivery.ShardMeta) error {
			buf := new(bytes.Buffer)
			_, err := buf.ReadFrom(r)
			s.Require().NoError(err)
			s.NotEqual(0, buf.Len())
			recv <- struct{}{}
			return nil
		},
	}
}

func (s *RefillSenderSuite) makeRefill(destinationsNames []string) {
	blockID, err := uuid.NewRandom()
	s.Require().NoError(err)

	mr, err := delivery.NewRefill(
		s.workDir,
		1,
		1,
		blockID,
		false,
		nil,
		destinationsNames...,
	)
	s.Require().NoError(err)

	segKey := cppbridge.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	for i := 0; i < 4; i++ {
		err = mr.WriteSegment(
			s.baseCtx,
			segKey,
			&dataTest{
				data: []byte(fmt.Sprintf("%d:%d:segment", segKey.ShardID, segKey.Segment)),
			},
		)
		s.Require().NoError(err)
		segKey.Segment++
	}

	err = mr.WriteAckStatus(s.baseCtx)
	s.Require().NoError(err)

	_, err = os.Stat(filepath.Join(s.refillDir, delivery.RefillFileName+refillExt))
	s.Require().NoError(err, "file not exist")

	// ack 0,1 segment, 2 - reject for all destinations
	for _, name := range destinationsNames {
		mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 0}, name)
		mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 1}, name)
		mr.Reject(cppbridge.SegmentKey{ShardID: 0, Segment: 2}, name)
	}

	// ack 3 segment for all except 1 destination
	for _, name := range destinationsNames[1:] {
		mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 3}, name)
	}

	// 4 - reject for all destinations
	for _, name := range destinationsNames[1:] {
		mr.Reject(cppbridge.SegmentKey{ShardID: 0, Segment: 4}, name)
	}

	// ack 5 segment for all except 1,2 destination
	for _, name := range destinationsNames[2:] {
		mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 5}, name)
	}

	err = mr.WriteAckStatus(s.baseCtx)
	s.Require().NoError(err)

	_, err = os.Stat(filepath.Join(s.refillDir, delivery.RefillFileName+refillExt))
	s.Require().NoError(err, "file not exist")

	s.Require().NoError(mr.IntermediateRename())

	s.Require().NoError(mr.Shutdown(s.baseCtx))
}

func (s *RefillSenderSuite) TestHappyPath() {
	s.T().Log("make refill file")
	s.makeRefill(s.destinationsNames)
	files, err := os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.Equal(1, len(files))

	recv := make(chan struct{}, len(s.destinationsNames)*2)

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerHappyPath(dname, recv)
	}

	s.T().Log("init and run refill manager")
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		s.workDir,
		dialers,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		i := len(s.destinationsNames)
		for {
			select {
			case <-time.NewTimer(2 * time.Second).C:
				cancel(delivery.ErrShutdown)
				wg.Done()
				return
			case <-recv:
				i--
				if i == 0 {
					cancel(delivery.ErrShutdown)
					wg.Done()
					return
				}
			}
		}
	}()

	go func() {
		s.T().Log("Run")
		rsmanager.Run(ctx)
		wg.Done()
	}()

	clockCtx, clockCancel := context.WithTimeout(s.baseCtx, 50*time.Millisecond)
	clock.BlockUntilContext(clockCtx, 1)
	clockCancel()
	s.T().Log("time shift")
	clock.Advance(2 * time.Second)

	wg.Wait()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that all files have been sent")
	files, err = os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.workDir))
	s.Require().NoError(err)

	s.Equal(0, len(recv))
}

func (s *RefillSenderSuite) TestHappyPathWithChangeDestinations() {
	s.T().Log("make refill file")
	s.makeRefill(append(s.destinationsNames[2:], "some_name"))
	files, err := os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.Equal(1, len(files))

	recv := make(chan struct{}, len(s.destinationsNames)*2)

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerHappyPath(dname, recv)
	}

	s.T().Log("init and run refill manager")
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		s.workDir,
		dialers,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		i := len(s.destinationsNames[2:])
		for {
			select {
			case <-time.NewTimer(2 * time.Second).C:
				cancel(delivery.ErrShutdown)
				wg.Done()
				return
			case <-recv:
				i--
				if i == 0 {
					cancel(delivery.ErrShutdown)
					wg.Done()
					return
				}
			}
		}
	}()

	go func() {
		s.T().Log("Run")
		rsmanager.Run(ctx)
		wg.Done()
	}()

	clockCtx, clockCancel := context.WithTimeout(s.baseCtx, 50*time.Millisecond)
	clock.BlockUntilContext(clockCtx, 1)
	clockCancel()
	s.T().Log("time shift")
	clock.Advance(2 * time.Second)

	wg.Wait()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that all files have been sent")
	files, err = os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.workDir))
	s.Require().NoError(err)

	s.Equal(0, len(recv))
}

//revive:disable-next-line:cognitive-complexity this is test
func (s *RefillSenderSuite) createDialerReject(name string, recv chan struct{}) delivery.Dialer {
	switcher := false
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context, shardMeta delivery.ShardMeta) (delivery.Transport, error) {
			transport := &TransportMock{
				OnAckFunc:       func(fn func(uint32)) {},
				OnRejectFunc:    func(fn func(uint32)) {},
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame frames.FrameWriter) error {
					return nil
				},
				ListenFunc: func(ctx context.Context) {},
				CloseFunc: func() error {
					return nil
				},
			}

			return transport, nil
		},
		SendRefillFunc: func(ctx context.Context, r io.Reader, shardMeta delivery.ShardMeta) error {
			if !switcher {
				switcher = true
				return errors.New("some errors")
			}
			buf := new(bytes.Buffer)
			_, err := buf.ReadFrom(r)
			s.Require().NoError(err)
			s.NotEqual(0, buf.Len())
			recv <- struct{}{}
			return nil
		},
	}
}

func (s *RefillSenderSuite) TestRejectAndAck() {
	s.T().Log("make refill file")
	s.makeRefill(s.destinationsNames)
	files, err := os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.LessOrEqual(1, len(files))

	recv := make(chan struct{}, len(s.destinationsNames)*2)

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerReject(dname, recv)
	}

	s.T().Log("init and run refill manager")
	s.rcfg.ScanInterval = time.Second
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		s.workDir,
		dialers,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		i := len(s.destinationsNames)
		for {
			select {
			case <-time.NewTimer(2 * time.Second).C:
				cancel(delivery.ErrShutdown)
				wg.Done()
				return
			case <-recv:
				i--
				if i == 0 {
					cancel(delivery.ErrShutdown)
					wg.Done()
					return
				}
			}
		}
	}()

	go func() {
		s.T().Log("Run")
		rsmanager.Run(ctx)
		wg.Done()
	}()

	clockCtx, clockCancel := context.WithTimeout(s.baseCtx, 50*time.Millisecond)
	clock.BlockUntilContext(clockCtx, 1)
	clockCancel()
	s.T().Log("time shift")
	clock.Advance(2 * time.Second)

	wg.Wait()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that all files have been sent")
	files, err = os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.workDir))
	s.Require().NoError(err)

	s.Equal(0, len(recv))
}

//revive:disable-next-line:cognitive-complexity this is test
func (*RefillSenderSuite) createDialerOnlyReject(name string, reject chan struct{}) delivery.Dialer {
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context, shardMeta delivery.ShardMeta) (delivery.Transport, error) {
			transport := &TransportMock{
				OnAckFunc:       func(fn func(uint32)) {},
				OnRejectFunc:    func(fn func(uint32)) {},
				OnReadErrorFunc: func(fn func(error)) {},
				SendFunc: func(ctx context.Context, frame frames.FrameWriter) error {
					return nil
				},
				ListenFunc: func(ctx context.Context) {},
				CloseFunc: func() error {
					return nil
				},
			}

			return transport, nil
		},
		SendRefillFunc: func(ctx context.Context, r io.Reader, shardMeta delivery.ShardMeta) error {
			reject <- struct{}{}
			return errors.New("some errors")
		},
	}
}

func (s *RefillSenderSuite) TestClearing() {
	s.T().Log("make refill file")
	s.makeRefill(s.destinationsNames)
	files, err := os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.LessOrEqual(1, len(files))

	reject := make(chan struct{}, len(s.destinationsNames)*2)

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerOnlyReject(dname, reject)
	}

	s.T().Log("init and run refill manager")
	s.rcfg.MaxRefillSize = 1
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		s.workDir,
		dialers,
		s.errorHandler,
		clock,
		nil,
	)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		i := len(s.destinationsNames)
		for {
			select {
			case <-time.NewTimer(2 * time.Second).C:
				cancel(delivery.ErrShutdown)
				wg.Done()
				return
			case <-reject:
				i--
				if i == 0 {
					cancel(delivery.ErrShutdown)
					wg.Done()
					return
				}
			}
		}
	}()

	go func() {
		s.T().Log("Run")
		rsmanager.Run(ctx)
		wg.Done()
	}()

	clockCtx, clockCancel := context.WithTimeout(s.baseCtx, 100*time.Millisecond)
	clock.BlockUntilContext(clockCtx, 1)
	clockCancel()
	s.T().Log("time shift")
	clock.Advance(1 * time.Second)

	wg.Wait()

	s.T().Log("refill manager shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = rsmanager.Shutdown(shutdownCtx)
	shutdownCancel()
	s.Require().NoError(err)

	s.T().Log("check that old files have been deleted")
	files, err = os.ReadDir(s.refillDir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.workDir))
	s.Require().NoError(err)

	s.Equal(0, len(reject))
}

func TestSendMap(t *testing.T) {
	sm := delivery.NewSendMap(1)
	referenceData := []uint32{4, 5, 6, 7, 8, 9, 10, 11, 12}
	for _, data := range referenceData {
		sm.Append("testName", 0, data)
	}

	t.Log("happy path")
	sm.Range(func(_ string, _ int, shardData []uint32) bool {
		require.Equal(t, referenceData, shardData)
		return true
	})

	t.Log("less segment")
	sm.Append("testName", 0, 6)
	for _, data := range referenceData[2:] {
		sm.Append("testName", 0, data)
	}
	sm.Range(func(_ string, _ int, shardData []uint32) bool {
		require.Equal(t, referenceData, shardData)
		return true
	})

	t.Log("less range segment")
	sm.Append("testName", 0, 3)
	for _, data := range referenceData {
		sm.Append("testName", 0, data)
	}
	sm.Range(func(_ string, _ int, shardData []uint32) bool {
		require.Equal(t, append([]uint32{3}, referenceData...), shardData)
		return true
	})

	sm.Remove("testName", 0)
	sm.Range(func(_ string, _ int, shardData []uint32) bool {
		require.Equal(t, []uint32{}, shardData)
		return true
	})
}
