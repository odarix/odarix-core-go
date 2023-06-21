package delivery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/delivery"
)

func TestRefillShardEOF(t *testing.T) {
	wrs := delivery.RefillShardEOF{
		NameID:  3,
		ShardID: 1,
	}

	buf, err := wrs.MarshalBinary()
	require.NoError(t, err)

	rw := NewFileBuffer()
	_, err = rw.Write(buf)
	require.NoError(t, err)

	rbuf := make([]byte, len(buf))
	_, err = rw.ReadAt(rbuf, 0)
	require.NoError(t, err)

	rrs := delivery.NewRefillShardEOFEmpty()
	err = rrs.UnmarshalBinary(rbuf)
	require.NoError(t, err)

	assert.Equal(t, rrs.NameID, wrs.NameID)
	assert.Equal(t, rrs.ShardID, wrs.ShardID)
}

type RefillSenderSuite struct {
	suite.Suite

	rcfg              *delivery.RefillSendManagerConfig
	destinationsNames []string
	baseCtx           context.Context
}

func TestRefillSenderSuite(t *testing.T) {
	suite.Run(t, new(RefillSenderSuite))
}

func (s *RefillSenderSuite) SetupSuite() {
	dir, err := os.MkdirTemp("", filepath.Clean("refill-"))
	s.Require().NoError(err)

	s.rcfg = &delivery.RefillSendManagerConfig{
		Dir:          dir,
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

func (*RefillSenderSuite) createDialerHappyPath(name string) delivery.Dialer {
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			var ack func(uint32)
			var numberOfMessage uint32
			transport := &TransportMock{
				OnAckFunc: func(fn func(uint32)) {
					ack = fn
				},
				OnRejectFunc:    func(fn func(uint32)) {},
				OnReadErrorFunc: func(fn func(error)) {},
				SendRefillFunc: func(_ context.Context, preparedDatas []delivery.PreparedData) error {
					numberOfMessage = uint32(len(preparedDatas))
					return nil
				},
				SendSegmentFunc: func(_ context.Context, _ delivery.Segment) error {
					numberOfMessage--
					if numberOfMessage == 0 {
						ack(0)
					}
					return nil
				},
				SendDrySegmentFunc: func(_ context.Context, _ delivery.Segment) error {
					numberOfMessage--
					if numberOfMessage == 0 {
						ack(0)
					}
					return nil
				},
				SendSnapshotFunc: func(_ context.Context, _ delivery.Snapshot) error {
					numberOfMessage--
					if numberOfMessage == 0 {
						ack(0)
					}
					return nil
				},
				CloseFunc: func() error {
					ack = func(u uint32) {}
					return nil
				},
			}

			return transport, nil
		},
	}
}

func (s *RefillSenderSuite) makeRefill(destinationsNames []string) {
	blockID, err := uuid.NewRandom()
	s.Require().NoError(err)

	fcfg := &delivery.FileStorageConfig{
		Dir:      s.rcfg.Dir,
		FileName: "current",
	}

	mr, err := delivery.NewRefill(
		fcfg,
		1,
		blockID,
		destinationsNames...,
	)
	s.Require().NoError(err)

	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	err = mr.WriteSnapshot(
		s.baseCtx,
		segKey,
		&dataTest{
			data: []byte(fmt.Sprintf("%d:%d:snapshot", segKey.ShardID, segKey.Segment)),
		},
	)
	s.Require().NoError(err)

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

	_, err = os.Stat(filepath.Join(fcfg.Dir, fcfg.FileName+".refill"))
	s.Require().NoError(err, "file not exist")

	// ack 0,1 segment, 2 - reject for all destinations
	for _, name := range destinationsNames {
		mr.Ack(delivery.SegmentKey{0, 0}, name)
		mr.Ack(delivery.SegmentKey{0, 1}, name)
		mr.Reject(delivery.SegmentKey{0, 2}, name)
	}

	// ack 3 segment for all except 1 destination
	for _, name := range destinationsNames[1:] {
		mr.Ack(delivery.SegmentKey{0, 3}, name)
	}

	// 4 - reject for all destinations
	for _, name := range destinationsNames[1:] {
		mr.Reject(delivery.SegmentKey{0, 4}, name)
	}

	// ack 5 segment for all except 1,2 destination
	for _, name := range destinationsNames[2:] {
		mr.Ack(delivery.SegmentKey{0, 5}, name)
	}

	err = mr.WriteAckStatus(s.baseCtx)
	s.Require().NoError(err)

	_, err = os.Stat(filepath.Join(fcfg.Dir, fcfg.FileName+".refill"))
	s.Require().NoError(err, "file not exist")

	s.Require().NoError(mr.IntermediateRename())

	s.Require().NoError(mr.Shutdown(s.baseCtx))
}

func (s *RefillSenderSuite) TestHappyPath() {
	s.T().Log("make refill file")
	s.makeRefill(s.destinationsNames)
	files, err := os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.Equal(1, len(files))

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerHappyPath(dname)
	}

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	time.AfterFunc(
		1*time.Second,
		func() {
			cancel(delivery.ErrShutdown)
		},
	)

	s.T().Log("init and run refill manager")
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		dialers,
		s.errorHandler,
		clock,
	)
	s.Require().NoError(err)

	time.AfterFunc(
		100*time.Millisecond,
		func() {
			s.T().Log("time shift")
			clock.Advance(1 * time.Second)
		},
	)
	rsmanager.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that all files have been sent")
	files, err = os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.rcfg.Dir))
	s.Require().NoError(err)
}

func (s *RefillSenderSuite) TestHappyPathWithChangeDestinations() {
	s.T().Log("make refill file")
	s.makeRefill(append(s.destinationsNames[2:], "some_name"))
	files, err := os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.Equal(1, len(files))

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerHappyPath(dname)
	}

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	time.AfterFunc(
		1*time.Second,
		func() {
			cancel(delivery.ErrShutdown)
		},
	)

	s.T().Log("init and run refill manager")
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		dialers,
		s.errorHandler,
		clock,
	)
	s.Require().NoError(err)

	time.AfterFunc(
		100*time.Millisecond,
		func() {
			s.T().Log("time shift")
			clock.Advance(1 * time.Second)
		},
	)
	rsmanager.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that all files have been sent")
	files, err = os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.Equal(0, len(files))
}

//revive:disable-next-line:cognitive-complexity this is test
func (*RefillSenderSuite) createDialerReject(name string) delivery.Dialer {
	switcher := false
	return &DialerMock{
		StringFunc: func() string { return name },
		DialFunc: func(ctx context.Context) (delivery.Transport, error) {
			var ack func(uint32)
			var reject func(uint32)
			var numberOfMessage uint32
			transport := &TransportMock{
				OnAckFunc: func(fn func(uint32)) {
					ack = fn
				},
				OnRejectFunc: func(fn func(uint32)) {
					reject = fn
				},
				OnReadErrorFunc: func(fn func(error)) {},
				SendRefillFunc: func(_ context.Context, preparedDatas []delivery.PreparedData) error {
					numberOfMessage = uint32(len(preparedDatas))
					return nil
				},
				SendSegmentFunc: func(_ context.Context, _ delivery.Segment) error {
					numberOfMessage--
					if numberOfMessage == 0 {
						if switcher {
							ack(0)
							return nil
						}
						switcher = true
						reject(0)
					}
					return nil
				},
				SendDrySegmentFunc: func(_ context.Context, _ delivery.Segment) error {
					numberOfMessage--
					if numberOfMessage == 0 {
						if switcher {
							ack(0)
							return nil
						}
						switcher = true
						reject(0)
					}
					return nil
				},
				SendSnapshotFunc: func(_ context.Context, _ delivery.Snapshot) error {
					numberOfMessage--
					if numberOfMessage == 0 {
						if switcher {
							ack(0)
							return nil
						}
						switcher = true
						reject(0)
					}
					return nil
				},
				CloseFunc: func() error {
					ack = func(u uint32) {}
					reject = func(u uint32) {}
					return nil
				},
			}

			return transport, nil
		},
	}
}

func (s *RefillSenderSuite) TestRejectAndAck() {
	s.T().Log("make refill file")
	s.makeRefill(s.destinationsNames)
	files, err := os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.LessOrEqual(1, len(files))

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerReject(dname)
	}

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	time.AfterFunc(
		2*time.Second,
		func() {
			cancel(delivery.ErrShutdown)
		},
	)

	s.T().Log("init and run refill manager")
	s.rcfg.ScanInterval = time.Second
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		dialers,
		s.errorHandler,
		clock,
	)
	s.Require().NoError(err)

	time.AfterFunc(
		100*time.Millisecond,
		func() {
			s.T().Log("time shift")
			clock.Advance(2 * time.Second)
		},
	)
	rsmanager.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that all files have been sent")
	files, err = os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.rcfg.Dir))
	s.Require().NoError(err)
}

func (s *RefillSenderSuite) TestClearing() {
	s.T().Log("make refill file")
	s.makeRefill(s.destinationsNames)
	files, err := os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.LessOrEqual(1, len(files))

	s.T().Log("init dialers")
	dialers := make([]delivery.Dialer, len(s.destinationsNames))
	for i, dname := range s.destinationsNames {
		dialers[i] = s.createDialerReject(dname)
	}

	ctx, cancel := context.WithCancelCause(s.baseCtx)
	time.AfterFunc(
		1*time.Second,
		func() {
			cancel(delivery.ErrShutdown)
		},
	)

	s.T().Log("init and run refill manager")
	s.rcfg.MaxRefillSize = 1
	clock := clockwork.NewFakeClock()
	rsmanager, err := delivery.NewRefillSendManager(
		s.rcfg,
		dialers,
		s.errorHandler,
		clock,
	)
	s.Require().NoError(err)

	time.AfterFunc(
		100*time.Millisecond,
		func() {
			s.T().Log("time shift")
			clock.Advance(1 * time.Second)
		},
	)
	rsmanager.Run(ctx)

	s.T().Log("refill manager shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("check that old files have been deleted")
	files, err = os.ReadDir(s.rcfg.Dir)
	s.Require().NoError(err)
	s.Equal(0, len(files))

	err = os.RemoveAll(filepath.Clean(s.rcfg.Dir))
	s.Require().NoError(err)
}
