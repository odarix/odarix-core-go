package e2e_test

import (
	"context"
	"errors"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/suite"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/server"
)

type BlockManagerSuite struct {
	MainSuite

	token string
}

func TestBlockManager(t *testing.T) {
	suite.Run(t, new(BlockManagerSuite))
}

func (s *BlockManagerSuite) SetupSuite() {
	s.token = "auth_token" + s.T().Name()
}

func (*BlockManagerSuite) mkDir() (string, error) {
	return os.MkdirTemp("", filepath.Clean("refill-"))
}

func (*BlockManagerSuite) removeDir(dir string) error {
	return os.RemoveAll(filepath.Clean(dir))
}

func (s *BlockManagerSuite) errorHandler(msg string, err error) {
	s.T().Logf("errorHandler: %s: %s", msg, err)
}

func (s *BlockManagerSuite) TestDeliveryManagerHappyPath() {
	s.testDeliveryManagerHappyPath(protobufOpenHeadSender{})
	s.testDeliveryManagerHappyPath(goModelOpenHeadSender{})
}

func (s *BlockManagerSuite) testDeliveryManagerHappyPath(sender OpenHeadSenderGenerator) {
	retCh := make(chan *server.Request, 30)
	baseCtx := context.Background()

	handleStream := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, fe))
		for {
			rq, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			if rq.Finalized {
				// something doing
				retCh <- rq
				return
			}

			// process data
			retCh <- rq

			if !s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
				Text:      "OK",
				Code:      200,
				SegmentID: rq.SegmentID,
				SendAt:    rq.SentAt,
			}), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
			Text: "OK",
			Code: 200,
		}), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:6000", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	clock := clockwork.NewRealClock()
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler, clock)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		generatedData, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
		rq := <-retCh
		s.EqualAsJSON(generatedData.AsRemoteWriteProto(), rq.Message)
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	rq := <-retCh
	s.True(rq.Finalized)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *BlockManagerSuite) TestDeliveryManagerBreakingConnection() {
	s.testDeliveryManagerBreakingConnection(protobufOpenHeadSender{})
	s.testDeliveryManagerBreakingConnection(goModelOpenHeadSender{})
}

func (s *BlockManagerSuite) testDeliveryManagerBreakingConnection(sender OpenHeadSenderGenerator) {
	var (
		retCh   = make(chan *frames.ReadFrame, 30)
		breaker int32
	)
	const (
		breakAfterAuth  = 0
		breakBeforeRead = 12
		breakAfterRead  = 25
	)
	baseCtx := context.Background()

	onAccept := func() bool {
		switch atomic.LoadInt32(&breaker) {
		case breakAfterAuth:
			s.T().Log("onAccept: break connection after auth")
		case breakBeforeRead:
			s.T().Log("onAccept: break connection before read")
		case breakAfterRead:
			s.T().Log("onAccept: break connection after read")
		default:
			atomic.AddInt32(&breaker, 1)
			return true
		}

		atomic.AddInt32(&breaker, 1)
		return false
	}

	handleStream := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		reader := server.StartWith(tcpReader, fe)
		for {
			if onAccept != nil && !onAccept() {
				s.T().Log("handleStream: disconnect before read")
				return
			}

			fe, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			if fe.GetType() == frames.FinalType {
				// something doing
				retCh <- fe
				return
			}

			if onAccept != nil && !onAccept() {
				s.T().Log("handleStream: disconnect after read")
				return
			}

			// process data
			retCh <- fe

			if !s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
				Text:      "OK",
				Code:      200,
				SegmentID: fe.GetSegmentID(),
				SendAt:    fe.GetCreatedAt(),
			}), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
			Text: "OK",
			Code: 200,
		}), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:6001", s.token, onAccept, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler, clockwork.NewRealClock())
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data before break")
	for i := 0; i < 5; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
		fe := <-retCh
		s.EqualValues(i, fe.GetSegmentID())
	}

	s.T().Log("client: break connection before read")
	for i := 5; i < 10; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
		fe := <-retCh
		s.EqualValues(i, fe.GetSegmentID())
	}

	s.T().Log("client: break connection after read")
	for i := 10; i < 15; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
		fe := <-retCh
		s.EqualValues(i, fe.GetSegmentID())
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	fe := <-retCh
	s.True(fe.GetType() == frames.FinalType)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *BlockManagerSuite) TestDeliveryManagerReject() {
	s.testDeliveryManagerReject(protobufOpenHeadSender{})
	s.testDeliveryManagerReject(goModelOpenHeadSender{})
}

func (s *BlockManagerSuite) testDeliveryManagerReject(sender OpenHeadSenderGenerator) {
	var (
		rejectSegment uint32 = 5
	)
	retCh := make(chan *frames.ReadFrame, 30)
	baseCtx := context.Background()

	handleStream := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		reader := server.StartWith(tcpReader, fe)
		for {
			fe, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			if fe.GetType() == frames.FinalType {
				// something doing
				retCh <- fe
				return
			}

			// process data
			retCh <- fe

			if fe.GetSegmentID() == rejectSegment {
				if !s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
					Text:      "reject",
					Code:      400,
					SegmentID: fe.GetSegmentID(),
					SendAt:    fe.GetCreatedAt(),
				}), "fail to send response") {
					return
				}
				return
			}

			if !s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
				Text:      "OK",
				Code:      200,
				SegmentID: fe.GetSegmentID(),
				SendAt:    fe.GetCreatedAt(),
			}), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
			Text: "OK",
			Code: 200,
		}), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:6002", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler, clockwork.NewRealClock())
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		if i == int(rejectSegment) {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}
		fe := <-retCh
		s.EqualValues(i, fe.GetSegmentID())
	}

	s.T().Log("client: check exist file current.refill")
	path := filepath.Join(dir, delivery.RefillDir, "current.refill")
	_, err = os.Stat(path)
	s.Require().NoError(err)

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	fe := <-retCh
	s.True(fe.GetType() == frames.FinalType)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}
