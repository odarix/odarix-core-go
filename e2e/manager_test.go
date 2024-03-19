package e2e_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"

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
	s.deliveryManagerHappyPath(protobufOpenHeadSender{})
	s.deliveryManagerHappyPath(goModelOpenHeadSender{})
}

func (s *BlockManagerSuite) deliveryManagerHappyPath(sender OpenHeadSenderGenerator) {
	retCh := make(chan *frames.ReadSegmentV4, 30)
	baseCtx := context.Background()
	tlscfg, err := s.makeTLSConfig()
	s.Require().NoError(err)

	handleStream := func(ctx context.Context, reader *server.WebSocketReader) {
		for {
			fr := frames.NewReadSegmentV4Empty()
			if errh := reader.Next(ctx, fr); errh != nil {
				if !errors.Is(errh, io.EOF) && !errors.Is(errh, context.Canceled) {
					s.NoError(errh, "fail to read next message")
				}
				return
			}

			// final frame
			if fr.Size == 0 {
				// something doing
				retCh <- fr
				return
			}

			// process data
			retCh <- fr
			if !s.NoError(reader.SendResponse(
				ctx,
				frames.NewResponseV4(fr.SentAt, fr.ID, 200, "OK"),
			), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(_ context.Context, rw http.ResponseWriter, _ io.Reader) {
		s.T().Log("not required")
		s.NoError(s.response(rw, "OK", http.StatusOK), "fail to send response")
	}

	addr := "127.0.0.1:6000"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManagerWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		generatedData, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
		_ = generatedData
		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	fr := <-retCh
	s.True(fr.Size == 0)

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(baseCtx))
}

func (s *BlockManagerSuite) TestDeliveryManagerBreakingConnection() {
	s.deliveryManagerBreakingConnection(protobufOpenHeadSender{})
	s.deliveryManagerBreakingConnection(goModelOpenHeadSender{})
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *BlockManagerSuite) deliveryManagerBreakingConnection(sender OpenHeadSenderGenerator) {
	var (
		retCh   = make(chan *frames.ReadSegmentV4, 30)
		breaker int32
	)
	const (
		breakAfterAuth  = 0
		breakBeforeRead = 12
		breakAfterRead  = 25
	)
	baseCtx := context.Background()
	tlscfg, err := s.makeTLSConfig()
	s.Require().NoError(err)

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

	handleStream := func(ctx context.Context, reader *server.WebSocketReader) {
		for {
			if onAccept != nil && !onAccept() {
				s.T().Log("handleStream: disconnect before read")
				return
			}
			fr := frames.NewReadSegmentV4Empty()
			if errh := reader.Next(ctx, fr); errh != nil {
				if !errors.Is(errh, io.EOF) && !errors.Is(errh, context.Canceled) {
					s.NoError(errh, "fail to read next message")
				}
				return
			}

			// final frame
			if fr.Size == 0 {
				// something doing
				retCh <- fr
				return
			}

			if onAccept != nil && !onAccept() {
				s.T().Log("handleStream: disconnect after read")
				return
			}

			// process data
			retCh <- fr
			if !s.NoError(reader.SendResponse(
				ctx,
				frames.NewResponseV4(fr.SentAt, fr.ID, 200, "OK"),
			), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(_ context.Context, rw http.ResponseWriter, _ io.Reader) {
		s.T().Log("not required")
		s.NoError(s.response(rw, "OK", http.StatusOK), "fail to send response")
	}

	addr := "127.0.0.1:6001"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManagerWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data before break")
	for i := 0; i < 5; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)

		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: break connection before read")
	for i := 5; i < 10; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)

		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: break connection after read")
	for i := 10; i < 15; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)

		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	fr := <-retCh
	s.True(fr.Size == 0)

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(baseCtx))
}

func (s *BlockManagerSuite) TestDeliveryManagerReject() {
	s.deliveryManagerReject(protobufOpenHeadSender{})
	s.deliveryManagerReject(goModelOpenHeadSender{})
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *BlockManagerSuite) deliveryManagerReject(sender OpenHeadSenderGenerator) {
	var (
		rejectSegment uint32 = 5
	)
	retCh := make(chan *frames.ReadSegmentV4, 30)
	baseCtx := context.Background()
	tlscfg, err := s.makeTLSConfig()
	s.Require().NoError(err)

	handleStream := func(ctx context.Context, reader *server.WebSocketReader) {
		for {
			fr := frames.NewReadSegmentV4Empty()
			if errh := reader.Next(ctx, fr); errh != nil {
				if !errors.Is(errh, io.EOF) && !errors.Is(errh, context.Canceled) {
					s.NoError(errh, "fail to read next message")
				}
				return
			}

			// final frame
			if fr.Size == 0 {
				// something doing
				retCh <- fr
				return
			}

			// process data
			retCh <- fr
			if fr.ID == rejectSegment {
				if !s.NoError(reader.SendResponse(
					ctx,
					frames.NewResponseV4(fr.SentAt, fr.ID, 400, "reject"),
				), "fail to send response") {
					return
				}
				return
			}

			if !s.NoError(reader.SendResponse(
				ctx,
				frames.NewResponseV4(fr.SentAt, fr.ID, 200, "OK"),
			), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(_ context.Context, rw http.ResponseWriter, _ io.Reader) {
		s.T().Log("not required")
		s.NoError(s.response(rw, "OK", http.StatusOK), "fail to send response")
	}

	addr := "127.0.0.1:6002"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManagerWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler)
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

		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: check exist file current.refill")
	path := filepath.Join(dir, delivery.RefillDir, "current.refill")
	_, err = os.Stat(path)
	s.Require().NoError(err)

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(baseCtx))
}
