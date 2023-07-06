package e2e_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/server"
	"github.com/odarix/odarix-core-go/transport"
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
	var wr *prompb.WriteRequest
	baseCtx := context.Background()

	handleStream := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, msg))
		defer reader.Destroy()
		for {
			segmentID, data, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			s.Equal(wr.String(), data.String())

			if !s.NoError(tcpReader.SendResponse(ctx, "OK", 200, segmentID), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, "OK", 200, 0), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:6000", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		wr = s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)

		delivered, errLoop := manager.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *BlockManagerSuite) TestDeliveryManagerBreakingConnection() {
	var (
		wr      *prompb.WriteRequest
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

	handleStream := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, msg))
		defer reader.Destroy()
		for {
			if onAccept != nil && !onAccept() {
				s.T().Log("handleStream: disconnect before read")
				return
			}

			segmentID, data, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			if onAccept != nil && !onAccept() {
				s.T().Log("handleStream: disconnect after read")
				return
			}

			// process data
			s.Equal(wr.String(), data.String())

			if !s.NoError(tcpReader.SendResponse(ctx, "OK", 200, segmentID), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, "OK", 200, 0), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:6001", s.token, onAccept, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data before break")
	for i := 0; i < 5; i++ {
		wr = s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)
		delivered, errLoop := manager.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
	}

	s.T().Log("client: break connection before read")
	for i := 5; i < 10; i++ {
		wr = s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)
		delivered, errLoop := manager.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
	}

	s.T().Log("client: break connection after read")
	for i := 10; i < 15; i++ {
		wr = s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)
		delivered, errLoop := manager.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		s.Require().True(delivered)
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *BlockManagerSuite) TestDeliveryManagerReject() {
	var (
		wr            *prompb.WriteRequest
		rejectSegment uint32 = 5
	)
	baseCtx := context.Background()

	handleStream := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, msg))
		defer reader.Destroy()
		for {
			segmentID, data, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			s.Equal(wr.String(), data.String())

			if segmentID == rejectSegment {
				if !s.NoError(tcpReader.SendResponse(ctx, "reject", 400, segmentID), "fail to send response") {
					return
				}
				return
			}

			if !s.NoError(tcpReader.SendResponse(ctx, "OK", 200, segmentID), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, "OK", 200, 0), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:6002", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		wr = s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)
		delivered, errLoop := manager.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		if i == int(rejectSegment) {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}
	}

	s.T().Log("client: check exist file current.refill")
	path := filepath.Join(dir, "current.refill")
	_, err = os.Stat(path)
	s.Require().NoError(err)

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}
