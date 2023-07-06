package e2e_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/server"
	"github.com/odarix/odarix-core-go/transport"
)

type ManagerKeeperSuite struct {
	MainSuite

	token string
}

func TestManagerKeeper(t *testing.T) {
	suite.Run(t, new(ManagerKeeperSuite))
}

func (s *ManagerKeeperSuite) SetupSuite() {
	s.token = "auth_token" + s.T().Name()
}

func (*ManagerKeeperSuite) mkDir() (string, error) {
	return os.MkdirTemp("", filepath.Clean("refill-"))
}

func (*ManagerKeeperSuite) removeDir(dir string) error {
	return os.RemoveAll(filepath.Clean(dir))
}

func (s *ManagerKeeperSuite) errorHandler(msg string, err error) {
	s.T().Logf("errorHandler: %s: %s", msg, err)
}

func (s *ManagerKeeperSuite) TestRefillSenderHappyPath() {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *prompb.WriteRequest, count)

	handleStream := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, msg))
		defer reader.Destroy()
		for {
			segmentID, wrMsg, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			retCh <- wrMsg

			if !s.NoError(tcpReader.SendResponse(ctx, "OK", 200, segmentID), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, "OK", 200, 0), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:5000", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager keeper create")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.NoError(s.removeDir(dir))
	clock := clockwork.NewFakeClock()
	managerKeeper, err := s.createManagerKeeper(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler, clock)
	s.Require().NoError(err)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		wr := s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)

		delivered, errLoop := managerKeeper.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		s.Require().True(delivered)

		wrMsg := <-retCh
		s.Equal(wr.String(), wrMsg.String())
	}

	s.T().Log("client: shutdown manager")
	err = managerKeeper.Shutdown(baseCtx)
	s.Require().NoError(err)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

func (s *ManagerKeeperSuite) TestWithRotate() {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *prompb.WriteRequest, count*2)

	handleStream := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, msg))
		defer reader.Destroy()
		for {
			segmentID, wrMsg, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			retCh <- wrMsg

			if !s.NoError(tcpReader.SendResponse(ctx, "OK", 200, segmentID), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		s.T().Log("not required")
		s.NoError(tcpReader.SendResponse(ctx, "OK", 200, 0), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:5001", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager keeper create")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	clock := clockwork.NewFakeClock()
	managerKeeper, err := s.createManagerKeeper(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler, clock)
	s.Require().NoError(err)

	s.T().Log("client: run time shift")
	advanceCtx, advanceCancel := context.WithCancel(baseCtx)
	defer advanceCancel()
	go func(ctx context.Context) {
		ticker := time.NewTicker(400 * time.Millisecond)
		l := 0
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l++
				s.T().Log("client: rotate loop:", l)
				clock.Advance(4 * time.Second)
			}
		}
	}(advanceCtx)

	s.T().Log("client: send data")
	for i := 0; i < count; i++ {
		wr := s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)

		delivered, errLoop := managerKeeper.Send(baseCtx, h)
		s.Require().NoError(errLoop)
		s.Require().True(delivered)

		wrMsg := <-retCh
		s.Equal(wr.String(), wrMsg.String())
	}

	s.T().Log("client: shutdown manager")
	err = managerKeeper.Shutdown(baseCtx)
	s.Require().NoError(err)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *ManagerKeeperSuite) TestWithReject() {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *prompb.WriteRequest, count*2)
	rejectsCh := make(chan *prompb.WriteRequest, count*2)

	handleStream := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, msg))
		defer reader.Destroy()
		for {
			segmentID, wrMsg, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			retCh <- wrMsg

			if segmentID%2 == 0 {
				if !s.NoError(tcpReader.SendResponse(ctx, "reject", 400, segmentID), "fail to send response") {
					return
				}
				rejectsCh <- wrMsg
				continue
			}

			if !s.NoError(tcpReader.SendResponse(ctx, "OK", 200, segmentID), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader) {
		var refillMsg transport.RefillMsg
		if !s.NoError(refillMsg.UnmarshalBinary(msg.Payload), "unmarshal binary") {
			return
		}

		// make tmp file for work
		dir, err := os.MkdirTemp("", "refill-server-dir-")
		if !s.NoError(err, "fail mkdir") {
			return
		}
		defer s.removeDir(dir)
		file, err := os.CreateTemp(dir, "refill-server-file-")
		if !s.NoError(err, "fail open file") {
			return
		}

		// save Messages to file
		for i := 0; i < len(refillMsg.Messages); i++ {
			raw, errNext := tcpReader.Next(ctx)
			if !s.NoError(errNext, "fail next") {
				return
			}

			switch raw.Header.Type {
			case transport.MsgSnapshot, transport.MsgDryPut, transport.MsgPut:
				if !s.NoError(transport.WriteRawMessage(file, raw), "fail write") {
					return
				}
			default:
				s.T().Errorf("unexpected msg type %d", raw.Header.Type)
				return
			}
		}
		if !s.NoError(file.Sync(), "sync file") {
			return
		}
		if !s.NoError(file.Close(), "close file") {
			return
		}

		// make FileReader
		fr, err := server.NewFileReader(file.Name())
		if !s.NoError(err, "fail init FileReader") {
			return
		}
		defer fr.Close()

		// make ProtocolReader over FileReader
		pr := server.NewProtocolReader(fr)
		defer pr.Destroy()

		// make BlockWriter
		// read until EOF from ProtocolReader and append to BlockWriter
		// save BlockWriter
		// send block to S3
		for {
			_, wrMsg, err := pr.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				break
			}

			s.Require().NotEqual(0, len(rejectsCh))
			ewr := <-rejectsCh
			s.Equal(ewr.String(), wrMsg.String())
		}

		_ = tcpReader.SendResponse(ctx, "OK", 200, 0)
	}

	listener := s.runServer(baseCtx, "127.0.0.1:5002", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager keeper create")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	clock := clockwork.NewFakeClock()
	managerKeeper, err := s.createManagerKeeper(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler, clock)
	s.Require().NoError(err)

	s.T().Log("client: run time shift")
	advanceCtx, advanceCancel := context.WithCancel(baseCtx)
	defer advanceCancel()
	go func(ctx context.Context) {
		ticker := time.NewTicker(400 * time.Millisecond)
		l := 0
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l++
				s.T().Log("client: rotate loop:", l)
				clock.Advance(4 * time.Second)
			}
		}
	}(advanceCtx)

	s.T().Log("client: send data")
	for i := 0; i < count; i++ {
		s.T().Log("client: send data", i)
		wr := s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		h := common.NewHashdex(data)
		_, errLoop = managerKeeper.Send(baseCtx, h)
		s.Require().NoError(errLoop)

		wrMsg := <-retCh
		s.Equal(wr.String(), wrMsg.String())
	}

	s.T().Log("client: start refil sender loop")
	clock.Advance(2 * time.Second)
	time.Sleep(300 * time.Millisecond)

	s.T().Log("client: shutdown manager")
	err = managerKeeper.Shutdown(baseCtx)
	s.Require().NoError(err)

	files, err := os.ReadDir(dir)
	s.Require().NoError(err)

	if !s.LessOrEqual(len(files), 1, "1 file should remain after shutdown and rotation current.refill") {
		for _, f := range files {
			s.T().Log(f.Name())
		}
	}

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}
