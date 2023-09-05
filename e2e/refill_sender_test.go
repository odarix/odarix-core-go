package e2e_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/server"
)

type RefillSenderSuite struct {
	MainSuite

	token string
}

func TestRefillSender(t *testing.T) {
	suite.Run(t, new(RefillSenderSuite))
}

func (s *RefillSenderSuite) SetupSuite() {
	s.token = "auth_token" + s.T().Name()
}

func (*RefillSenderSuite) mkDir() (string, error) {
	return os.MkdirTemp("", filepath.Clean("refill-"))
}

func (*RefillSenderSuite) removeDir(dir string) error {
	return os.RemoveAll(filepath.Clean(dir))
}

func (s *RefillSenderSuite) errorHandler(msg string, err error) {
	s.T().Logf("errorHandler: %s: %s", msg, err)
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *RefillSenderSuite) TestRefillSenderHappyPath() {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *prompb.WriteRequest, count*2)
	rejectsCh := make(chan *prompb.WriteRequest, count*2)

	handleStream := func(ctx context.Context, fe *frames.ReadFrame, tcpReader *server.TCPReader) {
		reader := server.NewProtocolReader(server.StartWith(tcpReader, fe))
		defer reader.Destroy()
		for {
			rq, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			retCh <- rq.Message

			if rq.SegmentID%2 == 0 {
				if !s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
					Text:      "reject",
					Code:      400,
					SegmentID: rq.SegmentID,
					SendAt:    rq.SentAt,
				}), "fail to send response") {
					return
				}
				rejectsCh <- rq.Message
				continue
			}

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
		rmsg := frames.NewRefillMsgEmpty()
		if !s.NoError(rmsg.UnmarshalBinary(fe.GetBody()), "unmarshal binary") {
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
		for i := 0; i < len(rmsg.Messages); i++ {
			fe, errNext := tcpReader.Next(ctx)
			if !s.NoError(errNext, "fail next") {
				return
			}

			switch fe.GetType() {
			case frames.SnapshotType, frames.DrySegmentType, frames.SegmentType:
				if _, err = fe.WriteTo(file); !s.NoError(err, "fail write") {
					return
				}
			default:
				s.T().Errorf("unexpected msg type %d", fe.GetType())
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
			rq, err := pr.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				break
			}

			s.Require().NotEqual(0, len(rejectsCh))
			ewr := <-rejectsCh
			s.Equal(ewr.String(), rq.Message.String())
		}

		s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
			Text: "OK",
			Code: 200,
		}), "fail to send response")
	}

	listener := s.runServer(baseCtx, "127.0.0.1:7000", s.token, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < count; i++ {
		wr := s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		delivered, errLoop := manager.Send(baseCtx, newProtoDataTest(data))
		s.Require().NoError(errLoop)
		if i%2 == 0 {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}

		wrMsg := <-retCh
		s.Equal(wr.String(), wrMsg.String())
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	rscfg := delivery.RefillSendManagerConfig{
		ScanInterval:  2 * time.Second,
		MaxRefillSize: 10000000, // 10mb
	}

	s.T().Log("init and run refill manager")
	dialers := s.createDialers(s.token, listener.Addr().String())
	rsmanager, err := delivery.NewRefillSendManager(
		rscfg,
		dir,
		dialers,
		s.errorHandler,
		clockwork.NewRealClock(),
		nil,
	)
	s.Require().NoError(err)
	ctx, cancel := context.WithCancelCause(baseCtx)
	time.AfterFunc(
		4*time.Second,
		func() {
			cancel(delivery.ErrShutdown)
		},
	)
	s.T().Log("client: send refill")
	rsmanager.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)

	s.T().Log("client: check refill files")
	files, err := os.ReadDir(filepath.Join(dir, delivery.RefillDir))
	s.Require().NoError(err)
	s.Equal(0, len(files))
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *RefillSenderSuite) TestRefillSenderBreakingConnection() {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *prompb.WriteRequest, count*2)
	rejectsCh := make(chan *prompb.WriteRequest, count*2)

	var (
		breaker int32 = 10
	)
	const (
		breakAfterAuth  = 0
		breakBeforeRead = 2
		breakAfterRead  = 5
	)

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
		reader := server.NewProtocolReader(server.StartWith(tcpReader, fe))
		defer reader.Destroy()
		for {
			rq, err := reader.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				return
			}

			// process data
			retCh <- rq.Message

			if rq.SegmentID%2 == 0 {
				if !s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
					Text:      "reject",
					Code:      400,
					SegmentID: rq.SegmentID,
					SendAt:    rq.SentAt,
				}), "fail to send response") {
					return
				}
				rejectsCh <- rq.Message
				continue
			}

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
		rmsg := frames.NewRefillMsgEmpty()
		if !s.NoError(rmsg.UnmarshalBinary(fe.GetBody()), "unmarshal binary") {
			return
		}

		// make tmp file for work
		dir, err := os.MkdirTemp("", "testDir-")
		if !s.NoError(err, "fail mkdir") {
			return
		}
		defer os.RemoveAll(dir)
		file, err := os.CreateTemp(dir, "testFile-")
		if !s.NoError(err, "fail open file") {
			return
		}

		// save Messages to file
		for i := 0; i < len(rmsg.Messages); i++ {
			if onAccept != nil && !onAccept() {
				s.T().Log("handleRefill: disconnect before read")
				return
			}

			fe, errNext := tcpReader.Next(ctx)
			if !s.NoError(errNext, "fail next") {
				return
			}

			switch fe.GetType() {
			case frames.SnapshotType, frames.DrySegmentType, frames.SegmentType:
				if _, err = fe.WriteTo(file); !s.NoError(err, "fail write") {
					return
				}
			default:
				s.T().Errorf("unexpected msg type %d", fe.GetType())
				return
			}
		}
		if !s.NoError(file.Sync(), "sync file") {
			return
		}
		if !s.NoError(file.Close(), "close file") {
			return
		}

		if onAccept != nil && !onAccept() {
			s.T().Log("handleRefill: disconnect after read")
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
			rq, err := pr.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
					s.NoError(err, "fail to read next message")
				}
				break
			}

			s.Require().NotEqual(0, len(rejectsCh))
			ewr := <-rejectsCh
			s.Equal(ewr.String(), rq.Message.String())
		}

		tcpReader.SendResponse(ctx, &frames.ResponseMsg{
			Text: "OK",
			Code: 200,
		})
	}

	listener := s.runServer(baseCtx, "127.0.0.1:7001", s.token, onAccept, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManager(baseCtx, s.token, listener.Addr().String(), dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < count; i++ {
		wr := s.makeData(5000, int64(i))
		data, errLoop := wr.Marshal()
		s.Require().NoError(errLoop)
		delivered, errLoop := manager.Send(baseCtx, newProtoDataTest(data))
		s.Require().NoError(errLoop)
		if i%2 == 0 {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}

		wrMsg := <-retCh
		s.Equal(wr.String(), wrMsg.String())
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	rscfg := delivery.RefillSendManagerConfig{
		ScanInterval:  1 * time.Second,
		MaxRefillSize: 10000000, // 10mb
	}

	atomic.StoreInt32(&breaker, 0)
	s.T().Log("client: init and run refill manager")
	dialers := s.createDialers(s.token, listener.Addr().String())
	rsmanager, err := delivery.NewRefillSendManager(
		rscfg,
		dir,
		dialers,
		s.errorHandler,
		clockwork.NewRealClock(),
		nil,
	)
	s.Require().NoError(err)
	ctx, cancel := context.WithCancelCause(baseCtx)
	time.AfterFunc(
		6*time.Second,
		func() {
			cancel(delivery.ErrShutdown)
		},
	)

	s.T().Log("client: send refill")
	rsmanager.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	err = rsmanager.Shutdown(shutdownCtx)
	s.Require().NoError(err)

	s.T().Log("client: shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)

	files, err := os.ReadDir(filepath.Join(dir, delivery.RefillDir))
	s.Require().NoError(err)
	if !s.Equal(0, len(files)) {
		for _, f := range files {
			s.T().Log(f.Name())
		}
	}
}
