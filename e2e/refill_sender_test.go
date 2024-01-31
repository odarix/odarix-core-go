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
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/server"
	"github.com/stretchr/testify/suite"
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

func (s *RefillSenderSuite) TestRefillSenderHappyPath() {
	s.refillSenderHappyPath(protobufOpenHeadSender{})
	s.refillSenderHappyPath(goModelOpenHeadSender{})
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *RefillSenderSuite) refillSenderHappyPath(sender OpenHeadSenderGenerator) {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *frames.ReadSegmentV4, count*2)
	rejectsCh := make(chan *frames.ReadSegmentV4, count*2)

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
			if fr.ID%2 == 0 {
				if !s.NoError(reader.SendResponse(
					ctx,
					frames.NewResponseV4(fr.SentAt, fr.ID, 400, "reject"),
				), "fail to send response") {
					return
				}
				rejectsCh <- fr
				continue
			}

			if !s.NoError(reader.SendResponse(
				ctx,
				frames.NewResponseV4(fr.SentAt, fr.ID, 200, "OK"),
			), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, rw http.ResponseWriter, r io.Reader) {
		// make tmp file for work
		dir, errh := os.MkdirTemp("", "refill-server-dir-")
		if !s.NoError(errh, "fail mkdir") {
			return
		}
		defer s.removeDir(dir)
		file, errh := os.CreateTemp(dir, "refill-server-file-")
		if !s.NoError(errh, "fail open file") {
			return
		}

		// save to file from reader
		if _, errh = file.ReadFrom(r); errh != nil {
			_ = s.response(rw, errh.Error(), http.StatusInternalServerError)
			return
		}
		if errh = file.Sync(); errh != nil {
			_ = s.response(rw, errh.Error(), http.StatusInternalServerError)
			return
		}
		if errh = file.Close(); errh != nil {
			_ = s.response(rw, errh.Error(), http.StatusInternalServerError)
			return
		}

		// make FileReader
		fr, errh := server.NewFileReaderV4(file.Name())
		if !s.NoError(errh, "fail init FileReader") {
			return
		}
		defer fr.Close()

		// make BlockWriter
		// read until EOF from ProtocolReader and append to BlockWriter
		// save BlockWriter
		// send block to S3
		for {
			rs, errh := fr.Next(ctx)
			if errh != nil {
				if !errors.Is(errh, io.EOF) && !errors.Is(errh, context.Canceled) {
					s.NoError(errh, "fail to read next message")
				}
				break
			}

			s.Require().NotEqual(0, len(rejectsCh))
			rfr := <-rejectsCh
			s.Equal(rfr.ID, rs.ID)
		}

		s.NoError(s.response(rw, "OK", http.StatusOK), "fail to send response")
	}

	addr := "127.0.0.1:7000"
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
	for i := 0; i < count; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		if i%2 == 0 {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}

		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: shutdown manager")
	s.Require().NoError(manager.Close())
	s.Require().NoError(manager.Shutdown(baseCtx))

	rscfg := delivery.RefillSendManagerConfig{
		ScanInterval:  2 * time.Second,
		MaxRefillSize: 10000000, // 10mb
	}

	s.T().Log("init and run refill manager")
	dialers := s.createDialersWebSocket(tlscfg, s.token, addr)
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

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(ctx))

	s.T().Log("client: check refill files")
	files, err := os.ReadDir(filepath.Join(dir, delivery.RefillDir))
	s.Require().NoError(err)
	s.Equal(0, len(files))
}

func (s *RefillSenderSuite) TestRefillSenderBreakingConnection() {
	s.refillSenderBreakingConnection(protobufOpenHeadSender{})
	s.refillSenderBreakingConnection(goModelOpenHeadSender{})
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *RefillSenderSuite) refillSenderBreakingConnection(sender OpenHeadSenderGenerator) {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *frames.ReadSegmentV4, count*2)
	rejectsCh := make(chan *frames.ReadSegmentV4, count*2)

	var (
		breaker int32 = 10
	)
	const (
		breakAfterAuth  = 0
		breakBeforeRead = 2
		breakAfterRead  = 5
	)

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
			if fr.ID%2 == 0 {
				if !s.NoError(reader.SendResponse(
					ctx,
					frames.NewResponseV4(fr.SentAt, fr.ID, 400, "reject"),
				), "fail to send response") {
					return
				}
				rejectsCh <- fr
				continue
			}

			if !s.NoError(reader.SendResponse(
				ctx,
				frames.NewResponseV4(fr.SentAt, fr.ID, 200, "OK"),
			), "fail to send response") {
				return
			}
		}
	}

	handleRefill := func(ctx context.Context, rw http.ResponseWriter, r io.Reader) {
		// make tmp file for work
		dir, errh := os.MkdirTemp("", "refill-server-dir-")
		if !s.NoError(errh, "fail mkdir") {
			return
		}
		defer s.removeDir(dir)
		file, errh := os.CreateTemp(dir, "refill-server-file-")
		if !s.NoError(errh, "fail open file") {
			return
		}

		// save to file from reader
		if _, errh = file.ReadFrom(r); errh != nil {
			_ = s.response(rw, errh.Error(), http.StatusInternalServerError)
			return
		}
		if errh = file.Sync(); errh != nil {
			_ = s.response(rw, errh.Error(), http.StatusInternalServerError)
			return
		}
		if errh = file.Close(); errh != nil {
			_ = s.response(rw, errh.Error(), http.StatusInternalServerError)
			return
		}

		if onAccept != nil && !onAccept() {
			s.T().Log("handleRefill: disconnect after read")
			return
		}

		// make FileReader
		fr, errh := server.NewFileReaderV4(file.Name())
		if !s.NoError(errh, "fail init FileReader") {
			return
		}
		defer fr.Close()

		// make BlockWriter
		// read until EOF from ProtocolReader and append to BlockWriter
		// save BlockWriter
		// send block to S3
		for {
			rs, errh := fr.Next(ctx)
			if errh != nil {
				if !errors.Is(errh, io.EOF) && !errors.Is(errh, context.Canceled) {
					s.NoError(errh, "fail to read next message")
				}
				break
			}

			s.Require().NotEqual(0, len(rejectsCh))
			rfr := <-rejectsCh
			s.Equal(rfr.ID, rs.ID)
		}

		s.NoError(s.response(rw, "OK", http.StatusOK), "fail to send response")
	}

	addr := "127.0.0.1:7001"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, onAccept, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager create and open")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	manager, err := s.createManagerWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler)
	s.Require().NoError(err)
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < count; i++ {
		_, delivered, errLoop := sender.SendOpenHead(baseCtx, manager, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		if i%2 == 0 {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}

		fr := <-retCh
		s.EqualValues(i, fr.ID)
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
	dialers := s.createDialersWebSocket(tlscfg, s.token, addr)
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

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(ctx))

	files, err := os.ReadDir(filepath.Join(dir, delivery.RefillDir))
	s.Require().NoError(err)
	if !s.Equal(0, len(files)) {
		for _, f := range files {
			s.T().Log(f.Name())
		}
	}
}
