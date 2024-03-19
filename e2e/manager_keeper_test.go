package e2e_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/server"
)

type ManagerKeeperSuite struct {
	MainSuite

	token string
}

func TestManagerKeeper(t *testing.T) {
	suite.Run(t, new(ManagerKeeperSuite))
}

func (s *ManagerKeeperSuite) SetupSuite() {
	s.token = "auth_token_keeper" + s.T().Name()
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

func (s *ManagerKeeperSuite) TestManagerKeeperHappyPath() {
	s.managerKeeperHappyPath(protobufOpenHeadSenderKeeper{})
	s.managerKeeperHappyPath(goModelOpenHeadSenderKeeper{})
}

func (s *ManagerKeeperSuite) managerKeeperHappyPath(sender OpenHeadSenderKeeperGenerator) {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *frames.ReadSegmentV4, count)

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

	addr := "127.0.0.1:5000"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager keeper create")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.NoError(s.removeDir(dir))
	clock := clockwork.NewRealClock()
	managerKeeper, err := s.createManagerKeeperWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler, clock)
	s.Require().NoError(err)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		generatedData, delivered, errLoop := sender.SendOpenHead(baseCtx, managerKeeper, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.Require().True(delivered)

		_ = generatedData
		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: shutdown manager")
	err = managerKeeper.Shutdown(baseCtx)
	s.Require().NoError(err)

	fr := <-retCh
	s.True(fr.Size == 0)

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(baseCtx))
}

func (s *ManagerKeeperSuite) TestWithRotate() {
	s.withRotate(protobufOpenHeadSenderKeeper{})
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *ManagerKeeperSuite) withRotate(sender OpenHeadSenderKeeperGenerator) {
	count := 10
	baseCtx := context.Background()
	retCh := make(chan *frames.ReadSegmentV4, count*2)
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

	addr := "127.0.0.1:5001"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager keeper create")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	clock := clockwork.NewFakeClock()
	managerKeeper, err := s.createManagerKeeperWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler, clock)
	s.Require().NoError(err)

	s.T().Log("client: run time shift")
	advanceCtx, advanceCancel := context.WithCancel(baseCtx)
	go func(ctx context.Context) {
		timer := time.NewTimer(500 * time.Millisecond)
		l := 0
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				l++
				s.T().Log("client: rotate loop:", l)
				clock.Advance(2 * time.Second)
				timer.Reset(500 * time.Millisecond)
			}
		}
	}(advanceCtx)

	s.T().Log("client: send data")
	for i := 0; i < count; i++ {
		generatedData, delivered, errLoop := sender.SendOpenHead(baseCtx, managerKeeper, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		s.True(delivered)
		_ = generatedData
		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}
	advanceCancel()

	s.T().Log("client: shutdown manager")
	err = managerKeeper.Shutdown(baseCtx)
	s.Require().NoError(err)

	fr := <-retCh
	s.True(fr.Size == 0)

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(baseCtx))
}

func (s *ManagerKeeperSuite) TestWithReject() {
	s.withReject(protobufOpenHeadSenderKeeper{})
	s.withReject(goModelOpenHeadSenderKeeper{})
}

//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *ManagerKeeperSuite) withReject(sender OpenHeadSenderKeeperGenerator) {
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

	addr := "127.0.0.1:5002"
	srv := s.runWebServer(baseCtx, addr, s.token, tlscfg, nil, handleStream, handleRefill)
	s.T().Logf("client: run server address: %s", addr)

	s.T().Log("client: manager keeper create")
	dir, err := s.mkDir()
	s.Require().NoError(err)
	defer s.removeDir(dir)
	clock := clockwork.NewFakeClock()
	managerKeeper, err := s.createManagerKeeperWithWebSocket(baseCtx, tlscfg, s.token, addr, dir, s.errorHandler, clock)
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
		generatedData, _, errLoop := sender.SendOpenHead(baseCtx, managerKeeper, testTimeSeriesCount, int64(i))
		s.Require().NoError(errLoop)
		_ = generatedData
		fr := <-retCh
		s.EqualValues(i, fr.ID)
	}

	s.T().Log("client: start refil sender loop")
	clock.Advance(2 * time.Second)
	time.Sleep(300 * time.Millisecond)

	s.T().Log("client: shutdown manager")
	err = managerKeeper.Shutdown(baseCtx)
	s.Require().NoError(err)

	files, err := os.ReadDir(filepath.Join(dir, delivery.RefillDir))
	s.Require().NoError(err)

	if !s.LessOrEqual(len(files), 1, "1 file should remain after shutdown and rotation current.refill") {
		for _, f := range files {
			s.T().Log(f.Name())
		}
	}

	s.T().Log("client: shutdown server")
	s.Require().NoError(srv.Shutdown(baseCtx))
}
