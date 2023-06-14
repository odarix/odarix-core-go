package e2e_test

//go:generate moq -out e2e_moq_test.go -pkg e2e_test -rm ../delivery ConnDialer

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/server"
	"github.com/odarix/odarix-core-go/transport"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"
)

type E2ESuite struct {
	suite.Suite

	connections []net.Conn
	handle      func(*prompb.WriteRequest) bool
}

func TestE2E(t *testing.T) {
	suite.Run(t, new(E2ESuite))
}

func (*E2ESuite) makeData(count int, sid int64) *prompb.WriteRequest {
	wr := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{},
	}

	var (
		startTime int64 = 1654608400000
		step      int64 = 60000
	)

	startTime += step * (sid * 3)

	for i := 0; i < count; i++ {
		wr.Timeseries = append(
			wr.Timeseries,
			prompb.TimeSeries{
				Labels: []prompb.Label{
					{
						Name:  "__name__",
						Value: "test" + strconv.Itoa(i),
					},
					{
						Name:  "instance",
						Value: "blablabla" + strconv.Itoa(i),
					},
					{
						Name:  "job",
						Value: "tester" + strconv.Itoa(i),
					},
					{
						Name:  "low",
						Value: "banan" + strconv.Itoa(i),
					},
					{
						Name:  "zero",
						Value: "non_zero" + strconv.Itoa(i),
					},
				},
				Samples: []prompb.Sample{
					{
						Timestamp: startTime,
						Value:     4444,
					},
					{
						Timestamp: startTime + step,
						Value:     4447,
					},
					{
						Timestamp: startTime + step*2,
						Value:     4448,
					},
				},
			},
		)
	}

	return wr
}

func (s *E2ESuite) createManager(
	ctx context.Context,
	token, address string,
) *delivery.Manager {
	dialer := &ConnDialerMock{
		StringFunc: func() string { return address },
		DialFunc: func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", address)
		},
	}

	dialers := []delivery.Dialer{
		delivery.NewTCPDialer(
			dialer,
			delivery.TCPDialerConfig{
				AuthToken:          token,
				AgentUUID:          uuid.NewString(),
				BackoffMaxInterval: 10 * time.Second,
				BackoffMaxTries:    2,
				Transport: transport.Config{
					ReadTimeout:  5 * time.Second,
					WriteTimeout: 5 * time.Second,
				},
			},
		),
	}

	encoderCtor := func(
		blockID uuid.UUID,
		shardID uint16,
		shardsNumberPower uint8,
	) (delivery.ManagerEncoder, error) {
		return delivery.NewEncoder(shardID, 1<<shardsNumberPower), nil
	}

	rcfg := &delivery.FileStorageConfig{
		Dir:      "/tmp/refill",
		FileName: "current",
	}

	refillCtor := func(
		ctx context.Context,
		blockID uuid.UUID,
		destinations []string,
		shardsNumberPower uint8,
	) (delivery.ManagerRefill, error) {
		return delivery.NewRefill(
			rcfg,
			shardsNumberPower,
			blockID,
			destinations...,
		)
	}

	shardsNumberPower := uint8(0)
	refillInterval := time.Minute
	clock := clockwork.NewFakeClock()

	manager, err := delivery.NewManager(
		ctx,
		dialers,
		encoderCtor,
		refillCtor,
		shardsNumberPower,
		refillInterval,
		s.errorHandler,
		clock,
	)
	s.Require().NoError(err)

	return manager
}

// onAccept function called after authorization
// handler function called on each segment receiving by server
func (s *E2ESuite) runServer(
	ctx context.Context,
	token string,
	onAccept func(*server.TCPReader) bool,
	handler func(uint32, *prompb.WriteRequest) bool,
) net.Listener {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:5000")
	s.Require().NoError(err)

	go func() {
		for {
			conn, err := listener.Accept()
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.Require().NoError(err)
			reader, err := server.NewTCPReader(ctx, &transport.Config{
				ReadTimeout:  5 * time.Second,
				WriteTimeout: time.Second,
			}, conn)
			s.Require().NoError(err)

			s.T().Log("server: client authorization")
			msg, err := reader.Authorization(ctx)
			s.Require().NoError(err)
			if !s.Equal(token, msg.Token) {
				s.NoError(reader.SendResponse(ctx, "Unauthorized", 401, 0))
				s.Require().NoError(reader.Close())
				continue
			}
			s.Require().NoError(reader.SendResponse(ctx, "Ok", http.StatusOK, 0))
			s.T().Log("server: client logged in")

			if onAccept != nil && !onAccept(reader) {
				continue
			}
			go func(reader *server.TCPReader) {
				for {
					if onAccept != nil && !onAccept(reader) {
						return
					}

					segmentID, msg, err := reader.Next(ctx)
					if err != nil {
						if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
							s.T().Errorf("server: %v", err)
						}
						reader.Close()
						return
					}

					if onAccept != nil && !onAccept(reader) {
						return
					}

					if handler(segmentID, msg) {
						reader.SendResponse(ctx, "ok", 200, segmentID)
					} else {
						reader.SendResponse(ctx, "reject", 400, segmentID)
					}
				}
			}(reader)
		}
	}()
	return listener
}

func (s *E2ESuite) errorHandler(msg string, err error) {
	s.T().Logf("errorHandler: %s: %s", msg, err)
}

func (s *E2ESuite) TestHappyPath() {
	// Запускаем сервер(иммитация коллектора), с функцией обработки протобафсообщения(accept, reject),
	// функция с соединением для evict.
	// Создаем менеджер и открываем его.

	var wr *prompb.WriteRequest

	handler := func(segmentID uint32, msg *prompb.WriteRequest) bool {
		s.Equal(wr.String(), msg.String())
		return true
	}
	token := "auth_token"
	baseCtx := context.Background()
	listener := s.runServer(baseCtx, token, nil, handler)
	s.T().Logf("run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	manager := s.createManager(baseCtx, token, listener.Addr().String())
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		wr = s.makeData(5000, int64(i))
		data, err := wr.Marshal()
		s.Require().NoError(err)
		h := delivery.NewHashdex()
		h.PreSharding(context.Background(), data)

		delivered, err := manager.Send(baseCtx, h)
		s.Require().NoError(err)
		s.Require().True(delivered)
		h.Destroy()
	}

	s.T().Log("client: shutdown manager")
	err := manager.Shutdown(baseCtx)
	s.Require().NoError(err)

	s.T().Log("shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

func (s *E2ESuite) TestBreakingConnection() {
	var (
		wr      *prompb.WriteRequest
		breaker int32
	)
	const (
		breakAfterAuth  = 0
		breakBeforeRead = 20
		breakAfterRead  = 40
	)

	handler := func(segmentID uint32, msg *prompb.WriteRequest) bool {
		s.Equal(wr.String(), msg.String())
		return true
	}
	onAccept := func(r *server.TCPReader) bool {
		switch atomic.LoadInt32(&breaker) {
		case breakAfterAuth:
			s.T().Log("server: break connection after auth")
		case breakBeforeRead:
			s.T().Log("server: break connection before read")
		case breakAfterRead:
			s.T().Log("server: break connection after read")
		default:
			atomic.AddInt32(&breaker, 1)
			return true
		}

		atomic.StoreInt32(&breaker, 1)
		err := r.Close()
		s.Require().NoError(err)
		return false
	}
	token := "auth_token"
	baseCtx := context.Background()
	listener := s.runServer(baseCtx, token, onAccept, handler)
	s.T().Logf("run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	manager := s.createManager(baseCtx, token, listener.Addr().String())
	manager.Open(baseCtx)

	s.T().Log("client: send data before break")
	for i := 0; i < 5; i++ {
		wr = s.makeData(5000, int64(i))
		data, err := wr.Marshal()
		s.Require().NoError(err)
		h := delivery.NewHashdex()
		h.PreSharding(context.Background(), data)

		delivered, err := manager.Send(baseCtx, h)
		s.Require().NoError(err)
		s.Require().True(delivered)
		h.Destroy()
	}

	s.T().Log("client: break connection before read")
	atomic.StoreInt32(&breaker, breakBeforeRead)

	for i := 5; i < 10; i++ {
		wr = s.makeData(5000, int64(i))
		data, err := wr.Marshal()
		s.Require().NoError(err)
		h := delivery.NewHashdex()
		h.PreSharding(context.Background(), data)

		delivered, err := manager.Send(baseCtx, h)
		s.Require().NoError(err)
		s.Require().True(delivered)
		h.Destroy()
	}

	s.T().Log("client: break connection after read")
	atomic.StoreInt32(&breaker, breakAfterRead-4)

	for i := 10; i < 15; i++ {
		wr = s.makeData(5000, int64(i))
		data, err := wr.Marshal()
		s.Require().NoError(err)
		h := delivery.NewHashdex()
		h.PreSharding(context.Background(), data)

		delivered, err := manager.Send(baseCtx, h)
		s.Require().NoError(err)
		s.Require().True(delivered)
		h.Destroy()
	}

	s.T().Log("client: shutdown manager")
	err := manager.Shutdown(baseCtx)
	s.Require().NoError(err)

	s.T().Log("shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}

func (s *E2ESuite) TestReject() {
	var (
		wr            *prompb.WriteRequest
		rejectSegment uint32 = 5
	)

	handler := func(segmentID uint32, msg *prompb.WriteRequest) bool {
		s.Equal(wr.String(), msg.String())
		return segmentID != rejectSegment
	}
	token := "auth_token"
	baseCtx := context.Background()
	listener := s.runServer(baseCtx, token, nil, handler)
	s.T().Logf("run server address: %s", listener.Addr().String())

	s.T().Log("client: manager create and open")
	manager := s.createManager(baseCtx, token, listener.Addr().String())
	manager.Open(baseCtx)

	s.T().Log("client: send data")
	for i := 0; i < 10; i++ {
		wr = s.makeData(5000, int64(i))
		data, err := wr.Marshal()
		s.Require().NoError(err)
		h := delivery.NewHashdex()
		h.PreSharding(context.Background(), data)

		delivered, err := manager.Send(baseCtx, h)
		s.Require().NoError(err)
		if i == int(rejectSegment) {
			s.Require().False(delivered)
		} else {
			s.Require().True(delivered)
		}
		h.Destroy()
	}

	_, err := os.Stat(filepath.Clean("/tmp/refill/current.refill"))
	s.Require().NoError(err)

	err = os.Remove(filepath.Clean("/tmp/refill/current.refill"))
	s.Require().NoError(err)

	s.T().Log("client: shutdown manager")
	err = manager.Shutdown(baseCtx)
	s.Require().NoError(err)

	s.T().Log("shutdown listener")
	err = listener.Close()
	s.Require().NoError(err)
}
