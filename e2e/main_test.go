package e2e_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/server"
	"github.com/odarix/odarix-core-go/transport"
)

type MainSuite struct {
	suite.Suite
}

func (*MainSuite) makeData(count int, sid int64) *prompb.WriteRequest {
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

func (*MainSuite) createDialers(token, address string) []delivery.Dialer {
	dialer := &ConnDialerMock{
		StringFunc: func() string { return address },
		DialFunc: func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", address)
		},
	}

	return []delivery.Dialer{
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
}

func (s *MainSuite) createManager(
	ctx context.Context,
	token, address, dir string,
	errorHandler delivery.ErrorHandler,
) (*delivery.Manager, error) {
	dialers := s.createDialers(token, address)

	encoderCtor := func(
		blockID uuid.UUID,
		shardID uint16,
		shardsNumberPower uint8,
	) (delivery.ManagerEncoder, error) {
		return common.NewEncoder(shardID, 1<<shardsNumberPower), nil
	}

	rcfg := &delivery.FileStorageConfig{
		Dir:      dir,
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
		errorHandler,
		clock,
	)
	if err != nil {
		return nil, err
	}

	return manager, nil
}

// onAccept function called after authorization
// handleStream function called on each segment receiving by server
// handleRefill function called on each refill receiving by server
//
//revive:disable-next-line:cyclomatic this is test
//revive:disable-next-line:cognitive-complexity this is test
func (s *MainSuite) runServer(
	ctx context.Context,
	listen, token string,
	onAccept func() bool,
	handleStream func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader),
	handleRefill func(ctx context.Context, msg *transport.RawMessage, tcpReader *server.TCPReader),
) net.Listener {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", listen)
	s.Require().NoError(err)

	go func() {
		for {
			conn, err := listener.Accept()
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.Require().NoError(err)

			go func(conn net.Conn) {
				defer conn.Close()
				tcpReader := server.NewTCPReader(
					&transport.Config{
						ReadTimeout:  5 * time.Second,
						WriteTimeout: time.Second,
					},
					conn,
				)
				auth, err := tcpReader.Authorization(ctx)
				s.Require().NoError(err, "fail to read auth message")

				s.T().Log("server: authorization")
				if !s.Equal(token, auth.Token) {
					s.NoError(tcpReader.SendResponse(ctx, "Unauthorized", 401, 0))
					return
				}
				s.Require().NoError(tcpReader.SendResponse(ctx, "Ok", http.StatusOK, 0))
				s.T().Log("server: logged in")

				if onAccept != nil && !onAccept() {
					s.T().Log("server: disconnect after auth")
					return
				}

				msg, err := tcpReader.Next(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
						s.NoError(err, "fail to read next message")
					}
					return
				}

				if msg.Header.Type == transport.MsgRefill {
					handleRefill(ctx, msg, tcpReader)
				} else {
					handleStream(ctx, msg, tcpReader)
				}
			}(conn)
		}
	}()
	return listener
}

func (s *MainSuite) createManagerKeeper(
	ctx context.Context,
	token, address, dir string,
	errorHandler delivery.ErrorHandler,
	clock clockwork.Clock,
) (*delivery.ManagerKeeper, error) {
	dialers := s.createDialers(token, address)

	managerCtor := func(
		rsmCfg *delivery.RefillSendManagerConfig,
		dialers []delivery.Dialer,
		errorHandler delivery.ErrorHandler,
		clock clockwork.Clock,
	) (delivery.ManagerRefillSender, error) {
		return delivery.NewRefillSendManager(rsmCfg, dialers, errorHandler, clock)
	}

	encoderCtor := func(
		blockID uuid.UUID,
		shardID uint16,
		shardsNumberPower uint8,
	) (delivery.ManagerEncoder, error) {
		return common.NewEncoder(shardID, 1<<shardsNumberPower), nil
	}

	rcfg := &delivery.FileStorageConfig{
		Dir:      dir,
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

	cfg := &delivery.ManagerKeeperConfig{
		RotateInterval: 4 * time.Second,
		RefillInterval: 5 * time.Second,
		RefillSenderManager: &delivery.RefillSendManagerConfig{
			Dir:           dir,
			ScanInterval:  2 * time.Second,
			MaxRefillSize: 10000000, // 10mb
		},
	}

	managerKeeper, err := delivery.NewManagerKeeper(
		ctx,
		cfg,
		delivery.NewManager,
		encoderCtor,
		refillCtor,
		managerCtor,
		clock,
		dialers,
		errorHandler,
	)
	if err != nil {
		return nil, err
	}

	return managerKeeper, nil
}
