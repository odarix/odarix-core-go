package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/model"
	"github.com/odarix/odarix-core-go/server"
	"github.com/odarix/odarix-core-go/transport"
)

const (
	testTimeSeriesCount = 5000
)

type MainSuite struct {
	suite.Suite
}

func (*MainSuite) makeProtobufData(count int, sid int64) *prompb.WriteRequest {
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
						Name:  "__replica__",
						Value: "blablabla" + strconv.Itoa(i),
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

func (*MainSuite) makeGoModelData(count int, sid int64) (timeSeriesSlice []model.TimeSeries) {
	var (
		startTime int64 = 1654608400000
		step      int64 = 60000
	)

	startTime += step * (sid * 3)

	for i := 0; i < count; i++ {
		timeSeriesSlice = append(timeSeriesSlice, model.TimeSeries{
			LabelSet: model.NewLabelSetBuilder().
				Set("__name__", "test"+strconv.Itoa(i)).
				Set("__replica__", "blobloblo"+strconv.Itoa(i)).
				Set("instance", "blobloblo"+strconv.Itoa(i)).
				Set("job", "tester"+strconv.Itoa(i)).
				Set("low", "banan"+strconv.Itoa(i)).
				Set("zero", "non_zero"+strconv.Itoa(i)).
				Build(),
			Timestamp: uint64(startTime),
			Value:     5555,
		})
	}

	return timeSeriesSlice
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
			clockwork.NewFakeClock(),
			nil,
		),
	}
}

func (s *MainSuite) createManager(
	ctx context.Context,
	token, address, dir string,
	errorHandler delivery.ErrorHandler,
	clock clockwork.Clock,
) (*delivery.Manager, error) {
	dialers := s.createDialers(token, address)

	encoderCtor := func(
		blockID uuid.UUID,
		shardID uint16,
		logShards uint8,
	) (delivery.ManagerEncoder, error) {
		return cppbridge.NewWALEncoder(shardID, logShards), nil
	}

	refillCtor := func(
		workingDir string,
		blockID uuid.UUID,
		destinations []string,
		shardsNumberPower uint8,
		alwaysToRefill bool,
		registerer prometheus.Registerer,
	) (delivery.ManagerRefill, error) {
		return delivery.NewRefill(
			workingDir,
			shardsNumberPower,
			blockID,
			alwaysToRefill,
			registerer,
			destinations...,
		)
	}

	shardsNumberPower := uint8(0)
	haTracker := delivery.NewHighAvailabilityTracker(ctx, nil, clock)
	rejectNotifyer := delivery.NewRotateTimer(
		clock,
		delivery.BlockLimits{DesiredBlockFormationDuration: time.Hour, DelayAfterNotify: 5 * time.Minute},
	)
	manager, err := delivery.NewManager(
		ctx,
		dialers,
		cppbridge.HashdexFactory{},
		encoderCtor,
		refillCtor,
		shardsNumberPower,
		time.Minute,
		dir,
		delivery.DefaultLimits(),
		rejectNotifyer,
		haTracker,
		errorHandler,
		clock,
		nil,
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
	handleStream func(ctx context.Context, msg *frames.ReadFrame, tcpReader *server.TCPReader),
	handleRefill func(ctx context.Context, msg *frames.ReadFrame, tcpReader *server.TCPReader),
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
				if errors.Is(err, io.EOF) {
					return
				}
				s.Require().NoError(err, "fail to read auth message")

				s.T().Log("server: authorization")
				if !s.Equal(token, auth.Token) {
					s.NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
						Text: "Unauthorized",
						Code: http.StatusUnauthorized,
					}))
					return
				}
				s.Require().NoError(tcpReader.SendResponse(ctx, &frames.ResponseMsg{
					Text: "OK",
					Code: 200,
				}))
				s.T().Log("server: logged in")

				if onAccept != nil && !onAccept() {
					s.T().Log("server: disconnect after auth")
					return
				}

				fe, err := tcpReader.Next(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
						s.NoError(err, "fail to read next message")
					}
					return
				}

				if fe.GetType() == frames.RefillType {
					handleRefill(ctx, fe, tcpReader)
				} else {
					handleStream(ctx, fe, tcpReader)
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

	rsmanagerCtor := func(
		rsmCfg delivery.RefillSendManagerConfig,
		workingDir string,
		dialers []delivery.Dialer,
		errorHandler delivery.ErrorHandler,
		clock clockwork.Clock,
		registerer prometheus.Registerer,
	) (delivery.ManagerRefillSender, error) {
		return delivery.NewRefillSendManager(rsmCfg, workingDir, dialers, errorHandler, clock, registerer)
	}

	encoderCtor := func(
		blockID uuid.UUID,
		shardID uint16,
		logShards uint8,
	) (delivery.ManagerEncoder, error) {
		return cppbridge.NewWALEncoder(shardID, logShards), nil
	}

	refillCtor := func(
		workinDir string,
		blockID uuid.UUID,
		destinations []string,
		shardsNumberPower uint8,
		alwaysToRefill bool,
		registerer prometheus.Registerer,
	) (delivery.ManagerRefill, error) {
		return delivery.NewRefill(
			workinDir,
			shardsNumberPower,
			blockID,
			alwaysToRefill,
			registerer,
			destinations...,
		)
	}

	cfg := delivery.ManagerKeeperConfig{
		ShutdownTimeout:       12 * time.Second,
		UncommittedTimeWindow: 6 * time.Second,
		WorkingDir:            dir,
		RefillSenderManager: delivery.RefillSendManagerConfig{
			ScanInterval:  2 * time.Second,
			MaxRefillSize: 10000000, // 10mb
		},
	}

	managerKeeper, err := delivery.NewManagerKeeper(
		ctx,
		cfg,
		delivery.NewManager,
		cppbridge.HashdexFactory{},
		encoderCtor,
		refillCtor,
		rsmanagerCtor,
		clock,
		dialers,
		errorHandler,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return managerKeeper, nil
}

func (s *MainSuite) EqualAsJSON(expected, actual interface{}, args ...interface{}) bool {
	e, err := json.Marshal(expected)
	s.Require().NoError(err)
	a, err := json.Marshal(actual)
	s.Require().NoError(err)
	return s.JSONEq(string(e), string(a), args...)
}

func (s *MainSuite) EqualAsJSONf(expected, actual interface{}, msg string, args ...interface{}) bool {
	e, err := json.Marshal(expected)
	s.Require().NoError(err)
	a, err := json.Marshal(actual)
	s.Require().NoError(err)
	return s.JSONEqf(string(e), string(a), msg, args...)
}

// protoDataTest - test data.
type protoDataTest struct {
	data []byte
}

func newProtoDataTest(data []byte) *protoDataTest {
	return &protoDataTest{
		data: data,
	}
}

// Bytes - return bytes, for implements.
func (pd *protoDataTest) Bytes() []byte {
	return pd.data
}

// Destroy - clear memory, for implements.
func (pd *protoDataTest) Destroy() {
	pd.data = nil
}

type GeneratedData interface {
	AsRemoteWriteProto() *prompb.WriteRequest
}
type Sender interface {
	SendOpenHeadProtobuf(ctx context.Context, protoData delivery.ProtoData) (delivered bool, err error)
	SendOpenHeadGoModel(ctx context.Context, data []model.TimeSeries) (delivered bool, err error)
}

type OpenHeadSenderGenerator interface {
	SendOpenHead(ctx context.Context, sender Sender, count int, sid int64) (generatedData GeneratedData, delivered bool, err error)
}

type protobufOpenHeadSender struct{}

type protobufGeneratedData struct {
	wr *prompb.WriteRequest
}

func (d *protobufGeneratedData) AsRemoteWriteProto() *prompb.WriteRequest {
	return d.wr
}

func (s protobufOpenHeadSender) SendOpenHead(ctx context.Context, sender Sender, count int, sid int64) (generatedData GeneratedData, delivered bool, err error) {
	wr := (&MainSuite{}).makeProtobufData(count, sid)
	encodedData, err := wr.Marshal()
	if err != nil {
		return nil, false, err
	}
	delivered, err = sender.SendOpenHeadProtobuf(ctx, newProtoDataTest(encodedData))
	return &protobufGeneratedData{wr: wr}, delivered, err
}

type goModelOpenHeadSender struct{}

type goModelGeneratedData struct {
	timeSeriesSlice []model.TimeSeries
}

func (d *goModelGeneratedData) AsRemoteWriteProto() *prompb.WriteRequest {
	wr := &prompb.WriteRequest{}
	for _, timeSeries := range d.timeSeriesSlice {
		ts := prompb.TimeSeries{}
		for i := 0; i < timeSeries.LabelSet.Len(); i++ {
			ts.Labels = append(ts.Labels, prompb.Label{
				Name:  timeSeries.LabelSet.Key(i),
				Value: timeSeries.LabelSet.Value(i),
			})
		}
		ts.Samples = append(ts.Samples, prompb.Sample{
			Timestamp: int64(timeSeries.Timestamp),
			Value:     timeSeries.Value,
		})
		wr.Timeseries = append(wr.Timeseries, ts)
	}

	return wr
}

func (goModelOpenHeadSender) SendOpenHead(ctx context.Context, sender Sender, count int, sid int64) (generatedData GeneratedData, delivered bool, err error) {
	timeSeriesSlice := (&MainSuite{}).makeGoModelData(count, sid)
	delivered, err = sender.SendOpenHeadGoModel(ctx, timeSeriesSlice)
	return &goModelGeneratedData{timeSeriesSlice: timeSeriesSlice}, delivered, err
}
