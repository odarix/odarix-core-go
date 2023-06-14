package delivery_test

/*
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/delivery"
)

func TestAuthMsg(t *testing.T) {
	t.Log("Init data for message")
	expectedToken := uuid.NewString()
	expectedAgentUUID := uuid.NewString()
	data := (&delivery.AuthMsg{
		Token:     expectedToken,
		AgentUUID: expectedAgentUUID,
	}).EncodeBinary()

	t.Log("Init new message for decode")
	nam := &delivery.AuthMsg{}
	nam.DecodeBinary(data)

	t.Log("compare the received data")
	assert.Equal(t, expectedToken, nam.Token, "Token not equal")
	assert.Equal(t, expectedAgentUUID, nam.AgentUUID, "AgentUUID not equal")
}

func TestResponseMsg(t *testing.T) {
	t.Log("Init data for message")
	expectedCode := uint32(faker.RandomUnixTime())
	expectedText := faker.Paragraph()
	expectedSegmentID := uint32(faker.RandomUnixTime())
	data := (&delivery.ResponseMsg{
		Code:      expectedCode,
		Text:      expectedText,
		SegmentID: expectedSegmentID,
	}).EncodeBinary()

	t.Log("Init new message for decode")
	nrm := &delivery.ResponseMsg{}
	nrm.DecodeBinary(data)

	t.Log("compare the received data")
	assert.Equal(t, expectedCode, nrm.Code, "Code not equal")
	assert.Equal(t, expectedText, nrm.Text, "Text not equal")
	assert.Equal(t, expectedSegmentID, nrm.SegmentID, "SegmentID not equal")
}

func TestRefillMsg(t *testing.T) {
	t.Log("Init data for message")
	expectedNumberOfMessage := uint32(faker.RandomUnixTime())
	data := (&delivery.RefillMsg{
		NumberOfMessage: expectedNumberOfMessage,
	}).EncodeBinary()

	t.Log("Init new message for decode")
	nrm := &delivery.RefillMsg{}
	nrm.DecodeBinary(data)

	t.Log("compare the received data")
	assert.Equal(t, expectedNumberOfMessage, nrm.NumberOfMessage, "NumberOfMessage not equal")
}

type TransportSuite struct {
	suite.Suite

	cfg       *delivery.DialerConfig
	readerMoq *TCPReaderMoq
	ctx       context.Context
	cancel    context.CancelFunc
	data      delivery.Segment
	listener  net.Listener
	done      chan struct{}
}

func TestTransportSuite(t *testing.T) {
	suite.Run(t, new(TransportSuite))
}

func (ts *TransportSuite) SetupSuite() {
	ts.ctx, ts.cancel = context.WithCancel(context.Background())
	ts.cfg = &delivery.DialerConfig{
		Host:        "127.0.0.1",
		Port:        "5000",
		MaxInterval: 10 * time.Second,
		MaxTries:    2,
		Transport: &delivery.TCPTransportConfig{
			AuthToken:       "AgentUUID",
			AgentUUID:       "AuthToken",
			ProtocolVersion: 3,
			NetTransport: &delivery.NetTransportConfig{
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 5 * time.Second,
			},
		},
	}

	ts.data = delivery.Segment{
		1, 2, 3, 4, 5, 6,
	}
}

func (ts *TransportSuite) SetupTest() {
	var err error
	ts.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%s", ts.cfg.Host, ts.cfg.Port))
	ts.NoError(err)
}

func (ts *TransportSuite) TearDownTest() {
	err := ts.listener.Close()
	ts.NoError(err)
}

func (ts *TransportSuite) TestRun() {
	ts.T().Log("run test in parallel")

	ts.done = make(chan struct{})

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go func() {
		defer wg.Done()
		ts.T().Log("run server")
		ts.server()
	}()

	go func() {
		defer wg.Done()
		ts.T().Log("run client")
		ts.client()
	}()

	wg.Wait()
}

func (ts *TransportSuite) server() {
	conn, err := ts.listener.Accept()
	ts.NoError(err)
	ts.T().Log("server: get income connection")

	ts.T().Log("server: init")
	ts.readerMoq, err = NewTCPReaderMoq(ts.ctx, ts.cfg.Transport.NetTransport, conn)
	ts.NoError(err)

	authFn := func(_ context.Context, cfs CtxFields, token string) (Project, CtxFields, error) {
		ts.Equal(ts.cfg.Transport.AuthToken, token)
		return nil, cfs, nil
	}

	ts.T().Log("server: read auth")
	var cfs CtxFields
	cfs = &ctxFields{}
	raw, err := ts.readerMoq.tt.Read(ts.ctx)
	ts.NoError(err)
	cfs = cfs.SetProtocolVersion(uint8(raw.Header.Version))

	_, cfs, err = ts.readerMoq.Auth(ts.ctx, cfs, raw.Payload, authFn)
	ts.NoError(err)

	err = ts.readerMoq.SendResponse(cfs, "ok", 200, 0)
	ts.NoError(err)

	ts.T().Log("server: read message")

	go func() {
		<-ts.done
		ts.T().Log("server: done")
		err = ts.readerMoq.Close()
		ts.NoError(err)
	}()

	for {
		data, err := ts.readerMoq.Handle(cfs)
		if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
			return
		}
		ts.NoError(err)

		if data[0] == 3 {
			err = ts.readerMoq.SendResponse(cfs, "NonFitForStreaming", 455, uint32(data[0]))
			ts.NoError(err)
			continue
		}

		if data[0] == 4 {
			err = ts.readerMoq.SendResponse(cfs, "UncorrectableErr", 456, uint32(data[0]))
			ts.NoError(err)
			continue
		}

		err = ts.readerMoq.SendResponse(cfs, "ok", 200, uint32(data[0]))
		ts.NoError(err)
	}
}

func (ts *TransportSuite) client() {
	ts.T().Log("client: try connect")
	dialer := delivery.DefaultRandomDialer(ts.cfg)
	transport, err := dialer.Dial(ts.ctx)
	ts.NoError(err)

	ts.T().Log("client: listen ack")
	var i int
	transport.OnAck(func(id uint32) {
		ts.EqualValues(ts.data[i], id)
		i++
		if i == 5 {
			close(ts.done)
		}
	})

	transport.OnReject(func(id uint32) {
		ts.EqualValues(ts.data[i], id)
		i++
		if i == 5 {
			close(ts.done)
		}
	})

	ts.T().Log("client: send segment")
	err = transport.SendSegment(ts.ctx, ts.data)
	ts.NoError(err)

	ts.T().Log("client: send restore")
	err = transport.SendRestore(
		ts.ctx,
		delivery.Snapshot(ts.data[1:]),
		[]delivery.Segment{ts.data[2:], ts.data[3:], ts.data[4:]},
	)
	ts.NoError(err)

	ts.T().Log("client: wait done")
	<-ts.done
	err = transport.Close()
	ts.NoError(err)
}

func (ts *TransportSuite) TestBackoff() {
	ts.T().Log("run test in parallel")
	ts.done = make(chan struct{})

	ts.T().Log("run server backoff")
	go ts.serverBackoff()

	ts.T().Log("run client backoff")
	go ts.clientBackoff()
	<-ts.done
}

func (ts *TransportSuite) serverBackoff() {
	var i int
	go func() {
		for {
			conn, err := ts.listener.Accept()
			if err != nil {
				return
			}
			ts.T().Log("server backoff: get income connection")
			i++

			err = conn.Close()
			ts.NoError(err)
			ts.T().Log("server backoff: connection drop")
		}
	}()

	<-ts.done
	ts.T().Log("server backoff: shutdown")
	ts.EqualValues(ts.cfg.MaxTries+1, i)
}

func (ts *TransportSuite) clientBackoff() {
	ts.T().Log("client backoff: try connect")
	dialer := delivery.DefaultRandomDialer(ts.cfg)
	_, err := dialer.Dial(ts.ctx)
	ts.Error(err)
	close(ts.done)
}

type RawMessageSuite struct {
	suite.Suite

	writer  *bytes.Buffer
	rm      *delivery.RawMessage
	payload []byte
}

func TestRawMessageSuite(t *testing.T) {
	suite.Run(t, new(RawMessageSuite))
}

func (rms *RawMessageSuite) SetupSuite() {
	rms.payload = []byte{1, 2, 3, 4, 5}
	rms.writer = &bytes.Buffer{}
}

func (rms *RawMessageSuite) SetupTest() {
	rms.rm = delivery.NewRawMessage(
		delivery.Reborn,
		delivery.MsgAuth,
		rms.payload,
	)
}

func (rms *RawMessageSuite) TearDownTest() {
	rms.writer.Reset()
}

func (rms *RawMessageSuite) TestRawMessageReborn() {
	rms.T().Log("write message")
	delivery.WriteRawMessage(rms.writer, rms.rm)
	rms.Equal(int(delivery.MaxHeaderSize)+int(rms.rm.Header.Size), rms.writer.Len())

	rms.T().Log("read message and compare with original")
	rmRead, err := delivery.ReadRawMessage(rms.writer)
	rms.NoError(err)

	rms.Equal(rms.rm.Header, rmRead.Header)
	rms.Equal(rms.payload, rmRead.Payload)
}

func (rms *RawMessageSuite) TestRawMessageRebornError() {
	rms.T().Log("write message")
	delivery.WriteRawMessage(rms.writer, rms.rm)
	rms.Equal(int(delivery.MaxHeaderSize)+int(rms.rm.Header.Size), rms.writer.Len())

	rms.T().Log("simulated data corruption with trancate 2 byte")
	rms.writer.Truncate(rms.writer.Len() - 2)

	rms.T().Log("check for error")
	_, err := delivery.ReadRawMessage(rms.writer)
	rms.Error(err)
}

func (rms *RawMessageSuite) TestRawMessageCompressedError() {
	rms.T().Log("write message")
	delivery.WriteRawMessage(rms.writer, rms.rm)
	rms.Equal(int(delivery.MaxHeaderSize)+int(rms.rm.Header.Size), rms.writer.Len())

	rms.T().Log("simulated compressed data corruption with replacement 2 byte")
	rms.writer.Truncate(rms.writer.Len() - (len(rms.payload) + 2))
	_, err := rms.writer.Write([]byte{1, 1})
	rms.NoError(err)
	_, err = rms.writer.Write(rms.payload)
	rms.NoError(err)

	rms.T().Log("check for error")
	_, err = delivery.ReadRawMessage(rms.writer)
	rms.ErrorContains(err, snappy.ErrCorrupt.Error())
}

type HeaderSuite struct {
	suite.Suite

	header delivery.Header
}

func TestHeaderSuite(t *testing.T) {
	suite.Run(t, new(HeaderSuite))
}

func (hs *HeaderSuite) SetupTest() {
	hs.header = delivery.Header{
		Version: 3,
		Type:    1,
		Size:    10,
	}
}

func (hs *HeaderSuite) TestHeaderString() {
	hs.Equal(
		`Header{version: 3, type: 1, size: 10}`,
		hs.header.String(),
	)
}

func (hs *HeaderSuite) TestHeaderValidate() {
	err := hs.header.Validate()
	hs.NoError(err)
}

func (hs *HeaderSuite) TestHeaderValidateErrMessageLarge() {
	hs.header.Size = 4294967295
	err := hs.header.Validate()
	hs.ErrorIs(
		err,
		delivery.ErrMessageLarge,
	)
}

func (hs *HeaderSuite) TestHeaderValidateErrMessageNull() {
	hs.header.Size = 0
	err := hs.header.Validate()
	hs.ErrorIs(
		err,
		delivery.ErrMessageNull,
	)
}

func (hs *HeaderSuite) TestHeaderValidateErrUnknownHeaderVersion() {
	hs.header.Version = 4
	err := hs.header.Validate()
	hs.ErrorIs(
		err,
		delivery.ErrUnknownHeaderVersion,
	)
}

func (hs *HeaderSuite) TestHeaderValidateErrUnknownHeader() {
	hs.header.Type = 5
	err := hs.header.Validate()
	hs.ErrorIs(
		err,
		delivery.ErrUnknownHeader,
	)
}
*/
