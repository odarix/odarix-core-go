package delivery

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/odarix/odarix-core-go/transport"
)

// Dialer used for connect to backend
//
// We suppose that dialer has its own backoff and returns only permanent error.
type Dialer interface {
	String() string
	Dial(context.Context) (Transport, error)
}

// Transport is a destination connection interface
//
// We suppose that Transport is full-initiated:
// - authorized
// - setted timeouts
type Transport interface {
	SendRestore(context.Context, Snapshot, []Segment) error
	SendSegment(context.Context, Segment) error
	SendRefill(context.Context, []PreparedData) error
	SendSnapshot(context.Context, Snapshot) error
	SendDrySegment(context.Context, Segment) error
	OnAck(func(uint32))
	OnReject(func(uint32))
	OnReadError(fn func(err error))
	Close() error
}

const (
	protocolVersion uint8 = 3
)

// ConnDialer - underlying dialer interface.
type ConnDialer interface {
	// String - dialer name.
	String() string
	// Dial - main method that is overridden by wrappers.
	Dial(ctx context.Context) (net.Conn, error)
}

// TCPDialerConfig - config for RandomDialer.
type TCPDialerConfig struct {
	Transport          transport.Config
	AuthToken          string
	AgentUUID          string
	BackoffMaxInterval time.Duration
	BackoffMaxTries    uint64
}

// TCPDialer - dialer for connect to a host.
type TCPDialer struct {
	connDialer ConnDialer
	config     TCPDialerConfig
}

var _ Dialer = (*TCPDialer)(nil)

// NewTCPDialer - init new TCPDialer.
func NewTCPDialer(dialer ConnDialer, config TCPDialerConfig) *TCPDialer {
	return &TCPDialer{
		connDialer: dialer,
		config:     config,
	}
}

// String - dialer name.
func (dialer *TCPDialer) String() string {
	return dialer.connDialer.String()
}

// Dial - create a connection and init stream.
func (dialer *TCPDialer) Dial(ctx context.Context) (Transport, error) {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Second
	ebo.RandomizationFactor = 0.5 //revive:disable-line:add-constant it's explained in field name
	ebo.Multiplier = 1.5          //revive:disable-line:add-constant it's explained in field name
	ebo.MaxElapsedTime = 0
	if dialer.config.BackoffMaxInterval > 0 {
		ebo.MaxInterval = dialer.config.BackoffMaxInterval
	}
	var bo backoff.BackOff = backoff.WithContext(ebo, ctx)
	if dialer.config.BackoffMaxTries > 0 {
		bo = backoff.WithMaxRetries(bo, dialer.config.BackoffMaxTries)
	}

	tr, err := backoff.RetryWithData(func() (*TCPTransport, error) {
		conn, err := dialer.connDialer.Dial(ctx)
		if err != nil {
			return nil, err
		}
		tr := NewTCPTransport(&dialer.config.Transport, conn)
		if err := tr.auth(ctx, dialer.config.AuthToken, dialer.config.AgentUUID); err != nil {
			_ = tr.Close()
			return nil, err
		}
		return tr, nil
	}, bo)
	if err != nil {
		return nil, err
	}
	ctx, tr.cancel = context.WithCancel(ctx)
	go tr.incomeStream(ctx)
	return tr, nil
}

// TCPTransport - transport implementation.
type TCPTransport struct {
	nt              *transport.Transport
	onAckFunc       func(id uint32)
	onRejectFunc    func(id uint32)
	onReadErrorFunc func(err error)
	cancel          context.CancelFunc
}

// NewTCPTransport - init new TCPTransport.
func NewTCPTransport(cfg *transport.Config, conn net.Conn) *TCPTransport {
	return &TCPTransport{
		nt: transport.New(cfg, conn),
	}
}

// auth - request for authentication connection.
func (tt *TCPTransport) auth(ctx context.Context, token, uuid string) error {
	payload := (&transport.AuthMsg{
		Token:     token,
		AgentUUID: uuid,
	}).EncodeBinary()

	if err := tt.nt.Write(ctx, transport.NewRawMessage(protocolVersion, transport.MsgAuth, payload)); err != nil {
		return fmt.Errorf("send auth request %w", err)
	}

	raw, err := tt.nt.Read(ctx)
	if err != nil {
		return fmt.Errorf("receive auth response: %w", err)
	}

	if raw.Header.Version != protocolVersion {
		return fmt.Errorf(
			"invalid response version %d, expected %d",
			raw.Header.Version,
			protocolVersion,
		)
	}
	if raw.Header.Type != transport.MsgResponse {
		return fmt.Errorf(
			"unknown msg type %d, expected %d",
			raw.Header.Type,
			transport.MsgResponse,
		)
	}

	respm := &transport.ResponseMsg{}
	respm.DecodeBinary(raw.Payload)

	if respm.Code != http.StatusOK {
		return fmt.Errorf("auth failed: %d %s", respm.Code, respm.Text)
	}

	return nil
}

// SendRestore -  send Snapshot and Segments for restore over connection.
func (tt *TCPTransport) SendRestore(ctx context.Context, snap Snapshot, segs []Segment) error {
	if err := tt.nt.Write(
		ctx,
		transport.NewRawMessage(protocolVersion, transport.MsgSnapshot, snap.Bytes()),
	); err != nil {
		return fmt.Errorf("failed send snapshot: %w", err)
	}

	for _, seg := range segs {
		if err := tt.nt.Write(
			ctx,
			transport.NewRawMessage(protocolVersion, transport.MsgDryPut, seg.Bytes()),
		); err != nil {
			return fmt.Errorf("failed send segment: %w", err)
		}
	}

	return nil
}

// SendSegment - send Segment over connection.
func (tt *TCPTransport) SendSegment(ctx context.Context, seg Segment) error {
	if err := tt.nt.Write(ctx, transport.NewRawMessage(protocolVersion, transport.MsgPut, seg.Bytes())); err != nil {
		return fmt.Errorf("failed send segment: %w", err)
	}

	return nil
}

// SendRefill - send Refill msg over connection.
func (tt *TCPTransport) SendRefill(ctx context.Context, pd []PreparedData) error {
	messages := make([]transport.MessageData, 0, len(pd))
	for _, data := range pd {
		switch data.MsgType {
		case transport.MsgPut:
			messages = append(
				messages,
				transport.MessageData{
					ID:      data.SegmentID,
					Size:    data.Value.size,
					Typemsg: transport.MsgPut,
				},
			)
		case transport.MsgDryPut:
			messages = append(
				messages,
				transport.MessageData{
					ID:      data.SegmentID,
					Size:    data.Value.size,
					Typemsg: transport.MsgDryPut,
				},
			)
		case transport.MsgSnapshot:
			messages = append(
				messages,
				transport.MessageData{
					ID:      data.SegmentID,
					Size:    data.Value.size,
					Typemsg: transport.MsgSnapshot,
				},
			)
		}
	}
	mr := transport.RefillMsg{
		Messages: messages,
	}

	mb, err := mr.MarshalBinary()
	if err != nil {
		return err
	}

	if err := tt.nt.Write(
		ctx,
		transport.NewRawMessage(protocolVersion, transport.MsgRefill, mb),
	); err != nil {
		return fmt.Errorf("failed send refill: %w", err)
	}

	return nil
}

// SendSnapshot -  send Snapshot for restore over connection.
func (tt *TCPTransport) SendSnapshot(ctx context.Context, snap Snapshot) error {
	if err := tt.nt.Write(
		ctx,
		transport.NewRawMessage(protocolVersion, transport.MsgSnapshot, snap.Bytes()),
	); err != nil {
		return fmt.Errorf("failed send snapshot: %w", err)
	}

	return nil
}

// SendDrySegment - send dry Segment over connection.
func (tt *TCPTransport) SendDrySegment(ctx context.Context, seg Segment) error {
	if err := tt.nt.Write(ctx, transport.NewRawMessage(protocolVersion, transport.MsgDryPut, seg.Bytes())); err != nil {
		return fmt.Errorf("failed send dry segment: %w", err)
	}

	return nil
}

// incomeStream - listener for income message.
func (tt *TCPTransport) incomeStream(ctx context.Context) {
	for {
		raw, err := tt.nt.Read(ctx)
		if err != nil {
			// TODO use of closed network connection when you close connection from the outside (close)
			tt.onReadErrorFunc(err)
			return
		}

		if raw.Header.Type != transport.MsgResponse {
			tt.onReadErrorFunc(fmt.Errorf("unknown msg type %d, expected %d", raw.Header.Type, raw.Header.Type))
			return
		}

		respmsg := &transport.ResponseMsg{}
		respmsg.DecodeBinary(raw.Payload)

		switch respmsg.Code {
		case http.StatusOK:
			tt.onAckFunc(respmsg.SegmentID)
		default:
			tt.onRejectFunc(respmsg.SegmentID)
		}
	}
}

// OnAck - read messages from the connection and acknowledges the send status via fn.
func (tt *TCPTransport) OnAck(fn func(id uint32)) {
	tt.onAckFunc = fn
}

// OnReject - read messages from connection and reject send status via fn.
func (tt *TCPTransport) OnReject(fn func(id uint32)) {
	tt.onRejectFunc = fn
}

// OnReadError - check error on income stream via fn.
func (tt *TCPTransport) OnReadError(fn func(err error)) {
	tt.onReadErrorFunc = fn
}

// Close - close connection.
func (tt *TCPTransport) Close() error {
	if tt.cancel != nil {
		tt.cancel()
	}

	return tt.nt.Close()
}
