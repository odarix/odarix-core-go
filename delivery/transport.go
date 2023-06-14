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
	nt           *transport.Transport
	onAckFunc    func(id uint32)
	onRejectFunc func(id uint32)
	cancel       context.CancelFunc
	errChan      chan error
}

// NewTCPTransport - init new TCPTransport.
func NewTCPTransport(cfg *transport.Config, conn net.Conn) *TCPTransport {
	return &TCPTransport{
		nt:      transport.New(cfg, conn),
		errChan: make(chan error, 1),
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

// incomeStream - listener for income message.
func (tt *TCPTransport) incomeStream(ctx context.Context) {
	for {
		raw, err := tt.nt.Read(ctx)
		if err != nil {
			tt.errChan <- fmt.Errorf("income stream: %w", err)
			return
		}

		if raw.Header.Type != transport.MsgResponse {
			tt.errChan <- fmt.Errorf("unknown msg type %d, expected %d", raw.Header.Type, raw.Header.Type)
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

// WithReaderError - run function with gorutines error.
func (tt *TCPTransport) WithReaderError(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		tt.errChan <- fn(ctx)
	}()

	select {
	case err := <-tt.errChan:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Close - close connection.
func (tt *TCPTransport) Close() error {
	if tt.cancel != nil {
		tt.cancel()
	}

	return tt.nt.Close()
}
