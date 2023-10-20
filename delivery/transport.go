package delivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/transport"
	"github.com/odarix/odarix-core-go/util"
)

// Dialer used for connect to backend
//
// We suppose that dialer has its own backoff and returns only permanent error.
type Dialer interface {
	String() string
	Dial(context.Context, string, uint16) (Transport, error)
}

// Transport is a destination connection interface
//
// We suppose that Transport is full-initiated:
// - authorized
// - setted timeouts
type Transport interface {
	Send(context.Context, *frames.WriteFrame) error
	Listen(ctx context.Context)
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
	ProductName        string
	AgentHostname      string
	BackoffMaxInterval time.Duration
	BackoffMaxTries    uint64
}

// TCPDialer - dialer for connect to a host.
type TCPDialer struct {
	connDialer ConnDialer
	config     TCPDialerConfig
	backoff    backoff.BackOff
	clock      clockwork.Clock
	registerer prometheus.Registerer
}

type backoffWithLock struct {
	m  *sync.Mutex
	bo backoff.BackOff
}

// BackoffWithLock wraps backoff with mutex to concurrent free use
func BackoffWithLock(bo backoff.BackOff) backoff.BackOff {
	return backoffWithLock{
		m:  new(sync.Mutex),
		bo: bo,
	}
}

func (bwl backoffWithLock) NextBackOff() time.Duration {
	bwl.m.Lock()
	defer bwl.m.Unlock()
	return bwl.bo.NextBackOff()
}

func (bwl backoffWithLock) Reset() {
	bwl.m.Lock()
	defer bwl.m.Unlock()
	bwl.bo.Reset()
}

var _ Dialer = (*TCPDialer)(nil)

// NewTCPDialer - init new TCPDialer.
func NewTCPDialer(dialer ConnDialer, config TCPDialerConfig, clock clockwork.Clock, registerer prometheus.Registerer) *TCPDialer {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Second
	ebo.RandomizationFactor = 0.5 //revive:disable-line:add-constant it's explained in field name
	ebo.Multiplier = 1.5          //revive:disable-line:add-constant it's explained in field name
	ebo.MaxElapsedTime = 0
	if config.BackoffMaxInterval > 0 {
		ebo.MaxInterval = config.BackoffMaxInterval
	}
	return &TCPDialer{
		connDialer: dialer,
		config:     config,
		// reset backoff may be called concurrent with it use
		// so here we add mutex on this operations.
		// WithStartDuration with dur=0 start immediately.
		backoff:    BackoffWithLock(WithStartDuration(ebo, 0)),
		clock:      clock,
		registerer: registerer,
	}
}

// String - dialer name.
func (dialer *TCPDialer) String() string {
	return dialer.connDialer.String()
}

// Dial - create a connection and init stream.
func (dialer *TCPDialer) Dial(ctx context.Context, blockID string, shardID uint16) (Transport, error) {
	var bo backoff.BackOff = backoff.WithContext(dialer.backoff, ctx)
	if dialer.config.BackoffMaxTries > 0 {
		bo = backoff.WithMaxRetries(bo, dialer.config.BackoffMaxTries)
	}

	tr, err := PostRetryWithData(ctx, func() (*TCPTransport, error) {
		conn, err := dialer.connDialer.Dial(ctx)
		if err != nil {
			return nil, err
		}
		tr := NewTCPTransport(&dialer.config.Transport, conn, dialer, dialer.clock, dialer.registerer)
		if err := tr.auth(
			ctx,
			dialer.config.AuthToken,
			dialer.config.AgentUUID,
			dialer.config.ProductName,
			dialer.config.AgentHostname,
			blockID,
			shardID,
		); err != nil {
			_ = tr.Close()
			return nil, err
		}
		return tr, nil
	}, bo)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// ResetBackoff resets next delay to zero
func (dialer *TCPDialer) ResetBackoff() {
	dialer.backoff.Reset()
}

// ErrCallbackNotSet - error for callback not set.
var ErrCallbackNotSet = errors.New("callback not set")

// TCPTransport - transport implementation.
type TCPTransport struct {
	// dependencies
	clock clockwork.Clock
	// state
	nt              *transport.Transport
	dialer          interface{ ResetBackoff() }
	onAckFunc       func(id uint32)
	onRejectFunc    func(id uint32)
	onReadErrorFunc func(err error)
	cancel          context.CancelFunc
	// metrics
	roundtripDuration prometheus.Histogram
}

var _ Transport = &TCPTransport{}

// NewTCPTransport - init new TCPTransport.
func NewTCPTransport(
	cfg *transport.Config,
	conn net.Conn,
	dialer *TCPDialer,
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) *TCPTransport {
	factory := util.NewUnconflictRegisterer(registerer)
	return &TCPTransport{
		clock:  clock,
		nt:     transport.New(cfg, conn),
		dialer: dialer,
		roundtripDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:        "odarix_core_delivery_tcptransport_roundtrip_duration_seconds",
				Help:        "Roundtrip of duration(s).",
				Buckets:     prometheus.ExponentialBucketsRange(0.01, 15, 10),
				ConstLabels: prometheus.Labels{"host": conn.RemoteAddr().String()},
			},
		),
	}
}

// auth - request for authentication connection.
func (tt *TCPTransport) auth(
	ctx context.Context,
	token, uuid, productName, agentHostname, blockID string,
	shardID uint16,
) error {
	fe, err := frames.NewAuthFrameWithMsg(
		protocolVersion,
		frames.NewAuthMsg(token, uuid, productName, agentHostname, blockID, shardID),
	)
	if err != nil {
		return err
	}

	if err = tt.nt.Write(ctx, fe); err != nil {
		return fmt.Errorf("send auth request %w", err)
	}

	rfe, err := tt.nt.Read(ctx)
	if err != nil {
		return fmt.Errorf("receive auth response: %w", err)
	}

	if rfe.GetVersion() != protocolVersion {
		return fmt.Errorf(
			"invalid response version %d, expected %d",
			rfe.GetVersion(),
			protocolVersion,
		)
	}
	if rfe.GetType() != frames.ResponseType {
		return fmt.Errorf(
			"unknown msg type %d, expected %d",
			rfe.GetType(),
			frames.ResponseType,
		)
	}

	respm := frames.NewResponseMsgEmpty()
	if err = respm.UnmarshalBinary(rfe.GetBody()); err != nil {
		return err
	}

	if respm.Code != http.StatusOK {
		return fmt.Errorf("auth failed: %d %s", respm.Code, respm.Text)
	}

	return nil
}

// Send frame to server
func (tt *TCPTransport) Send(ctx context.Context, frame *frames.WriteFrame) error {
	_, err := frame.WriteTo(tt.nt.Writer(ctx))
	return err
}

// Listen - start listening for an incoming connection.
// Will return an error if no callbacks are set.
func (tt *TCPTransport) Listen(ctx context.Context) {
	if tt.onAckFunc == nil {
		panic("callback not set: onAckFunc")
	}

	if tt.onRejectFunc == nil {
		panic("callback not set: onRejectFunc")
	}

	if tt.onReadErrorFunc == nil {
		panic("callback not set: onReadErrorFunc")
	}

	ctx, tt.cancel = context.WithCancel(ctx)
	go tt.incomeStream(ctx)
}

// incomeStream - listener for income message.
func (tt *TCPTransport) incomeStream(ctx context.Context) {
	for {
		fe, err := tt.nt.Read(ctx)
		if err != nil {
			// TODO use of closed network connection when you close connection from the outside (close)
			tt.onReadErrorFunc(err)
			return
		}

		if fe.GetType() != frames.ResponseType {
			tt.onReadErrorFunc(fmt.Errorf("unknown msg type %d, expected %d", fe.GetType(), frames.ResponseType))
			return
		}

		respmsg := frames.NewResponseMsgEmpty()
		if err := respmsg.UnmarshalBinary(fe.GetBody()); err != nil {
			tt.onReadErrorFunc(err)
			return
		}
		tt.roundtripDuration.Observe(float64(time.Now().UnixNano()-respmsg.SendAt) / float64(time.Second))

		switch respmsg.Code {
		case http.StatusOK:
			tt.dialer.ResetBackoff()
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

// PostRetryWithData - is like Retry but returns data in the response too. Only ExponentialBackOff.
func PostRetryWithData[T any](ctx context.Context, op backoff.OperationWithData[T], b backoff.BackOff) (T, error) {
	var (
		err  error
		next time.Duration
		res  T
	)
	t := &defaultTimer{}
	defer t.Stop()

	for {
		if next = b.NextBackOff(); next == backoff.Stop {
			if cerr := ctx.Err(); cerr != nil {
				return res, cerr
			}

			return res, err
		}

		t.Start(next)

		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-t.C():
		}

		res, err = op()
		if err == nil {
			return res, nil
		}

		var permanent *backoff.PermanentError
		if errors.As(err, &permanent) {
			return res, permanent.Err
		}
	}
}

// startWithBackOff - start backoff with timeout.
type startWithBackOff struct {
	delegate  backoff.BackOff
	firstNext *time.Duration
	first     time.Duration
}

// WithStartDuration - creates a wrapper around another BackOff. Return backoff which next with first timeout.
func WithStartDuration(b backoff.BackOff, first time.Duration) backoff.BackOff {
	return &startWithBackOff{delegate: b, firstNext: &first, first: first}
}

// NextBackOff - returns the duration to wait before retrying the operation, or backoff.
// Stop to indicate that no more retries should be made.
func (b *startWithBackOff) NextBackOff() time.Duration {
	if b.firstNext != nil {
		next := *b.firstNext
		b.firstNext = nil
		return next
	}
	return b.delegate.NextBackOff()
}

// Reset - reset to initial state.
func (b *startWithBackOff) Reset() {
	b.firstNext = &b.first
	b.delegate.Reset()
}

// defaultTimer implements Timer interface using time.Timer
type defaultTimer struct {
	timer *time.Timer
}

// C returns the timers channel which receives the current time when the timer fires.
func (t *defaultTimer) C() <-chan time.Time {
	return t.timer.C
}

// Start starts the timer to fire after the given duration
func (t *defaultTimer) Start(duration time.Duration) {
	if t.timer == nil {
		t.timer = time.NewTimer(duration)
	} else {
		t.timer.Reset(duration)
	}
}

// Stop is called when the timer is not used anymore and resources may be freed.
func (t *defaultTimer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}
