package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/websocket"

	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/transport"
	"github.com/odarix/odarix-core-go/util"
)

// ShardMeta - shard metadata.
type ShardMeta struct {
	BlockID                uuid.UUID
	ShardID                uint16
	ShardsLog              uint8
	SegmentEncodingVersion uint8
	ContentLength          int64
}

// WithContentLength - return copy ShardMeta with ContentLength.
func (sm *ShardMeta) WithContentLength(cl int64) ShardMeta {
	newsm := *sm
	newsm.ContentLength = cl
	return newsm
}

// Dialer used for connect to backend
//
// We suppose that dialer has its own backoff and returns only permanent error.
type Dialer interface {
	// String - dialer name.
	String() string
	// Dial - create a connection and init stream.
	Dial(ctx context.Context, shardMeta ShardMeta) (Transport, error)
	// SendRefill - send refill via http.
	SendRefill(ctx context.Context, r io.Reader, shardMeta ShardMeta) error
}

// Transport is a destination connection interface
//
// We suppose that Transport is full-initiated:
// - authorized
// - setted timeouts
type Transport interface {
	Send(context.Context, frames.FrameWriter) error
	Listen(ctx context.Context)
	OnAck(func(uint32))
	OnReject(func(uint32))
	OnReadError(fn func(err error))
	Close() error
}

const (
	protocolVersion       uint8 = 3
	protocolVersionSocket uint8 = 4
	streamMethod                = "stream"
	refillMethod                = "refill"
	refillPath                  = "/refill"
)

// ConnDialer - underlying dialer interface.
type ConnDialer interface {
	// String - dialer name.
	String() string
	// Dial - main method that is overridden by wrappers.
	Dial(ctx context.Context) (net.Conn, error)
}

// DialerConfig - config for Dialer.
type DialerConfig struct {
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
	config     DialerConfig
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
func NewTCPDialer(
	dialer ConnDialer,
	config DialerConfig,
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) *TCPDialer {
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
func (dialer *TCPDialer) Dial(ctx context.Context, shardMeta ShardMeta) (Transport, error) {
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
			shardMeta.BlockID.String(),
			shardMeta.ShardID,
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

// SendRefill - send refill via http.
func (*TCPDialer) SendRefill(_ context.Context, _ io.Reader, _ ShardMeta) error {
	return errors.New("not implemented")
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
	nt              *transport.NetTransport
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
		nt:     transport.NewNetTransport(cfg, conn),
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
	token, agentUUID, productName, agentHostname, blockID string,
	shardID uint16,
) error {
	fe, err := frames.NewAuthFrameWithMsg(
		protocolVersionSocket,
		frames.NewAuthMsg(token, agentUUID, productName, agentHostname, blockID, shardID),
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

	if rfe.GetVersion() != protocolVersionSocket {
		return fmt.Errorf(
			"invalid response version %d, expected %d",
			rfe.GetVersion(),
			protocolVersionSocket,
		)
	}
	if rfe.GetType() != frames.ResponseType {
		return UnknownMsgType(frames.ResponseType, fe.GetType())
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
func (tt *TCPTransport) Send(ctx context.Context, frame frames.FrameWriter) error {
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
			tt.onReadErrorFunc(UnknownMsgType(frames.ResponseType, fe.GetType()))
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

// WebSocketDialer - dialer for connect with web socket to a host.
type WebSocketDialer struct {
	connDialer ConnDialer
	config     DialerConfig
	host       string
	backoff    backoff.BackOff
	clock      clockwork.Clock
	registerer prometheus.Registerer
}

var _ Dialer = (*WebSocketDialer)(nil)

// NewWebSocketDialer - init new WebSocketDialer.
func NewWebSocketDialer(
	dialer ConnDialer,
	hostName string, port string,
	config DialerConfig,
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) *WebSocketDialer {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Second
	ebo.RandomizationFactor = 0.5 //revive:disable-line:add-constant it's explained in field name
	ebo.Multiplier = 1.5          //revive:disable-line:add-constant it's explained in field name
	ebo.MaxElapsedTime = 0
	if config.BackoffMaxInterval > 0 {
		ebo.MaxInterval = config.BackoffMaxInterval
	}
	var buf strings.Builder
	_, _ = buf.WriteString(hostName)
	if port != "" {
		_ = buf.WriteByte(':')
		_, _ = buf.WriteString(port)
	}
	return &WebSocketDialer{
		connDialer: dialer,
		config:     config,
		host:       buf.String(),
		// reset backoff may be called concurrent with it use
		// so here we add mutex on this operations.
		// WithStartDuration with dur=0 start immediately.
		backoff:    BackoffWithLock(WithStartDuration(ebo, 0)),
		clock:      clock,
		registerer: registerer,
	}
}

// String - dialer name.
func (d *WebSocketDialer) String() string {
	return d.connDialer.String()
}

// Dial - create a connection and init stream.
func (d *WebSocketDialer) Dial(ctx context.Context, shardMeta ShardMeta) (Transport, error) {
	var bo backoff.BackOff = backoff.WithContext(d.backoff, ctx)
	if d.config.BackoffMaxTries > 0 {
		bo = backoff.WithMaxRetries(bo, d.config.BackoffMaxTries)
	}

	wsconfig := d.makeConfig(streamMethod, shardMeta)
	tr, err := PostRetryWithData(ctx, func() (*WebSocketTransport, error) {
		conn, errDial := d.connDialer.Dial(ctx)
		if errDial != nil {
			return nil, errDial
		}

		wsconn, errClient := websocket.NewClient(wsconfig, conn)
		if errClient != nil {
			_ = conn.Close()
			return nil, errClient
		}

		return NewWebSocketTransport(
			&d.config.Transport,
			wsconn,
			d.ResetBackoff,
			d.clock,
			d.registerer,
		), nil
	}, bo)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// SendRefill - send refill via http.
func (d *WebSocketDialer) SendRefill(ctx context.Context, r io.Reader, shardMeta ShardMeta) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://"+d.host+refillPath,
		r,
	)
	if err != nil {
		return err
	}

	client := http.Client{
		Transport: &http.Transport{
			DialTLSContext: func(dctx context.Context, _ string, _ string) (net.Conn, error) {
				return d.connDialer.Dial(dctx)
			},
		},
	}
	req.Header = d.makeHeader(refillMethod, shardMeta)
	req.ContentLength = shardMeta.ContentLength
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		return d.makeHTTPError(res)
	}

	return nil
}

// makeConfig - create *websocket.Config.
func (d *WebSocketDialer) makeConfig(method string, shardMeta ShardMeta) *websocket.Config {
	return &websocket.Config{
		Location: &url.URL{Scheme: "wss", Host: d.host},
		Origin:   &url.URL{Scheme: "https", Host: d.host},
		Version:  websocket.ProtocolVersionHybi13,
		Header:   d.makeHeader(method, shardMeta),
	}
}

// makeHeader - create http.Header.
func (d *WebSocketDialer) makeHeader(method string, shardMeta ShardMeta) http.Header {
	return http.Header{
		"Authorization": []string{"Bearer " + d.config.AuthToken},
		"User-Agent":    []string{d.config.ProductName},
		"Content-Type": []string{
			fmt.Sprintf(
				"application/opcore.%s;version=%d;segment_encoding_version=%d",
				method,
				protocolVersionSocket,
				shardMeta.SegmentEncodingVersion,
			),
		},
		"X-Agent-UUID":     []string{d.config.AgentUUID},
		"X-Agent-Hostname": []string{d.config.AgentHostname},
		"X-Block-ID":       []string{shardMeta.BlockID.String()},
		"X-Shard-ID":       []string{strconv.Itoa(int(shardMeta.ShardID))},
		"X-Shards-Log":     []string{strconv.Itoa(int(shardMeta.ShardsLog))},
	}
}

// makeHTTPError - make error from response.
func (*WebSocketDialer) makeHTTPError(res *http.Response) error {
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(res.Body)
	return &httpError{
		code: res.StatusCode,
		blob: buf.String(),
	}
}

// ResetBackoff resets next delay to zero
func (d *WebSocketDialer) ResetBackoff() {
	d.backoff.Reset()
}

// WebSocketTransport - transport implementation.
type WebSocketTransport struct {
	// dependencies
	clock clockwork.Clock
	// state
	wt              *transport.WebSocketTransport
	resetBackoff    func()
	onAckFunc       func(id uint32)
	onRejectFunc    func(id uint32)
	onReadErrorFunc func(err error)
	cancel          context.CancelFunc
	// metrics
	roundtripDuration prometheus.Histogram
}

// NewWebSocketTransport - init new WebSocketTransport.
func NewWebSocketTransport(
	cfg *transport.Config,
	wsconn *websocket.Conn,
	resetBackoff func(),
	clock clockwork.Clock,
	registerer prometheus.Registerer,
) *WebSocketTransport {
	factory := util.NewUnconflictRegisterer(registerer)
	return &WebSocketTransport{
		clock:        clock,
		wt:           transport.NewWebSocketTransport(cfg, wsconn),
		resetBackoff: resetBackoff,
		roundtripDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:        "odarix_core_delivery_wstransport_roundtrip_duration_seconds",
				Help:        "Roundtrip of duration(s).",
				Buckets:     prometheus.ExponentialBucketsRange(0.01, 15, 10),
				ConstLabels: prometheus.Labels{"host": wsconn.RemoteAddr().String()},
			},
		),
	}
}

// Send frame to server
func (tt *WebSocketTransport) Send(ctx context.Context, frame frames.FrameWriter) error {
	_, err := frame.WriteTo(tt.wt.Writer(ctx))
	return err
}

// Listen - start listening for an incoming connection.
// Will return an error if no callbacks are set.
func (tt *WebSocketTransport) Listen(ctx context.Context) {
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
func (tt *WebSocketTransport) incomeStream(ctx context.Context) {
	for {
		respmsg := frames.NewResponseV4Empty()
		if err := tt.wt.Read(ctx, respmsg); err != nil {
			// TODO use of closed network connection when you close connection from the outside (close)
			tt.onReadErrorFunc(err)
			return
		}
		tt.roundtripDuration.Observe(float64(time.Now().UnixNano()-respmsg.SentAt) / float64(time.Second))

		switch respmsg.Code {
		case http.StatusOK:
			tt.resetBackoff()
			tt.onAckFunc(respmsg.SegmentID)
		default:
			tt.onRejectFunc(respmsg.SegmentID)
		}
	}
}

// OnAck - read messages from the connection and acknowledges the send status via fn.
func (tt *WebSocketTransport) OnAck(fn func(id uint32)) {
	tt.onAckFunc = fn
}

// OnReject - read messages from connection and reject send status via fn.
func (tt *WebSocketTransport) OnReject(fn func(id uint32)) {
	tt.onRejectFunc = fn
}

// OnReadError - check error on income stream via fn.
func (tt *WebSocketTransport) OnReadError(fn func(err error)) {
	tt.onReadErrorFunc = fn
}

// Close - close connection.
func (tt *WebSocketTransport) Close() error {
	if tt.cancel != nil {
		tt.cancel()
	}

	return tt.wt.Close()
}

// ErrUnknownMsgType - msg type mismatch error.
type ErrUnknownMsgType struct {
	expected frames.TypeFrame
	received frames.TypeFrame
}

// UnknownMsgType - create ErrUnknownMsgType error.
func UnknownMsgType(exp, dec frames.TypeFrame) *ErrUnknownMsgType {
	return &ErrUnknownMsgType{exp, dec}
}

// Error - implements error.
func (err ErrUnknownMsgType) Error() string {
	return fmt.Sprintf("unknown msg type %d, expected %d", err.received, err.expected)
}

// Is - implements errors.Is interface.
func (*ErrUnknownMsgType) Is(target error) bool {
	_, ok := target.(*ErrUnknownMsgType)
	return ok
}

// httpError - wrapper for http error.
type httpError struct {
	code int
	blob string
}

// Error - implements error.
func (he *httpError) Error() string {
	return fmt.Sprintf("HTTP Error Code %d", he.code)
}

// Format - implements error.
func (he *httpError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = fmt.Fprintf(s, "%s\n\n%s", he.Error(), he.blob)
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, he.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", he.Error())
	}
}
