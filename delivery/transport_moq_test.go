package delivery_test

/*
import (
	"context"
	"fmt"
	"net"

	"github.com/odarix/odarix-core-go/delivery"
)

// Project - interface for project.
type Project interface {
}

// TCPReaderMoq - wrappers over connection from cient.
type TCPReaderMoq struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     *delivery.NetTransportConfig
	tt      *delivery.NetTransport
	decoder *Decoder
}

// NewTCPReaderMoq - init new TCPReaderMoq.
func NewTCPReaderMoq(ctx context.Context, cfg *delivery.NetTransportConfig, conn net.Conn) (*TCPReaderMoq, error) {
	ctx, cancel := context.WithCancel(ctx)

	return &TCPReaderMoq{
		ctx:     ctx,
		cancel:  cancel,
		cfg:     cfg,
		tt:      delivery.NewNetTransportWithConn(cfg, conn),
		decoder: NewDecoder(),
	}, nil
}

// authFn - external authorization function.
type authFn func(context.Context, CtxFields, string) (Project, CtxFields, error)

// Auth - incoming connection authorization.
func (*TCPReaderMoq) Auth(ctx context.Context, cfs CtxFields, payload []byte, fn authFn) (Project, CtxFields, error) {
	cfs = cfs.SetMethod("auth")

	am := &delivery.AuthMsg{}
	am.DecodeBinary(payload)
	err := am.Validate()
	if err != nil {
		return nil, cfs, err
	}
	cfs = cfs.SetAgentUUID(am.AgentUUID)

	return fn(ctx, cfs, am.Token)
}

// SendResponse - send response to client.
func (r *TCPReaderMoq) SendResponse(cfs CtxFields, text string, code, segmentID uint32) error {
	resp := &delivery.ResponseMsg{
		Text:      text,
		Code:      code,
		SegmentID: segmentID,
	}

	return r.tt.Write(
		r.ctx,
		delivery.NewRawMessage(
			delivery.VersionType(cfs.GetProtocolVersion()),
			delivery.MsgResponse,
			resp.EncodeBinary(),
		),
	)
}

// Handle - read connection, decoding message and return protobuf in byte.
func (r *TCPReaderMoq) Handle(_ CtxFields) ([]byte, error) {
	raw, err := r.tt.Read(r.ctx)
	if err != nil {
		return nil, err
	}

	switch raw.Header.Type {
	case delivery.MsgSnapshot:
		// restore from snapshot
		return r.decoder.Restore(r.ctx, raw.Payload)
		// return nil, nil
	case delivery.MsgPut:
		return r.decoder.Decode(r.ctx, raw.Payload)
	case delivery.MsgRefill:
		// read refill and convert
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown msg type %d", raw.Header.Type)
	}
}

// Close - close TCPReader.
func (r *TCPReaderMoq) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.ctx.Done()

	return r.tt.Close()
}

// Decoder -
type Decoder struct {
	// dencoder            C.Dencoder
}

// NewDecoder - init new Decoder.
func NewDecoder() *Decoder {
	return &Decoder{}
}

// Decode -
func (*Decoder) Decode(_ context.Context, encodeData []byte) ([]byte, error) {
	return encodeData, nil
}

// Restore -
func (*Decoder) Restore(_ context.Context, encodeData []byte) ([]byte, error) {
	return encodeData, nil
}
*/
