package cppbridge

import (
	"context"
	"hash/crc32"
	"io"
	"runtime"

	"github.com/gogo/protobuf/proto"
	"github.com/odarix/odarix-core-go/frames"
)

// ProtobufStats - stats data for decoded segment to RemoteWrite protobuf.
type ProtobufStats interface {
	// CreatedAt - return timestamp in ns when data was start writed to encoder.
	CreatedAt() int64
	// EncodedAt - return timestamp in ns when segment was encoded.
	EncodedAt() int64
	// Samples - return number of samples in segment.
	Samples() uint32
	// SegmentID - return processed segment id.
	SegmentID() uint32
	// Series - return number of series in segment.
	Series() uint32
}

// DecodedSegmentStats - stats data for decoded segment.
type DecodedSegmentStats struct {
	createdAt           int64
	encodedAt           int64
	samples             uint32
	series              uint32
	segmentID           uint32
	earliestBlockSample int64
	latestBlockSample   int64
}

var _ ProtobufStats = (*DecodedSegmentStats)(nil)

// CreatedAt - return timestamp in ns when data was start writed to encoder.
func (s DecodedSegmentStats) CreatedAt() int64 {
	return s.createdAt
}

// EncodedAt - return timestamp in ns when segment was encoded.
func (s DecodedSegmentStats) EncodedAt() int64 {
	return s.encodedAt
}

// Samples - return number of samples in segment.
func (s DecodedSegmentStats) Samples() uint32 {
	return s.samples
}

// SegmentID - return processed segment id.
func (s DecodedSegmentStats) SegmentID() uint32 {
	return s.segmentID
}

// Series - return number of series in segment.
func (s DecodedSegmentStats) Series() uint32 {
	return s.series
}

// EarliestBlockSample return earliest sample timestamp from block.
func (s DecodedSegmentStats) EarliestBlockSample() int64 {
	return s.earliestBlockSample
}

// LatestBlockSample return latest sample timestamp from block.
func (s DecodedSegmentStats) LatestBlockSample() int64 {
	return s.latestBlockSample
}

// ProtobufContent - decoded to RemoteWrite protobuf segment
type ProtobufContent interface {
	frames.WritePayload
	CreatedAt() int64
	EncodedAt() int64
	Samples() uint32
	SegmentID() uint32
	Series() uint32
	EarliestBlockSample() int64
	LatestBlockSample() int64
	UnmarshalTo(proto.Unmarshaler) error
}

// DecodedProtobuf - is GO wrapper for decoded RemoteWrite protobuf content.
type DecodedProtobuf struct {
	buf []byte
	DecodedSegmentStats
}

var _ ProtobufContent = (*DecodedProtobuf)(nil)

// NewDecodedProtobuf - init new DecodedProtobuf.
func NewDecodedProtobuf(b []byte, stats DecodedSegmentStats) *DecodedProtobuf {
	p := &DecodedProtobuf{
		buf:                 b,
		DecodedSegmentStats: stats,
	}
	runtime.SetFinalizer(p, func(p *DecodedProtobuf) {
		freeBytes(p.buf)
	})
	return p
}

// Size - returns len of bytes.
func (p *DecodedProtobuf) Size() int64 {
	return int64(len(p.buf))
}

func (p *DecodedProtobuf) CRC32() uint32 {
	return crc32.ChecksumIEEE(p.buf)
}

// WriteTo - implements io.WriterTo interface.
func (p *DecodedProtobuf) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(p.buf)
	runtime.KeepAlive(p)
	return int64(n), err
}

// UnmarshalTo - unmarshals data to given protobuf message.
func (p *DecodedProtobuf) UnmarshalTo(v proto.Unmarshaler) error {
	err := v.Unmarshal(p.buf)
	runtime.KeepAlive(p)
	return err
}

// HashdexContent decoded to WALBasicDecoderHashdex segment.
type HashdexContent interface {
	CreatedAt() int64
	EncodedAt() int64
	Samples() uint32
	SegmentID() uint32
	Series() uint32
	EarliestBlockSample() int64
	LatestBlockSample() int64
	ShardedData() ShardedData
}

// DecodedHashdex is GO wrapper for decoded hashdex content.
type DecodedHashdex struct {
	hashdex *WALBasicDecoderHashdex
	DecodedSegmentStats
}

var _ HashdexContent = (*DecodedHashdex)(nil)

// NewDecodedHashdex init new DecodedHashdex.
func NewDecodedHashdex(
	hashdex uintptr,
	meta *MetaInjection,
	cluster, replica string,
	stats DecodedSegmentStats,
) *DecodedHashdex {
	return &DecodedHashdex{
		hashdex:             NewWALBasicDecoderHashdex(hashdex, meta, cluster, replica),
		DecodedSegmentStats: stats,
	}
}

// ShardedData return hashdex as ShardedData.
func (dh *DecodedHashdex) ShardedData() ShardedData {
	return dh.hashdex
}

// WALDecoder - go wrapper for C-WALDecoder.
//
//	decoder - pointer to a C++ decoder initiated in C++ memory;
type WALDecoder struct {
	decoder uintptr
}

// NewWALDecoder - init new Decoder.
func NewWALDecoder(encodersVersion uint8) *WALDecoder {
	d := &WALDecoder{
		decoder: walDecoderCtor(encodersVersion),
	}
	runtime.SetFinalizer(d, func(d *WALDecoder) {
		walDecoderDtor(d.decoder)
	})
	return d
}

// Decode - decodes incoming encoding data and return protobuf.
func (d *WALDecoder) Decode(ctx context.Context, segment []byte) (ProtobufContent, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	stats, protobuf, exception := walDecoderDecode(d.decoder, segment)
	return NewDecodedProtobuf(protobuf, stats), handleException(exception)
}

// DecodeToHashdex decode incoming encoding data and return WALBasicDecoderHashdex.
func (d *WALDecoder) DecodeToHashdex(ctx context.Context, segment []byte) (HashdexContent, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	stats, hashdex, cluster, replica, exception := walDecoderDecodeToHashdex(d.decoder, segment)
	return NewDecodedHashdex(hashdex, nil, cluster, replica, stats), handleException(exception)
}

// DecodeToHashdexWithMetricInjection decode incoming encoding data and return WALBasicDecoderHashdex
// with metadata for injection metrics.
func (d *WALDecoder) DecodeToHashdexWithMetricInjection(
	ctx context.Context,
	segment []byte,
	meta *MetaInjection,
) (HashdexContent, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	stats, hashdex, cluster, replica, exception := walDecoderDecodeToHashdexWithMetricInjection(
		d.decoder,
		meta,
		segment,
	)
	return NewDecodedHashdex(hashdex, meta, cluster, replica, stats), handleException(exception)
}

// DecodeDry - decode incoming encoding data, restores decoder.
func (d *WALDecoder) DecodeDry(ctx context.Context, segment []byte) (uint32, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	segmentID, exception := walDecoderDecodeDry(d.decoder, segment)
	return segmentID, handleException(exception)
}

// RestoreFromStream - restore from incoming encoding data, restores decoder.
func (d *WALDecoder) RestoreFromStream(
	ctx context.Context,
	buf []byte,
	requiredSegmentID uint32,
) (offset uint64, restoredID uint32, err error) {
	if ctx.Err() != nil {
		return 0, 0, ctx.Err()
	}
	var exception []byte
	offset, restoredID, exception = walDecoderRestoreFromStream(d.decoder, buf, requiredSegmentID)
	return offset, restoredID, handleException(exception)
}
