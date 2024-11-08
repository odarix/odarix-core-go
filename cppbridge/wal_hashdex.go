package cppbridge

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unsafe"

	"github.com/odarix/odarix-core-go/model"
)

// ShardedData - array of structures with (*LabelSet, timestamp, value, LSHash)
type ShardedData interface {
	Cluster() string
	Replica() string
}

// WALProtobufHashdex - Presharding data, GO wrapper for WALProtobufHashdex, init from GO and filling from C/C++.
type WALProtobufHashdex struct {
	hashdex uintptr
	data    []byte
	cluster string
	replica string
}

// WALHashdexLimits - memory limits for Hashdex.
type WALHashdexLimits struct {
	MaxLabelNameLength         uint32 `validate:"required"`
	MaxLabelValueLength        uint32 `validate:"required"`
	MaxLabelNamesPerTimeseries uint32 `validate:"required"`
	MaxTimeseriesCount         uint64
}

const (
	defaultMaxLabelNameLength         = 4096
	defaultMaxLabelValueLength        = 65536
	defaultMaxLabelNamesPerTimeseries = 320
)

// DefaultWALHashdexLimits - Default memory limits for Hashdex.
func DefaultWALHashdexLimits() WALHashdexLimits {
	return WALHashdexLimits{
		MaxLabelNameLength:         defaultMaxLabelNameLength,
		MaxLabelValueLength:        defaultMaxLabelValueLength,
		MaxLabelNamesPerTimeseries: defaultMaxLabelNamesPerTimeseries,
		MaxTimeseriesCount:         0,
	}
}

// MarshalBinary - encoding to byte.
func (l *WALHashdexLimits) MarshalBinary() ([]byte, error) {
	//revive:disable-next-line:add-constant sum 2+3+2+4
	buf := make([]byte, 0, 11)

	buf = binary.AppendUvarint(buf, uint64(l.MaxLabelNameLength))
	buf = binary.AppendUvarint(buf, uint64(l.MaxLabelValueLength))
	buf = binary.AppendUvarint(buf, uint64(l.MaxLabelNamesPerTimeseries))
	buf = binary.AppendUvarint(buf, l.MaxTimeseriesCount)
	return buf, nil
}

// UnmarshalBinary - decoding from byte.
func (l *WALHashdexLimits) UnmarshalBinary(data []byte) error {
	var offset int

	maxLabelNameLength, n := binary.Uvarint(data[offset:])
	l.MaxLabelNameLength = uint32(maxLabelNameLength)
	offset += n

	maxLabelValueLength, n := binary.Uvarint(data[offset:])
	l.MaxLabelValueLength = uint32(maxLabelValueLength)
	offset += n

	maxLabelNamesPerTimeseries, n := binary.Uvarint(data[offset:])
	l.MaxLabelNamesPerTimeseries = uint32(maxLabelNamesPerTimeseries)
	offset += n

	maxTimeseriesCount, _ := binary.Uvarint(data[offset:])
	l.MaxTimeseriesCount = maxTimeseriesCount

	return nil
}

// NewWALProtobufHashdex - init new WALProtobufHashdex with limits.
func NewWALProtobufHashdex(protoData []byte, limits WALHashdexLimits) (ShardedData, error) {
	// cluster and replica - in memory GO(protoData)
	h := &WALProtobufHashdex{
		hashdex: walProtobufHashdexCtor(limits),
		data:    protoData,
	}
	runtime.SetFinalizer(h, func(h *WALProtobufHashdex) {
		runtime.KeepAlive(h.data)
		walProtobufHashdexDtor(h.hashdex)
	})
	var exception []byte
	h.cluster, h.replica, exception = walProtobufHashdexPresharding(h.hashdex, protoData)
	return h, handleException(exception)
}

// cptr - pointer to underlying c++ object.
func (h *WALProtobufHashdex) cptr() uintptr {
	return h.hashdex
}

// Cluster - get Cluster name.
func (h *WALProtobufHashdex) Cluster() string {
	return strings.Clone(h.cluster)
}

// Replica - get Replica name.
func (h *WALProtobufHashdex) Replica() string {
	return strings.Clone(h.replica)
}

// WALGoModelHashdex - Go wrapper for OkDB::WAL::GoModelHashdex..
type WALGoModelHashdex struct {
	hashdex uintptr
	data    []model.TimeSeries
	cluster string
	replica string
}

// cptr - pointer to underlying c++ object.
func (h *WALGoModelHashdex) cptr() uintptr {
	return h.hashdex
}

// Cluster - get Cluster name.
func (h *WALGoModelHashdex) Cluster() string {
	return strings.Clone(h.cluster)
}

// Replica - get Replica name.
func (h *WALGoModelHashdex) Replica() string {
	return strings.Clone(h.replica)
}

// NewWALGoModelHashdex - init new GoModelHashdex with limits.
func NewWALGoModelHashdex(limits WALHashdexLimits, data []model.TimeSeries) (ShardedData, error) {
	h := &WALGoModelHashdex{
		hashdex: walGoModelHashdexCtor(limits),
		data:    data,
	}
	runtime.SetFinalizer(h, func(h *WALGoModelHashdex) {
		runtime.KeepAlive(h.data)
		walGoModelHashdexDtor(h.hashdex)
	})
	var exception []byte
	h.cluster, h.replica, exception = walGoModelHashdexPresharding(h.hashdex, data)
	return h, handleException(exception)
}

// HashdexFactory - hashdex factory.
type HashdexFactory struct{}

// Protobuf - constructs Prometheus Remote Write based hashdex.
func (HashdexFactory) Protobuf(data []byte, limits WALHashdexLimits) (ShardedData, error) {
	return NewWALProtobufHashdex(data, limits)
}

// GoModel - constructs model.TimeSeries based hashdex.
func (HashdexFactory) GoModel(data []model.TimeSeries, limits WALHashdexLimits) (ShardedData, error) {
	return NewWALGoModelHashdex(limits, data)
}

// MetaInjection metedata for injection metrics.
type MetaInjection struct {
	Now       int64
	SentAt    int64
	AgentUUID string
	Hostname  string
}

// WALBasicDecoderHashdex Go wrapper for OkDB::WAL::WALBasicDecoderHashdex.
type WALBasicDecoderHashdex struct {
	hashdex  uintptr
	metadata *MetaInjection
	cluster  string
	replica  string
}

// NewWALBasicDecoderHashdex init new WALBasicDecoderHashdex with c-pointer OkDB::WAL::WALBasicDecoderHashdex.
func NewWALBasicDecoderHashdex(hashdex uintptr, meta *MetaInjection, cluster, replica string) *WALBasicDecoderHashdex {
	h := &WALBasicDecoderHashdex{
		hashdex:  hashdex,
		metadata: meta,
		cluster:  cluster,
		replica:  replica,
	}
	runtime.SetFinalizer(h, func(h *WALBasicDecoderHashdex) {
		runtime.KeepAlive(h.metadata)
		if h.hashdex == 0 {
			return
		}
		walBasicDecoderHashdexDtor(h.hashdex)
	})
	return h
}

// Cluster get Cluster name.
func (h *WALBasicDecoderHashdex) Cluster() string {
	return strings.Clone(h.cluster)
}

// Replica get Replica name.
func (h *WALBasicDecoderHashdex) Replica() string {
	return strings.Clone(h.replica)
}

// cptr pointer to underlying c++ object.
func (h *WALBasicDecoderHashdex) cptr() uintptr {
	return h.hashdex
}

const (
	// Error codes from parsing.
	scraperParseNoError uint32 = iota
	scraperParseUnexpectedToken
	scraperParseNoMetricName
	scraperInvalidUtf8
	scraperParseInvalidValue
	scraperParseInvalidTimestamp
)

var (
	// ErrScraperParseUnexpectedToken error when parse unexpected token.
	ErrScraperParseUnexpectedToken = errors.New("scraper parse unexpected token")
	// ErrScraperParseNoMetricName error when parse no metric name.
	ErrScraperParseNoMetricName = errors.New("scraper parse no metric name")
	// ErrScraperInvalidUtf8 error when parse invalid utf8.
	ErrScraperInvalidUtf8 = errors.New("scraper parse invalid utf8")
	// ErrScraperParseInvalidValue error when parse invalid value.
	ErrScraperParseInvalidValue = errors.New("scraper parse invalid value")
	// ErrScraperParseInvalidTimestamp error when parse invalid timestamp.
	ErrScraperParseInvalidTimestamp = errors.New("scraper parse invalid timestamp")

	codeToError = map[uint32]error{
		scraperParseNoError:          nil,
		scraperParseUnexpectedToken:  ErrScraperParseUnexpectedToken,
		scraperParseNoMetricName:     ErrScraperParseNoMetricName,
		scraperInvalidUtf8:           ErrScraperInvalidUtf8,
		scraperParseInvalidValue:     ErrScraperParseInvalidValue,
		scraperParseInvalidTimestamp: ErrScraperParseInvalidTimestamp,
	}
)

func errorFromCode(code uint32) error {
	if code == scraperParseNoError {
		return nil
	}

	if err, ok := codeToError[code]; ok {
		return err
	}

	return fmt.Errorf("scraper parse unknown code error: %d", code)
}

// WALScraperHashdex hashdex for sraped incoming data.
type WALScraperHashdex struct {
	hashdex uintptr
	buffer  []byte
}

const (
	// ScraperMetadataHelp type of metadata "Help" from hashdex metadata.
	ScraperMetadataHelp uint32 = iota
	// ScraperMetadataType type of metadata "Type" from hashdex metadata.
	ScraperMetadataType
)

// WALScraperHashdexMetadata metadata from hashdex.
type WALScraperHashdexMetadata struct {
	MetricName string
	Text       string
	Type       uint32
}

var _ ShardedData = (*WALScraperHashdex)(nil)

// NewScraperHashdex init new *WALScraperHashdex.
func NewScraperHashdex() *WALScraperHashdex {
	h := &WALScraperHashdex{
		hashdex: walScraperHashdexCtor(),
		buffer:  nil,
	}
	runtime.SetFinalizer(h, func(h *WALScraperHashdex) {
		walScraperHashdexDtor(h.hashdex)
	})
	return h
}

// Parse parsing incoming slice byte with default timestamp to hashdex.
func (h *WALScraperHashdex) Parse(buffer []byte, default_timestamp int64) error {
	h.buffer = buffer
	return errorFromCode(walScraperHashdexParse(h.hashdex, h.buffer, default_timestamp))
}

// RangeMetadata calls f sequentially for each metadata present in the hashdex.
// If f returns false, range stops the iteration.
func (h *WALScraperHashdex) RangeMetadata(f func(metadata WALScraperHashdexMetadata) bool) {
	mds := walScraperHashdexGetMetadata(h.hashdex)

	for i := range mds {
		if !f(mds[i]) {
			break
		}
	}

	freeBytes(*(*[]byte)(unsafe.Pointer(&mds)))
}

// Cluster get Cluster name.
func (*WALScraperHashdex) Cluster() string {
	return ""
}

// Replica get Replica name.
func (*WALScraperHashdex) Replica() string {
	return ""
}

// cptr pointer to underlying c++ object.
func (h *WALScraperHashdex) cptr() uintptr {
	return h.hashdex
}
