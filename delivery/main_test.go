package delivery_test

import (
	"hash/crc32"
	"io"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/model"
)

const refillExt = ".refill"

// dataTest - test data.
type dataTest struct {
	data []byte
}

func newDataTest(data []byte) *dataTest {
	return &dataTest{
		data: data,
	}
}

// Add - sum incoming stats.
func (*dataTest) Add(_ cppbridge.SegmentStats) {
}

// AllocatedMemory - .
func (*dataTest) AllocatedMemory() uint64 {
	return 0
}

// Size returns count of bytes in data
func (dt *dataTest) Size() int64 {
	return int64(len(dt.data))
}

func (dt *dataTest) CRC32() uint32 {
	return crc32.ChecksumIEEE(dt.data)
}

// WriteTo implements io.WriterTo interface
func (dt *dataTest) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(dt.data)
	return int64(n), err
}

func (dt *dataTest) Bytes() []byte {
	return dt.data
}

func (*dataTest) Samples() uint32 {
	return 0
}

// Series returns count of series in segment
func (*dataTest) Series() uint32 {
	return 0
}

// Earliest returns timestamp in ms of earliest sample in segment
func (*dataTest) EarliestTimestamp() int64 {
	return 0
}

// Latest returns timestamp in ms of latest sample in segment
func (*dataTest) LatestTimestamp() int64 {
	return 0
}

func (*dataTest) RemainingTableSize() uint32 {
	return 0
}

// Destroy - clear memory, for implements.
func (dt *dataTest) Destroy() {
	dt.data = nil
}

type testHashdexFactory struct{}

func (testHashdexFactory) SnappyProtobuf(data []byte, _ cppbridge.WALHashdexLimits) (cppbridge.ShardedData, error) {
	return newShardedDataTest(string(data)), nil
}

func (testHashdexFactory) GoModel(data []model.TimeSeries, _ cppbridge.WALHashdexLimits) (cppbridge.ShardedData, error) {
	return noOpShardedData{}, nil
}

// dataTest - test data.
type shardedDataTest struct {
	data string
}

func (dt *shardedDataTest) RangeMetadata(f func(metadata cppbridge.WALScraperHashdexMetadata) bool) {
	//TODO implement me
	panic("implement me")
}

func newByteShardedDataTest(data []byte, _ cppbridge.WALHashdexLimits) (cppbridge.ShardedData, error) {
	return newShardedDataTest(string(data)), nil
}

func newShardedDataTest(data string) *shardedDataTest {
	return &shardedDataTest{
		data: data,
	}
}

// Bytes - return bytes, for implements.
func (dt *shardedDataTest) Bytes() []byte {
	return []byte(dt.data)
}

func (*shardedDataTest) Type() uint8 {
	return 0
}

// Cluster -  return cluster name, for implements.
func (*shardedDataTest) Cluster() string {
	return ""
}

// Replica - return replica name, for implements.
func (*shardedDataTest) Replica() string {
	return ""
}

// Destroy - clear memory, for implements.
func (dt *shardedDataTest) Destroy() {
	dt.data = ""
}

type noOpShardedData struct{}

func (d noOpShardedData) RangeMetadata(f func(metadata cppbridge.WALScraperHashdexMetadata) bool) {
	//TODO implement me
	panic("implement me")
}

func (noOpShardedData) Type() uint8 {
	return 1
}

func (noOpShardedData) Cluster() string {
	return ""
}

func (noOpShardedData) Replica() string {
	return ""
}
