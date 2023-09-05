package delivery_test

import (
	"io"

	"github.com/odarix/odarix-core-go/common"
)

// dataTest - test data.
type dataTest struct {
	data []byte
}

func newDataTest(data []byte) *dataTest {
	return &dataTest{
		data: data,
	}
}

// Size returns count of bytes in data
func (dt *dataTest) Size() int64 {
	return int64(len(dt.data))
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
func (*dataTest) Earliest() int64 {
	return 0
}

// Latest returns timestamp in ms of latest sample in segment
func (*dataTest) Latest() int64 {
	return 0
}

func (*dataTest) RemainingTableSize() uint32 {
	return 0
}

// Destroy - clear memory, for implements.
func (dt *dataTest) Destroy() {
	dt.data = nil
}

// dataTest - test data.
type shardedDataTest struct {
	data string
}

func newByteShardedDataTest(data []byte, _ common.HashdexLimits) (common.ShardedData, error) {
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
