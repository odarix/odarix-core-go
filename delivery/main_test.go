package delivery_test

import "github.com/odarix/odarix-core-go/common"

// dataTest - test data.
type dataTest struct {
	data []byte
}

func newDataTest(data []byte) *dataTest {
	return &dataTest{
		data: data,
	}
}

// Bytes - return data byte.
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

// Destroy - clear memory, for implements.
func (dt *dataTest) Destroy() {
	dt.data = nil
}

// dataTest - test data.
type shardedDataTest struct {
	data string
}

func newByteShardedDataTest(data []byte) (common.ShardedData, error) {
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
