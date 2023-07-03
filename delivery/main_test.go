package delivery_test

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

func (dt *dataTest) Samples() uint32 {
	return 0
}

// Series returns count of series in segment
func (dt *dataTest) Series() uint32 {
	return 0
}

// Earliest returns timestamp in ms of earliest sample in segment
func (dt *dataTest) Earliest() int64 {
	return 0
}

// Latest returns timestamp in ms of latest sample in segment
func (dt *dataTest) Latest() int64 {
	return 0
}

// Destroy - clear memory, for implements.
func (dt *dataTest) Destroy() {
	dt.data = nil
}
