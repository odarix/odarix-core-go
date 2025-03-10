// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package relabeler_test

import (
	"github.com/odarix/odarix-core-go/cppbridge"
	"io"
	"sync"
)

// Ensure, that SegmentMock does implement cppbridge.Segment.
// If this is not the case, regenerate this file with moq.
var _ cppbridge.Segment = &SegmentMock{}

// SegmentMock is a mock implementation of cppbridge.Segment.
//
//	func TestSomethingThatUsesSegment(t *testing.T) {
//
//		// make and configure a mocked cppbridge.Segment
//		mockedSegment := &SegmentMock{
//			AllocatedMemoryFunc: func() uint64 {
//				panic("mock out the AllocatedMemory method")
//			},
//			CRC32Func: func() uint32 {
//				panic("mock out the CRC32 method")
//			},
//			EarliestTimestampFunc: func() int64 {
//				panic("mock out the EarliestTimestamp method")
//			},
//			LatestTimestampFunc: func() int64 {
//				panic("mock out the LatestTimestamp method")
//			},
//			RemainingTableSizeFunc: func() uint32 {
//				panic("mock out the RemainingTableSize method")
//			},
//			SamplesFunc: func() uint32 {
//				panic("mock out the Samples method")
//			},
//			SeriesFunc: func() uint32 {
//				panic("mock out the Series method")
//			},
//			SizeFunc: func() int64 {
//				panic("mock out the Size method")
//			},
//			WriteToFunc: func(w io.Writer) (int64, error) {
//				panic("mock out the WriteTo method")
//			},
//		}
//
//		// use mockedSegment in code that requires cppbridge.Segment
//		// and then make assertions.
//
//	}
type SegmentMock struct {
	// AllocatedMemoryFunc mocks the AllocatedMemory method.
	AllocatedMemoryFunc func() uint64

	// CRC32Func mocks the CRC32 method.
	CRC32Func func() uint32

	// EarliestTimestampFunc mocks the EarliestTimestamp method.
	EarliestTimestampFunc func() int64

	// LatestTimestampFunc mocks the LatestTimestamp method.
	LatestTimestampFunc func() int64

	// RemainingTableSizeFunc mocks the RemainingTableSize method.
	RemainingTableSizeFunc func() uint32

	// SamplesFunc mocks the Samples method.
	SamplesFunc func() uint32

	// SeriesFunc mocks the Series method.
	SeriesFunc func() uint32

	// SizeFunc mocks the Size method.
	SizeFunc func() int64

	// WriteToFunc mocks the WriteTo method.
	WriteToFunc func(w io.Writer) (int64, error)

	// calls tracks calls to the methods.
	calls struct {
		// AllocatedMemory holds details about calls to the AllocatedMemory method.
		AllocatedMemory []struct {
		}
		// CRC32 holds details about calls to the CRC32 method.
		CRC32 []struct {
		}
		// EarliestTimestamp holds details about calls to the EarliestTimestamp method.
		EarliestTimestamp []struct {
		}
		// LatestTimestamp holds details about calls to the LatestTimestamp method.
		LatestTimestamp []struct {
		}
		// RemainingTableSize holds details about calls to the RemainingTableSize method.
		RemainingTableSize []struct {
		}
		// Samples holds details about calls to the Samples method.
		Samples []struct {
		}
		// Series holds details about calls to the Series method.
		Series []struct {
		}
		// Size holds details about calls to the Size method.
		Size []struct {
		}
		// WriteTo holds details about calls to the WriteTo method.
		WriteTo []struct {
			// W is the w argument value.
			W io.Writer
		}
	}
	lockAllocatedMemory    sync.RWMutex
	lockCRC32              sync.RWMutex
	lockEarliestTimestamp  sync.RWMutex
	lockLatestTimestamp    sync.RWMutex
	lockRemainingTableSize sync.RWMutex
	lockSamples            sync.RWMutex
	lockSeries             sync.RWMutex
	lockSize               sync.RWMutex
	lockWriteTo            sync.RWMutex
}

// AllocatedMemory calls AllocatedMemoryFunc.
func (mock *SegmentMock) AllocatedMemory() uint64 {
	if mock.AllocatedMemoryFunc == nil {
		panic("SegmentMock.AllocatedMemoryFunc: method is nil but Segment.AllocatedMemory was just called")
	}
	callInfo := struct {
	}{}
	mock.lockAllocatedMemory.Lock()
	mock.calls.AllocatedMemory = append(mock.calls.AllocatedMemory, callInfo)
	mock.lockAllocatedMemory.Unlock()
	return mock.AllocatedMemoryFunc()
}

// AllocatedMemoryCalls gets all the calls that were made to AllocatedMemory.
// Check the length with:
//
//	len(mockedSegment.AllocatedMemoryCalls())
func (mock *SegmentMock) AllocatedMemoryCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockAllocatedMemory.RLock()
	calls = mock.calls.AllocatedMemory
	mock.lockAllocatedMemory.RUnlock()
	return calls
}

// CRC32 calls CRC32Func.
func (mock *SegmentMock) CRC32() uint32 {
	if mock.CRC32Func == nil {
		panic("SegmentMock.CRC32Func: method is nil but Segment.CRC32 was just called")
	}
	callInfo := struct {
	}{}
	mock.lockCRC32.Lock()
	mock.calls.CRC32 = append(mock.calls.CRC32, callInfo)
	mock.lockCRC32.Unlock()
	return mock.CRC32Func()
}

// CRC32Calls gets all the calls that were made to CRC32.
// Check the length with:
//
//	len(mockedSegment.CRC32Calls())
func (mock *SegmentMock) CRC32Calls() []struct {
} {
	var calls []struct {
	}
	mock.lockCRC32.RLock()
	calls = mock.calls.CRC32
	mock.lockCRC32.RUnlock()
	return calls
}

// EarliestTimestamp calls EarliestTimestampFunc.
func (mock *SegmentMock) EarliestTimestamp() int64 {
	if mock.EarliestTimestampFunc == nil {
		panic("SegmentMock.EarliestTimestampFunc: method is nil but Segment.EarliestTimestamp was just called")
	}
	callInfo := struct {
	}{}
	mock.lockEarliestTimestamp.Lock()
	mock.calls.EarliestTimestamp = append(mock.calls.EarliestTimestamp, callInfo)
	mock.lockEarliestTimestamp.Unlock()
	return mock.EarliestTimestampFunc()
}

// EarliestTimestampCalls gets all the calls that were made to EarliestTimestamp.
// Check the length with:
//
//	len(mockedSegment.EarliestTimestampCalls())
func (mock *SegmentMock) EarliestTimestampCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockEarliestTimestamp.RLock()
	calls = mock.calls.EarliestTimestamp
	mock.lockEarliestTimestamp.RUnlock()
	return calls
}

// LatestTimestamp calls LatestTimestampFunc.
func (mock *SegmentMock) LatestTimestamp() int64 {
	if mock.LatestTimestampFunc == nil {
		panic("SegmentMock.LatestTimestampFunc: method is nil but Segment.LatestTimestamp was just called")
	}
	callInfo := struct {
	}{}
	mock.lockLatestTimestamp.Lock()
	mock.calls.LatestTimestamp = append(mock.calls.LatestTimestamp, callInfo)
	mock.lockLatestTimestamp.Unlock()
	return mock.LatestTimestampFunc()
}

// LatestTimestampCalls gets all the calls that were made to LatestTimestamp.
// Check the length with:
//
//	len(mockedSegment.LatestTimestampCalls())
func (mock *SegmentMock) LatestTimestampCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockLatestTimestamp.RLock()
	calls = mock.calls.LatestTimestamp
	mock.lockLatestTimestamp.RUnlock()
	return calls
}

// RemainingTableSize calls RemainingTableSizeFunc.
func (mock *SegmentMock) RemainingTableSize() uint32 {
	if mock.RemainingTableSizeFunc == nil {
		panic("SegmentMock.RemainingTableSizeFunc: method is nil but Segment.RemainingTableSize was just called")
	}
	callInfo := struct {
	}{}
	mock.lockRemainingTableSize.Lock()
	mock.calls.RemainingTableSize = append(mock.calls.RemainingTableSize, callInfo)
	mock.lockRemainingTableSize.Unlock()
	return mock.RemainingTableSizeFunc()
}

// RemainingTableSizeCalls gets all the calls that were made to RemainingTableSize.
// Check the length with:
//
//	len(mockedSegment.RemainingTableSizeCalls())
func (mock *SegmentMock) RemainingTableSizeCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockRemainingTableSize.RLock()
	calls = mock.calls.RemainingTableSize
	mock.lockRemainingTableSize.RUnlock()
	return calls
}

// Samples calls SamplesFunc.
func (mock *SegmentMock) Samples() uint32 {
	if mock.SamplesFunc == nil {
		panic("SegmentMock.SamplesFunc: method is nil but Segment.Samples was just called")
	}
	callInfo := struct {
	}{}
	mock.lockSamples.Lock()
	mock.calls.Samples = append(mock.calls.Samples, callInfo)
	mock.lockSamples.Unlock()
	return mock.SamplesFunc()
}

// SamplesCalls gets all the calls that were made to Samples.
// Check the length with:
//
//	len(mockedSegment.SamplesCalls())
func (mock *SegmentMock) SamplesCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockSamples.RLock()
	calls = mock.calls.Samples
	mock.lockSamples.RUnlock()
	return calls
}

// Series calls SeriesFunc.
func (mock *SegmentMock) Series() uint32 {
	if mock.SeriesFunc == nil {
		panic("SegmentMock.SeriesFunc: method is nil but Segment.Series was just called")
	}
	callInfo := struct {
	}{}
	mock.lockSeries.Lock()
	mock.calls.Series = append(mock.calls.Series, callInfo)
	mock.lockSeries.Unlock()
	return mock.SeriesFunc()
}

// SeriesCalls gets all the calls that were made to Series.
// Check the length with:
//
//	len(mockedSegment.SeriesCalls())
func (mock *SegmentMock) SeriesCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockSeries.RLock()
	calls = mock.calls.Series
	mock.lockSeries.RUnlock()
	return calls
}

// Size calls SizeFunc.
func (mock *SegmentMock) Size() int64 {
	if mock.SizeFunc == nil {
		panic("SegmentMock.SizeFunc: method is nil but Segment.Size was just called")
	}
	callInfo := struct {
	}{}
	mock.lockSize.Lock()
	mock.calls.Size = append(mock.calls.Size, callInfo)
	mock.lockSize.Unlock()
	return mock.SizeFunc()
}

// SizeCalls gets all the calls that were made to Size.
// Check the length with:
//
//	len(mockedSegment.SizeCalls())
func (mock *SegmentMock) SizeCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockSize.RLock()
	calls = mock.calls.Size
	mock.lockSize.RUnlock()
	return calls
}

// WriteTo calls WriteToFunc.
func (mock *SegmentMock) WriteTo(w io.Writer) (int64, error) {
	if mock.WriteToFunc == nil {
		panic("SegmentMock.WriteToFunc: method is nil but Segment.WriteTo was just called")
	}
	callInfo := struct {
		W io.Writer
	}{
		W: w,
	}
	mock.lockWriteTo.Lock()
	mock.calls.WriteTo = append(mock.calls.WriteTo, callInfo)
	mock.lockWriteTo.Unlock()
	return mock.WriteToFunc(w)
}

// WriteToCalls gets all the calls that were made to WriteTo.
// Check the length with:
//
//	len(mockedSegment.WriteToCalls())
func (mock *SegmentMock) WriteToCalls() []struct {
	W io.Writer
} {
	var calls []struct {
		W io.Writer
	}
	mock.lockWriteTo.RLock()
	calls = mock.calls.WriteTo
	mock.lockWriteTo.RUnlock()
	return calls
}
