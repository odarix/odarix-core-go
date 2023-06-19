package delivery

import (
	"fmt"
	"math"
	"runtime"
	"unsafe" // nolint

	"context"
)

// Encoder -
type Encoder struct {
	encoder            cEncoder
	lastEncodedSegment uint32
	shardID            uint16
}

// NewEncoder - init new Encoder.
func NewEncoder(shardID, numberOfShards uint16) *Encoder {
	return &Encoder{
		encoder:            cEncoderCtor(shardID, numberOfShards),
		shardID:            shardID,
		lastEncodedSegment: math.MaxUint32,
	}
}

// Encode - encode income data(ShardedData) through C++ encoder to Segment.
//
//revive:disable-next-line:function-result-limit all results are essential
func (e *Encoder) Encode(ctx context.Context, shardedData ShardedData) (SegmentKey, Segment, Redundant, error) {
	hashdex, ok := shardedData.(*Hashdex)
	if !ok {
		return SegmentKey{}, nil, nil, fmt.Errorf("shardedData not casting to Hashdex")
	}

	if ctx.Err() != nil {
		return SegmentKey{}, nil, nil, ctx.Err()
	}

	// init memory in GO
	csegment := NewGoSegment()
	credundant := NewGoRedundant()

	// transfer go-slice in C/C++
	// e.encoder - C-Encoder
	// shardedData.hashdex - Hashdex, struct(init from GO), filling in C/C++
	// *(*C.c_slice_with_stream_buffer)(unsafe.Pointer(csegment)) - C-Segment struct(init from GO)
	// (*C.c_redundant)(unsafe.Pointer(credundant)) - C-Redundant struct(init from GO)
	cEncoderEncode(e.encoder, hashdex.hashdex, csegment, credundant)
	e.lastEncodedSegment++
	segKey := SegmentKey{
		ShardID: e.shardID,
		Segment: e.lastEncodedSegment,
	}

	return segKey, csegment, credundant, nil
}

// Snapshot - get Snapshot from C-Encoder.
func (e *Encoder) Snapshot(ctx context.Context, rds []Redundant) (Snapshot, error) {
	gosnapshot := NewGoSnapshot()
	crs := make([]unsafe.Pointer, len(rds))
	for i := range rds {
		crs[i] = rds[i].(*GoRedundant).PointerData()
	}

	if ctx.Err() != nil {
		return gosnapshot, ctx.Err()
	}

	cEncoderSnapshot(e.encoder, crs, gosnapshot)

	return gosnapshot, nil
}

// LastEncodedSegment - get last encoded segment ID.
func (e *Encoder) LastEncodedSegment() uint32 {
	return e.lastEncodedSegment
}

// Destroy - distructor for C-Encoder.
func (e *Encoder) Destroy() {
	cEncoderDtor(e.encoder)
	e.encoder = nil
}

// GoSnapshot - GO wrapper for snapshot, init from GO and filling from C/C++.
// data - slice struct for cast in C/C++. Contained C/C++ memory
// buf - unsafe.Pointer for stringstream in C/C++, need for clear memory.
type GoSnapshot struct {
	data cSlice
	buf  unsafe.Pointer
}

// NewGoSnapshot - init GoSnapshot.
func NewGoSnapshot() *GoSnapshot {
	return &GoSnapshot{
		data: cSlice{},
	}
}

// Bytes - convert in go-slice byte from struct.
func (gs *GoSnapshot) Bytes() []byte {
	return *(*[]byte)((unsafe.Pointer(&gs.data))) //nolint:gosec // this is memory optimisation
}

// Destroy - clear memory in C/C++.
func (gs *GoSnapshot) Destroy() {
	cSliceWithStreamBufferDestroy(unsafe.Pointer(gs)) //nolint:gosec // this is memory optimisation
}

// GoSegment - GO wrapper for Segment, init from GO and filling from C/C++.
// data - slice struct for cast in C/C++. Contained C/C++ memory
// buf - unsafe.Pointer for stringstream in C/C++, need for clear memory.
type GoSegment struct {
	data cSlice
	buf  unsafe.Pointer
}

// NewGoSegment - init GoSegment.
func NewGoSegment() *GoSegment {
	return &GoSegment{
		data: cSlice{},
	}
}

// Bytes - convert in go-slice byte from struct.
func (gs *GoSegment) Bytes() []byte {
	return *(*[]byte)((unsafe.Pointer(&gs.data))) //nolint:gosec // this is memory optimisation
}

// Destroy - clear memory in C/C++.
func (gs *GoSegment) Destroy() {
	cSliceWithStreamBufferDestroy(unsafe.Pointer(gs)) //nolint:gosec // this is memory optimisation
}

// GoRedundant - GO wrapper for Redundant, init from GO and filling from C/C++.
// data - for cast in C/C++. Contained C/C++ memory
type GoRedundant struct {
	data unsafe.Pointer
}

// NewGoRedundant - init GoRedundant.
func NewGoRedundant() *GoRedundant {
	return &GoRedundant{}
}

// PointerData - get contained data, ONLY FORTEST.
func (gr *GoRedundant) PointerData() unsafe.Pointer {
	return gr.data
}

// Destroy - clear memory in C/C++.
func (gr *GoRedundant) Destroy() {
	cRedundantDestroy(unsafe.Pointer(gr)) //nolint:gosec // this is memory optimisation
}

// Hashdex - Presharding data, GO wrapper for Hashdex, init from GO and filling from C/C++.
type Hashdex struct {
	hashdex cHashdex
	data    []byte
}

// NewHashdex - init new Hashdex.
func NewHashdex(protoData []byte) *Hashdex {
	h := &Hashdex{
		hashdex: cHashdexCtor(),
		data:    protoData,
	}
	cHashdexPresharding(h.hashdex, h.data)

	return h
}

// Destroy - clear memory in C/C++.
func (h *Hashdex) Destroy() {
	// so that the Garbage Collector does not clear the memory
	// associated with the slice, we hold it through KeepAlive
	runtime.KeepAlive(h.data)
	cHashdexDtor(h.hashdex)
}
