package delivery

// #cgo LDFLAGS: -L ../common/wal_c_bindings -l:aarch64_wal_c_api.a
// #cgo LDFLAGS: -lstdc++
// #include <stdlib.h>
// #include "../common/wal_c_encoder.h"
import "C" // nolint
import (
	"unsafe" // nolint
)

type cEncoder C.c_encoder

func cEncoderCtor(shardID, numberOfShards uint16) cEncoder {
	return cEncoder(C.okdb_wal_c_encoder_ctor(C.uint16_t(shardID), C.uint16_t(numberOfShards)))
}

func cEncoderEncode(encoder cEncoder, hashdex cHashdex, segment *GoSegment, redundant *GoRedundant) {
	C.okdb_wal_c_encoder_encode(
		C.c_encoder(encoder),
		C.c_hashdex(hashdex),
		(*C.c_slice_with_stream_buffer)(unsafe.Pointer(segment)),
		(*C.c_redundant)(unsafe.Pointer(redundant)),
	)
}

func cEncoderSnapshot(encoder cEncoder, redundants []unsafe.Pointer, snapshot *GoSnapshot) {
	C.okdb_wal_c_encoder_snapshot(
		C.c_encoder(encoder),
		*(*C.c_slice)(unsafe.Pointer(&redundants)),
		(*C.c_slice_with_stream_buffer)(unsafe.Pointer(snapshot)),
	)
}

func cEncoderDtor(encoder cEncoder) {
	C.okdb_wal_c_encoder_dtor(C.c_encoder(encoder))
}

type cHashdex C.c_hashdex

func cHashdexCtor() cHashdex {
	return cHashdex(C.okdb_wal_c_hashdex_ctor())
}

func cHashdexPresharding(hashdex cHashdex, protoData []byte) {
	C.okdb_wal_c_hashdex_presharding(
		C.c_hashdex(hashdex),
		*(*C.c_slice)(unsafe.Pointer(&protoData)),
	)
}

func cHashdexDtor(hashdex cHashdex) {
	C.okdb_wal_c_hashdex_dtor(C.c_hashdex(hashdex))
}

type cSlice C.c_slice

func cSliceWithStreamBufferDestroy(p unsafe.Pointer) {
	C.okdb_wal_c_slice_with_stream_buffer_destroy((*C.c_slice_with_stream_buffer)(p))
}

func cRedundantDestroy(p unsafe.Pointer) {
	C.okdb_wal_c_redundant_destroy((*C.c_redundant)(p))
}
