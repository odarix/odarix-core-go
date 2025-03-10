package head

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/odarix/odarix-core-go/cppbridge"
)

type WriteSyncCloser interface {
	io.WriteCloser
	Sync() error
}

type WriteFlusher interface {
	io.Writer
	Flush() error
}

type bufferedShardWalWriter struct {
	writeSyncCloser WriteSyncCloser
	bufferedWriter  *bufio.Writer
}

func newBufferedShardWalWriter(writeSyncCloser WriteSyncCloser) *bufferedShardWalWriter {
	return &bufferedShardWalWriter{
		writeSyncCloser: writeSyncCloser,
		bufferedWriter:  bufio.NewWriterSize(writeSyncCloser, 1024*1024),
	}
}

func (w *bufferedShardWalWriter) Write(p []byte) (n int, err error) {
	return w.bufferedWriter.Write(p)
}

func (w *bufferedShardWalWriter) Sync() error {
	if err := w.bufferedWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if err := w.writeSyncCloser.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

func (w *bufferedShardWalWriter) Close() error {
	return errors.Join(w.bufferedWriter.Flush(), w.writeSyncCloser.Sync(), w.writeSyncCloser.Close())
}

type ShardWal struct {
	corrupted           bool
	encoder             *cppbridge.HeadWalEncoder
	writeSyncCloser     WriteSyncCloser
	fileHeaderIsWritten bool
	buf                 [binary.MaxVarintLen32]byte
	maxSegmentSize      uint32
	uncommited          bool
}

func newShardWal(encoder *cppbridge.HeadWalEncoder, fileHeaderIsWritten bool, maxSegmentSize uint32, writeSyncCloser WriteSyncCloser) *ShardWal {
	return &ShardWal{
		encoder:             encoder,
		writeSyncCloser:     writeSyncCloser,
		fileHeaderIsWritten: fileHeaderIsWritten,
		maxSegmentSize:      maxSegmentSize,
	}
}

func newCorruptedShardWal() *ShardWal {
	return &ShardWal{
		corrupted: true,
	}
}

func (w *ShardWal) WriteHeader() error {
	if w.fileHeaderIsWritten {
		return nil
	}

	_, err := WriteHeader(w.writeSyncCloser, 1, w.encoder.Version())
	if err != nil {
		return fmt.Errorf("failed to write file header: %w", err)
	}

	if err = w.writeSyncCloser.Sync(); err != nil {
		return fmt.Errorf("failed to sync file header: %w", err)
	}

	w.fileHeaderIsWritten = true
	return nil
}

func (w *ShardWal) Write(innerSeriesSlice []*cppbridge.InnerSeries) (bool, error) {
	if w.corrupted {
		return false, fmt.Errorf("writing in corrupted wal")
	}

	stats, err := w.encoder.Encode(innerSeriesSlice)
	if err != nil {
		return false, fmt.Errorf("failed to encode inner series: %w", err)
	}

	w.uncommited = true

	if w.maxSegmentSize > 0 && stats.Samples() >= w.maxSegmentSize {
		return true, nil
	}

	return false, nil
}

func (w *ShardWal) Uncommitted() bool {
	return w.uncommited
}

func (w *ShardWal) Commit() error {
	if w.corrupted {
		return fmt.Errorf("committing corrupted wal")
	}

	if !w.uncommited {
		return nil
	}

	segment, err := w.encoder.Finalize()
	if err != nil {
		return fmt.Errorf("failed to finalize segment: %w", err)
	}

	_, err = WriteSegment(w.writeSyncCloser, segment)
	if err != nil {
		return fmt.Errorf("failed to write segment: %w", err)
	}

	if err = w.writeSyncCloser.Sync(); err != nil {
		return fmt.Errorf("failed to flush segment: %w", err)
	}

	w.uncommited = false

	return nil
}

func (w *ShardWal) Close() error {
	if w.writeSyncCloser != nil {
		return w.writeSyncCloser.Close()
	}

	return nil
}

type CorruptedShardWal struct {
	writeCloser io.WriteCloser
}

func WriteHeader(writer io.Writer, fileFormatVersion uint8, encoderVersion uint8) (n int, err error) {
	var buf [binary.MaxVarintLen32]byte
	var size int
	var bytesWritten int

	size = binary.PutUvarint(buf[:], uint64(fileFormatVersion))
	bytesWritten, err = writer.Write(buf[:size])
	if err != nil {
		return n, fmt.Errorf("failed to write file format version: %w", err)
	}
	n += bytesWritten

	size = binary.PutUvarint(buf[:], uint64(encoderVersion))
	bytesWritten, err = writer.Write(buf[:size])
	if err != nil {
		return n, fmt.Errorf("failed to write encoder version: %w", err)
	}
	n += bytesWritten

	return n, nil
}

type byteReader struct {
	r io.Reader
	n int
}

func (r *byteReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	n, err := io.ReadFull(r.r, b)
	if err != nil {
		return 0, err
	}
	r.n += n
	return b[0], nil
}

func ReadHeader(reader io.Reader) (fileFormatVersion uint8, encoderVersion uint8, n int, err error) {
	br := &byteReader{r: reader}
	fileFormatVersionU64, err := binary.ReadUvarint(br)
	if err != nil {
		return 0, 0, n, fmt.Errorf("failed to read file format version: %w", err)
	}
	fileFormatVersion = uint8(fileFormatVersionU64)
	n = br.n

	encoderVersionU64, err := binary.ReadUvarint(br)
	if err != nil {
		return 0, 0, n, fmt.Errorf("failed to read encoder version: %w", err)
	}
	encoderVersion = uint8(encoderVersionU64)
	n = br.n

	return fileFormatVersion, encoderVersion, n, nil
}

type EncodedSegment interface {
	Size() int64
	CRC32() uint32
	io.WriterTo
	cppbridge.SegmentStats
}

func WriteSegment(writer io.Writer, segment EncodedSegment) (n int, err error) {
	var buf [binary.MaxVarintLen32]byte
	var size int
	var bytesWritten int

	segmentSize := uint64(segment.Size())
	size = binary.PutUvarint(buf[:], segmentSize)
	bytesWritten, err = writer.Write(buf[:size])
	if err != nil {
		return n, fmt.Errorf("failed to write segment size: %w", err)
	}
	n += bytesWritten

	size = binary.PutUvarint(buf[:], uint64(segment.CRC32()))
	bytesWritten, err = writer.Write(buf[:size])
	if err != nil {
		return n, fmt.Errorf("failed to write segment crc32 hash: %w", err)
	}
	n += bytesWritten

	size = binary.PutUvarint(buf[:], uint64(segment.Samples()))
	bytesWritten, err = writer.Write(buf[:size])
	if err != nil {
		return n, fmt.Errorf("failed to write segment sample count: %w", err)
	}
	n += bytesWritten

	var bytesWritten64 int64
	bytesWritten64, err = segment.WriteTo(writer)
	if err != nil {
		return n, fmt.Errorf("failed to write segment data: %w", err)
	}
	n += int(bytesWritten64)

	return n, nil
}

type DecodedSegment struct {
	data        []byte
	sampleCount uint32
}

func (d DecodedSegment) Data() []byte {
	return d.data
}

func (d DecodedSegment) SampleCount() uint32 {
	return d.sampleCount
}

func ReadSegment(reader io.Reader) (decodedSegment DecodedSegment, n int, err error) {
	br := &byteReader{r: reader}
	var size uint64
	size, err = binary.ReadUvarint(br)
	if err != nil {
		return decodedSegment, br.n, fmt.Errorf("failed to read segment size: %w", err)
	}

	crc32HashU64, err := binary.ReadUvarint(br)
	if err != nil {
		return decodedSegment, br.n, fmt.Errorf("failed to read segment crc32 hash: %w", err)
	}
	crc32Hash := uint32(crc32HashU64)

	sampleCountU64, err := binary.ReadUvarint(br)
	if err != nil {
		return decodedSegment, br.n, fmt.Errorf("failed to read segment sample count: %w", err)
	}
	decodedSegment.sampleCount = uint32(sampleCountU64)

	decodedSegment.data = make([]byte, size)
	n, err = io.ReadFull(reader, decodedSegment.data)
	if err != nil {
		return decodedSegment, br.n, fmt.Errorf("failed to read segment data: %w", err)
	}
	n += br.n

	if crc32Hash != crc32.ChecksumIEEE(decodedSegment.data) {
		return decodedSegment, n, fmt.Errorf("crc32 did not match, want: %d, have: %d", crc32Hash, crc32.ChecksumIEEE(decodedSegment.data))
	}

	return decodedSegment, n, nil
}

func TryReadSegment(source io.ReadSeeker) (decodedSegment DecodedSegment, err error) {
	br := &byteReader{r: source}
	var size uint64
	size, err = binary.ReadUvarint(br)
	if err != nil {
		return decodedSegment, errors.Join(fmt.Errorf("failed to read segment size: %w", err), io.ErrUnexpectedEOF)
	}

	crc32HashU64, err := binary.ReadUvarint(br)
	if err != nil {
		return decodedSegment, errors.Join(fmt.Errorf("failed to read segment crc32 hash: %w", err), io.ErrUnexpectedEOF)
	}
	crc32Hash := uint32(crc32HashU64)

	sampleCountU64, err := binary.ReadUvarint(br)
	if err != nil {
		return decodedSegment, errors.Join(fmt.Errorf("failed to read segment sample count: %w", err), io.ErrUnexpectedEOF)
	}
	decodedSegment.sampleCount = uint32(sampleCountU64)

	decodedSegment.data = make([]byte, size)
	bytesRead, err := io.ReadFull(source, decodedSegment.data)
	if err != nil {
		return decodedSegment, fmt.Errorf("failed to read segment data: %w", err)
	}
	offset := bytesRead + br.n

	if uint64(bytesRead) != size {
		if _, err = source.Seek(-int64(offset), io.SeekCurrent); err != nil {
			return decodedSegment, fmt.Errorf("try read segment set offset failed: %w", err)
		}
		return decodedSegment, io.ErrUnexpectedEOF
	}

	if crc32Hash != crc32.ChecksumIEEE(decodedSegment.data) {
		return decodedSegment, fmt.Errorf("crc32 did not match, want: %d, have: %d", crc32Hash, crc32.ChecksumIEEE(decodedSegment.data))
	}

	return decodedSegment, nil
}
