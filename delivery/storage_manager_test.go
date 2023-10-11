package delivery_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/frames/framestest"
)

// FileBuffer - implement file.
type FileBuffer struct {
	Buffer bytes.Buffer
	Index  int64
}

// NewFileBuffer - init FileBuffer.
func NewFileBuffer() *FileBuffer {
	return &FileBuffer{}
}

// Write - implement Bytes.
func (fb *FileBuffer) Bytes() []byte {
	return fb.Buffer.Bytes()
}

// Write - implement Reset.
func (fb *FileBuffer) Reset() {
	fb.Buffer.Reset()
	fb.Index = 0
}

// Read - implement Read.
func (fb *FileBuffer) Read(p []byte) (int, error) {
	n, err := bytes.NewBuffer(fb.Buffer.Bytes()[fb.Index:]).Read(p)

	if err == nil {
		if fb.Index+int64(len(p)) < int64(fb.Buffer.Len()) {
			fb.Index += int64(len(p))
		} else {
			fb.Index = int64(fb.Buffer.Len())
		}
	}

	return n, err
}

// ReadAt - implement Read.
func (fb *FileBuffer) ReadAt(p []byte, off int64) (int, error) {
	_, err := fb.Seek(off, 0)
	if err != nil {
		return 0, err
	}

	n, err := bytes.NewBuffer(fb.Buffer.Bytes()[fb.Index:]).Read(p)

	if err == nil {
		if fb.Index+int64(len(p)) < int64(fb.Buffer.Len()) {
			fb.Index += int64(len(p))
		} else {
			fb.Index = int64(fb.Buffer.Len())
		}
	}

	return n, err
}

// Write - implement Write.
func (fb *FileBuffer) Write(p []byte) (int, error) {
	n, err := fb.Buffer.Write(p)

	if err == nil {
		fb.Index = int64(fb.Buffer.Len())
	}

	return n, err
}

// Seek - implement Seek.
func (fb *FileBuffer) Seek(offset int64, whence int) (int64, error) {
	base := int64(fb.Buffer.Len())

	switch whence {
	case 0:
		if offset >= base || offset < 0 {
			return 0, errors.New("Seek: invalid offset")
		}
		fb.Index = offset
		return fb.Index, nil
	case 1:
		if fb.Index+offset > base || fb.Index+offset < 0 {
			return 0, errors.New("Seek: invalid offset")
		}

		fb.Index += offset
		return fb.Index, nil
	case 2:
		if offset > 0 || (base+offset) < 0 {
			return 0, errors.New("Seek: invalid offset")
		}
		fb.Index = (base + offset)
		return fb.Index, nil
	}

	return 0, errors.New("Seek: invalid whence")
}

type StorageManagerSuite struct {
	suite.Suite

	cfg                     delivery.FileStorageConfig
	etalonNewFileName       string
	etalonsNames            []string
	etalonShardsNumberPower uint8
	etalonBlockID           uuid.UUID
	etalonsData             *dataTest
	sm                      *delivery.StorageManager
	ctx                     context.Context
}

func TestStorageManagerSuite(t *testing.T) {
	suite.Run(t, new(StorageManagerSuite))
}

func (s *StorageManagerSuite) SetupSuite() {
	var err error
	s.ctx = context.Background()
	s.etalonNewFileName = "blablalblaUUID"

	dir, err := os.MkdirTemp("", filepath.Clean("refill-"))
	s.Require().NoError(err)

	s.cfg = delivery.FileStorageConfig{
		Dir:      dir,
		FileName: "current",
	}

	s.etalonsNames = []string{
		"www.collector.com",
		"www.collector-dev.com",
		"www.collector-prod.com",
		"www.collector-replica.com",
	}
	s.etalonShardsNumberPower = 1
	s.etalonBlockID, err = uuid.NewRandom()
	s.NoError(err)
	data := make([]byte, 42)
	for i := range data {
		data[i] = byte(i + 1)
	}
	s.etalonsData = newDataTest(data)
}

func (s *StorageManagerSuite) SetupTest() {
	var err error
	s.sm, err = delivery.NewStorageManager(
		s.cfg,
		s.etalonShardsNumberPower,
		s.etalonBlockID,
		nil,
		s.etalonsNames...,
	)
	s.NoError(err)
	s.Equal(s.etalonBlockID.String(), s.sm.BlockID().String())
}

func (s *StorageManagerSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.cfg.Dir))

	s.NoError(s.sm.Close())
}

func (s *StorageManagerSuite) TestStorageManagerInit() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)
}

func (s *StorageManagerSuite) TestSegment() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)

	segKey := common.SegmentKey{
		ShardID: 0,
		Segment: 0,
	}

	err = s.sm.WriteSegment(
		context.Background(),
		segKey,
		s.etalonsData,
	)
	s.NoError(err)

	ok, err = s.sm.FileExist()
	s.NoError(err)
	s.True(ok)

	actualSeg, err := s.sm.GetSegment(s.ctx, segKey)
	if s.NoError(err) {
		if buf, err := framestest.ReadPayload(actualSeg); s.NoError(err) {
			s.Equal(s.etalonsData.Bytes(), buf)
		}
	}
}

func (s *StorageManagerSuite) TestWriteSegmentWithError() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)

	err = s.sm.WriteSegment(
		context.Background(),
		common.SegmentKey{
			ShardID: 0,
			Segment: 2,
		},
		s.etalonsData,
	)
	s.ErrorIs(err, delivery.ErrSnapshotRequired)
}

func (s *StorageManagerSuite) TestSnapshot() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)

	segKey := common.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	err = s.sm.WriteSnapshot(
		context.Background(),
		segKey,
		s.etalonsData,
	)
	s.NoError(err)

	ok, err = s.sm.FileExist()
	s.NoError(err)
	s.True(ok)

	actualSnap, err := s.sm.GetSnapshot(s.ctx, segKey)
	if s.NoError(err) {
		if buf, err := framestest.ReadPayload(actualSnap); s.NoError(err) {
			s.Equal(s.etalonsData.Bytes(), buf)
		}
	}
}

func (s *StorageManagerSuite) TestAckStatus() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)

	err = s.sm.WriteAckStatus(context.Background())
	s.NoError(err)

	ok, err = s.sm.FileExist()
	s.NoError(err)
	s.False(ok)
}

func (s *StorageManagerSuite) TestRename() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)

	segKey := common.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	err = s.sm.WriteSnapshot(
		context.Background(),
		segKey,
		s.etalonsData,
	)
	s.NoError(err)

	ok, err = s.sm.FileExist()
	s.NoError(err)
	s.True(ok)

	s.NoError(s.sm.IntermediateRename(s.etalonNewFileName))

	_, err = os.Stat(filepath.Join(s.cfg.Dir, s.etalonNewFileName+".tmprefill"))
	s.NoError(err)

	s.NoError(s.sm.Close())

	s.NoError(s.sm.Rename(s.etalonNewFileName))

	_, err = os.Stat(filepath.Join(s.cfg.Dir, s.etalonNewFileName+".refill"))
	s.NoError(err)
}

func (s *StorageManagerSuite) TestRestore() {
	ok, err := s.sm.FileExist()
	s.NoError(err)
	s.False(ok)

	segKey := common.SegmentKey{
		ShardID: 0,
		Segment: 1,
	}

	err = s.sm.WriteAckStatus(context.Background())
	s.NoError(err)

	err = s.sm.WriteSnapshot(context.Background(), segKey, s.etalonsData)
	s.NoError(err)

	err = s.sm.WriteSegment(context.Background(), segKey, s.etalonsData)
	s.NoError(err)

	expectAckStatus := s.sm.GetAckStatus()
	expectAckStatus.Ack(common.SegmentKey{ShardID: 0, Segment: 0}, "www.collector.com")
	err = s.sm.WriteAckStatus(context.Background())
	s.NoError(err)

	ok, err = s.sm.FileExist()
	s.NoError(err)
	s.True(ok)

	err = s.sm.Close()
	s.NoError(err)

	newBlockID, err := uuid.NewRandom()
	s.NoError(err)
	var shardsNumberPower uint8 = 1

	s.sm, err = delivery.NewStorageManager(s.cfg, shardsNumberPower, newBlockID, nil, s.etalonsNames...)
	s.NoError(err)
	s.Equal(s.etalonBlockID.String(), s.sm.BlockID().String())

	actualSeg, err := s.sm.GetSegment(s.ctx, segKey)
	if s.NoError(err) {
		if buf, errRead := framestest.ReadPayload(actualSeg); s.NoError(errRead) {
			s.Equal(s.etalonsData.Bytes(), buf)
		}
	}

	actualSnap, err := s.sm.GetSnapshot(s.ctx, segKey)
	if s.NoError(err) {
		if buf, err := framestest.ReadPayload(actualSnap); s.NoError(err) {
			s.Equal(s.etalonsData.Bytes(), buf)
		}
	}

	actualAckStatus := s.sm.GetAckStatus()
	index, ok := actualAckStatus.Index("www.collector.com")
	s.True(ok)
	s.Equal(3, index)

	s.Equal(expectAckStatus.GetCopyAckStatuses(), actualAckStatus.GetCopyAckStatuses())
	s.Equal(s.etalonsNames, actualAckStatus.GetNames().ToString())
}

type AckStatusSuite struct {
	suite.Suite

	etalonsNames             []string
	etalonsShardsNumberPower uint8
	rw                       *FileBuffer
}

func TestAckStatusSuite(t *testing.T) {
	suite.Run(t, new(AckStatusSuite))
}

func (ass *AckStatusSuite) SetupSuite() {
	ass.etalonsNames = []string{
		"www.bcollector.com",
		"www.ucollector-dev.com",
		"www.fcollector-prod.com",
		"www.ncollector-replica.com",
	}
	ass.etalonsShardsNumberPower = 2

	ass.rw = NewFileBuffer()
}

func (ass *AckStatusSuite) TearDownTest() {
	ass.rw.Reset()
}

func (ass *AckStatusSuite) TestAckStatus() {
	ass.T().Log("init new AckStatus")
	as := delivery.NewAckStatus(ass.etalonsNames, ass.etalonsShardsNumberPower)

	ass.T().Log("check last ack")
	ass.EqualValues(math.MaxUint32, as.Last(0, ass.etalonsNames[0]))

	segKey := common.SegmentKey{ShardID: 0, Segment: 0}
	ass.T().Logf("ack key(%v) for name(%s)", segKey, ass.etalonsNames[0])
	as.Ack(segKey, ass.etalonsNames[0])
	ass.False(as.IsAck(segKey))

	ass.T().Logf("ack key(%v) for all names", segKey)
	for _, name := range ass.etalonsNames[1:] {
		as.Ack(segKey, name)
	}
	ass.True(as.IsAck(segKey))

	ass.T().Log("check last ack")
	ass.EqualValues(0, as.Last(0, ass.etalonsNames[0]))

	segKey.Segment++
	ass.T().Logf("ack key(%v) for name(%s)", segKey, ass.etalonsNames[0])
	as.Ack(segKey, ass.etalonsNames[0])
	ass.False(as.IsAck(segKey))

	ass.T().Log("check last ack")
	ass.EqualValues(1, as.Last(0, ass.etalonsNames[0]))

	ass.T().Log("check reject")
	segKey.Segment++
	as.Reject(segKey, ass.etalonsNames[0])
	rs := as.RotateRejects()
	ass.EqualValues(0, rs[0].NameID)
	ass.EqualValues(segKey.Segment, rs[0].Segment)
	ass.EqualValues(segKey.ShardID, rs[0].ShardID)

	ass.T().Log("check len Destinations")
	ass.Equal(len(ass.etalonsNames), as.Destinations())
	ass.Equal(1<<ass.etalonsShardsNumberPower, as.Shards())

	ass.T().Log("check Destination name index")
	index, ok := as.Index("www.ucollector-dev.com")
	ass.True(ok)
	ass.Equal(3, index)

	ass.T().Log("check unknown name index")
	index, ok = as.Index("com")
	ass.False(ok)
	ass.Equal(-1, index)
}

type RejectsSuite struct {
	suite.Suite

	etalonsData frames.RejectStatuses
	rw          *FileBuffer
	ctx         context.Context
}

func TestRejectsSuite(t *testing.T) {
	suite.Run(t, new(RejectsSuite))
}

func (rss *RejectsSuite) SetupSuite() {
	rss.ctx = context.Background()
	rss.etalonsData = frames.RejectStatuses{
		{NameID: 3, Segment: 3, ShardID: 2},
		{NameID: 3, Segment: 2, ShardID: 2},
		{NameID: 3, Segment: 1, ShardID: 2},

		{NameID: 3, Segment: 3, ShardID: 1},
		{NameID: 3, Segment: 2, ShardID: 1},
		{NameID: 3, Segment: 1, ShardID: 1},

		{NameID: 1, Segment: 3, ShardID: 2},
		{NameID: 1, Segment: 2, ShardID: 2},
		{NameID: 1, Segment: 1, ShardID: 2},

		{NameID: 1, Segment: 3, ShardID: 1},
		{NameID: 1, Segment: 2, ShardID: 1},
		{NameID: 1, Segment: 1, ShardID: 1},
	}

	rss.rw = NewFileBuffer()
}

func (rss *RejectsSuite) TearDownTest() {
	rss.rw.Reset()
}

func (rss *RejectsSuite) TestEncodeDecode() {
	rss.T().Log("init new rejects")
	wrs := delivery.NewRejects()

	rss.T().Log("add rejects")
	for _, rs := range rss.etalonsData {
		wrs.Add(rs.NameID, rs.Segment, rs.ShardID)
	}

	rss.T().Log("rotate slice")
	rjss := wrs.Rotate()

	rss.T().Log("encoding reject statuses and write")
	buf, err := rjss.MarshalBinary()
	rss.NoError(err)

	rss.T().Log("read and decoding reject statuses")
	var rrs frames.RejectStatuses
	err = rrs.UnmarshalBinary(buf)
	rss.NoError(err)

	rss.T().Log("sort etalon data and match with decoding data")
	rss.Equal(rss.etalonsData, rrs)
}
