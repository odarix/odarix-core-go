package delivery_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/common"
	"github.com/odarix/odarix-core-go/delivery"
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

type HeaderFrameSuite struct {
	suite.Suite

	rw         *FileBuffer
	etalonSize uint32
	ctx        context.Context
}

func TestHeaderFrameSuite(t *testing.T) {
	suite.Run(t, new(HeaderFrameSuite))
}

func (hs *HeaderFrameSuite) SetupSuite() {
	hs.rw = NewFileBuffer()
	hs.etalonSize = 42
	hs.ctx = context.Background()
}

func (hs *HeaderFrameSuite) TearDownTest() {
	hs.rw.Reset()
}

func (hs *HeaderFrameSuite) TestHeaderWriteRead() {
	wh := delivery.NewHeaderFrame(delivery.DestinationNamesType, 0, hs.etalonSize, 0)

	hs.Equal(hs.etalonSize, wh.GetSize())

	b := wh.EncodeBinary()
	n, err := hs.rw.Write(b)
	hs.NoError(err)
	hs.Equal(n, wh.SizeOf())

	rh, err := delivery.ReadHeader(hs.ctx, hs.rw, 0)
	hs.NoError(err)
	hs.Equal(hs.etalonSize, rh.GetSize())
	hs.Equal(delivery.DestinationNamesType, rh.GetType())
}

type FrameSuite struct {
	suite.Suite

	etalonsNames         []string
	etalonNumberOfShards uint8
	etalonBlockID        uuid.UUID
	rw                   *FileBuffer
	ctx                  context.Context
}

func TestFrameSuite(t *testing.T) {
	suite.Run(t, new(FrameSuite))
}

func (fs *FrameSuite) SetupSuite() {
	var err error
	fs.ctx = context.Background()
	fs.rw = NewFileBuffer()

	fs.etalonsNames = []string{
		"www.collector.com",
		"www.collector-dev.com",
		"www.collector-prod.com",
		"www.collector-replica.com",
	}
	fs.etalonNumberOfShards = 42
	fs.etalonBlockID, err = uuid.NewRandom()
	fs.NoError(err)
}

func (fs *FrameSuite) TearDownTest() {
	fs.rw.Reset()
}

func (fs *FrameSuite) TestFrameEmpty() {
	fre := delivery.NewFrameEmpty()
	buf := fre.Encode()
	n, err := fs.rw.Write(buf)
	fs.NoError(err)

	fs.Equal(11, fre.SizeOf())
	fs.Equal(fre.SizeOf(), len(buf))
	fs.Equal(fre.SizeOf(), n)

	_, err = delivery.ReadHeader(fs.ctx, fs.rw, 0)
	fs.ErrorIs(err, delivery.ErrUnknownFrameType)
}

func (fs *FrameSuite) TestFrameWriteRead() {
	tbf := delivery.NewTitleFrame(fs.etalonNumberOfShards, fs.etalonBlockID)
	buf := tbf.Encode()
	n, err := fs.rw.Write(buf)
	fs.NoError(err)
	fs.Equal(len(buf), n)
	fs.Equal(len(buf), tbf.SizeOf())

	dnf := delivery.NewDestinationsNamesFrameWithNames(fs.etalonsNames...)
	buf = dnf.Encode()
	n, err = fs.rw.Write(buf)
	fs.NoError(err)
	fs.Equal(len(buf), n)
	fs.Equal(len(buf), dnf.SizeOf())

	var off int64

	title, off, err := delivery.ReadFrameTitle(fs.ctx, fs.rw, off)
	fs.NoError(err)
	fs.Equal(fs.etalonBlockID, title.GetBlockID())
	fs.Equal(fs.etalonNumberOfShards, title.GetShardsNumberPower())

	names, _, err := delivery.ReadDestinationsNamesFrame(fs.ctx, fs.rw, off)
	fs.NoError(err)
	fs.ElementsMatch(fs.etalonsNames, names.ToString())
}

type StorageManagerSuite struct {
	suite.Suite

	cfg                     *delivery.FileStorageConfig
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

	s.cfg = &delivery.FileStorageConfig{
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
	s.etalonsData = newDataTest([]byte{
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		12,
		13,
		14,
		15,
		16,
		17,
		18,
		19,
		20,
		21,
		22,
		23,
		24,
		25,
		26,
		27,
		28,
		29,
		30,
		31,
		32,
		33,
		34,
		35,
		36,
		37,
		38,
		39,
		40,
		41,
		42,
	})
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
	s.NoError(err)

	s.ElementsMatch(s.etalonsData.Bytes(), actualSeg.Bytes())
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
	s.NoError(err)

	s.ElementsMatch(s.etalonsData.Bytes(), actualSnap.Bytes())
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
	s.NoError(err)
	s.ElementsMatch(s.etalonsData.Bytes(), actualSeg.Bytes())

	actualSnap, err := s.sm.GetSnapshot(s.ctx, segKey)
	s.NoError(err)
	s.ElementsMatch(s.etalonsData.Bytes(), actualSnap.Bytes())

	actualAckStatus := s.sm.GetAckStatus()
	index, ok := actualAckStatus.Index("www.collector.com")
	s.True(ok)
	s.Equal(3, index)

	s.Equal(expectAckStatus.GetCopyAckStatuses(), actualAckStatus.GetCopyAckStatuses())
	s.Equal(s.etalonsNames, actualAckStatus.GetNames().ToString())
}

type TitleFrameSuite struct {
	suite.Suite

	etalonNumberOfShards uint8
	etalonBlockID        uuid.UUID
	rw                   *FileBuffer
	ctx                  context.Context
}

func TestTitleBodySuite(t *testing.T) {
	suite.Run(t, new(TitleFrameSuite))
}

func (tfs *TitleFrameSuite) SetupSuite() {
	var err error
	tfs.ctx = context.Background()
	tfs.etalonNumberOfShards = 42
	tfs.etalonBlockID, err = uuid.NewRandom()
	tfs.NoError(err)

	tfs.rw = NewFileBuffer()
}

func (tfs *TitleFrameSuite) TearDownTest() {
	tfs.rw.Reset()
}

func (tfs *TitleFrameSuite) TestTitleBodyWriteRead() {
	wtb := delivery.NewTitle(tfs.etalonNumberOfShards, tfs.etalonBlockID)

	n, err := tfs.rw.Write(wtb.EncodeBinary())
	tfs.NoError(err)
	tfs.Equal(n, wtb.SizeOf())

	rtb, err := delivery.ReadTitle(tfs.rw, 0)
	tfs.NoError(err)
	tfs.Equal(tfs.etalonNumberOfShards, rtb.GetShardsNumberPower())
	tfs.Equal(tfs.etalonBlockID, rtb.GetBlockID())
}

func (tfs *TitleFrameSuite) TestFrame() {
	fr := delivery.NewTitleFrame(tfs.etalonNumberOfShards, tfs.etalonBlockID)

	_, err := tfs.rw.Write(fr.Encode())
	tfs.NoError(err)

	title, _, err := delivery.ReadFrameTitle(tfs.ctx, tfs.rw, 0)
	tfs.NoError(err)

	tfs.Equal(tfs.etalonNumberOfShards, title.GetShardsNumberPower())
	tfs.Equal(tfs.etalonBlockID, title.GetBlockID())
}

type StatusesFrameSuite struct {
	suite.Suite

	etalonsStatuses delivery.Statuses
	rw              *FileBuffer
	ctx             context.Context
}

func TestStatusesFrameSuite(t *testing.T) {
	suite.Run(t, new(StatusesFrameSuite))
}

func (sfs *StatusesFrameSuite) SetupSuite() {
	sfs.ctx = context.Background()
	sfs.etalonsStatuses = []uint32{
		1, 2, 3,
		4, 5, 6,
		7, 8, 9,
	}

	sfs.rw = NewFileBuffer()
}

func (sfs *StatusesFrameSuite) TearDownTest() {
	sfs.rw.Reset()
}

func (sfs *StatusesFrameSuite) TestStatusesWriteRead() {
	buf, err := sfs.etalonsStatuses.MarshalBinary()
	sfs.NoError(err)

	var rs delivery.Statuses
	err = rs.UnmarshalBinary(buf)
	sfs.NoError(err)

	sfs.ElementsMatch(sfs.etalonsStatuses, rs)
}

func (sfs *StatusesFrameSuite) TestFrameStatuses() {
	wframe, err := delivery.NewStatusesFrame(sfs.etalonsStatuses)
	sfs.NoError(err)

	_, err = sfs.rw.Write(wframe.Encode())
	sfs.NoError(err)

	rss, err := delivery.ReadFrameStatuses(sfs.ctx, sfs.rw, 0)
	sfs.NoError(err)
	sfs.ElementsMatch(sfs.etalonsStatuses, rss)
}

func (sfs *StatusesFrameSuite) TestNewStatuses() {
	newss := make(delivery.Statuses, len(sfs.etalonsStatuses))
	copy(newss, sfs.etalonsStatuses)

	sfs.ElementsMatch(sfs.etalonsStatuses, newss)

	newss[0] = 4
	sfs.NotEqual(sfs.etalonsStatuses, newss)

	newss[0] = sfs.etalonsStatuses[0]
	newss.Reset()
	sfs.NotEqual(sfs.etalonsStatuses, newss)
}

type DestinationsNamesFrameSuite struct {
	suite.Suite

	etalonsNames []string
	rw           *FileBuffer
	ctx          context.Context
}

func TestDestinationsNamesFrameSuite(t *testing.T) {
	suite.Run(t, new(DestinationsNamesFrameSuite))
}

func (dnfs *DestinationsNamesFrameSuite) SetupSuite() {
	dnfs.ctx = context.Background()
	dnfs.etalonsNames = []string{
		"www.bcollector.com",
		"www.ucollector-dev.com",
		"www.fcollector-prod.com",
		"www.ncollector-replica.com",
	}

	dnfs.rw = NewFileBuffer()
}

func (dnfs *DestinationsNamesFrameSuite) TearDownTest() {
	dnfs.rw.Reset()
}

func (dnfs *DestinationsNamesFrameSuite) TestBodyToString() {
	nb := delivery.NewDestinationsNames(dnfs.etalonsNames...)
	actualNames := nb.ToString()
	dnfs.True(sort.StringsAreSorted(actualNames))

	dnfs.ElementsMatch(dnfs.etalonsNames, actualNames)
}

func (dnfs *DestinationsNamesFrameSuite) TestNamesWriteRead() {
	wdn := delivery.NewDestinationsNames(dnfs.etalonsNames...)

	_, err := dnfs.rw.Write(wdn.EncodeBinary())
	dnfs.NoError(err)

	var off int64

	rb1, err := delivery.ReadDestinationsNames(dnfs.ctx, dnfs.rw, off)
	dnfs.NoError(err)
	dnfs.ElementsMatch(dnfs.etalonsNames, rb1.ToString())
}

func (dnfs *DestinationsNamesFrameSuite) TestFrameNames() {
	wframe := delivery.NewDestinationsNamesFrameWithNames(dnfs.etalonsNames...)

	_, err := dnfs.rw.Write(wframe.Encode())
	dnfs.NoError(err)

	rdn, _, err := delivery.ReadDestinationsNamesFrame(dnfs.ctx, dnfs.rw, 0)
	dnfs.NoError(err)

	actualNames := rdn.ToString()
	dnfs.ElementsMatch(dnfs.etalonsNames, actualNames)
}

type BinaryFrameSuite struct {
	suite.Suite

	etalonsData []byte
	rw          *FileBuffer
	ctx         context.Context
}

func TestSegmentFrameSuite(t *testing.T) {
	suite.Run(t, new(BinaryFrameSuite))
}

func (bfs *BinaryFrameSuite) SetupSuite() {
	bfs.ctx = context.Background()
	bfs.etalonsData = []byte{
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		12,
		13,
		14,
		15,
		16,
		17,
		18,
		19,
		20,
		21,
		22,
		23,
		24,
		25,
		26,
		27,
		28,
		29,
		30,
		31,
		32,
		33,
		34,
		35,
		36,
		37,
		38,
		39,
		40,
		41,
		42,
	}

	bfs.rw = NewFileBuffer()
}

func (bfs *BinaryFrameSuite) TearDownTest() {
	bfs.rw.Reset()
}

func (bfs *BinaryFrameSuite) TestSegmentBody() {
	wbb := delivery.NewBinaryBody(bfs.etalonsData)

	_, err := bfs.rw.Write(wbb.EncodeBinary())
	bfs.NoError(err)

	rbody, err := delivery.ReadBinaryBodyBody(bfs.ctx, bfs.rw, 0)
	bfs.NoError(err)
	bfs.ElementsMatch(bfs.etalonsData, rbody.Bytes())
}

func (bfs *BinaryFrameSuite) TestSegmentFrame() {
	wframe := delivery.NewSegmentFrame(0, 0, bfs.etalonsData)

	_, err := bfs.rw.Write(wframe.Encode())
	bfs.NoError(err)

	rsb, err := delivery.ReadFrameSegment(bfs.ctx, bfs.rw, 0)
	bfs.NoError(err)

	bfs.ElementsMatch(bfs.etalonsData, rsb.Bytes())
}

func (bfs *BinaryFrameSuite) TestSnapshotFrame() {
	wframe := delivery.NewSnapshotFrame(0, 0, bfs.etalonsData)

	_, err := bfs.rw.Write(wframe.Encode())
	bfs.NoError(err)

	rsb, err := delivery.ReadFrameSnapshot(bfs.ctx, bfs.rw, 0)
	bfs.NoError(err)

	bfs.ElementsMatch(bfs.etalonsData, rsb.Bytes())
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

type RejectStatusesSuite struct {
	suite.Suite

	etalonsData delivery.RejectStatuses
	rw          *FileBuffer
	ctx         context.Context
}

func TestRejectStatusesSuite(t *testing.T) {
	suite.Run(t, new(RejectStatusesSuite))
}

func (rss *RejectStatusesSuite) SetupSuite() {
	rss.ctx = context.Background()
	rss.etalonsData = delivery.RejectStatuses{
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

func (rss *RejectStatusesSuite) TearDownTest() {
	rss.rw.Reset()
}

func (rss *RejectStatusesSuite) TestEncodeDecode() {
	rss.T().Log("encoding reject statuses and write")
	buf, err := rss.etalonsData.MarshalBinary()
	rss.NoError(err)

	rss.T().Log("read and decoding reject statuses")
	var rjss delivery.RejectStatuses
	err = rjss.UnmarshalBinary(buf)
	rss.NoError(err)

	rss.T().Log("sort etalon data and match with decoding data")
	rss.Equal(rss.etalonsData, rjss)
}

type RejectsSuite struct {
	suite.Suite

	etalonsData delivery.RejectStatuses
	rw          *FileBuffer
	ctx         context.Context
}

func TestRejectsSuite(t *testing.T) {
	suite.Run(t, new(RejectsSuite))
}

func (rss *RejectsSuite) SetupSuite() {
	rss.ctx = context.Background()
	rss.etalonsData = delivery.RejectStatuses{
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
	var rrs delivery.RejectStatuses
	err = rrs.UnmarshalBinary(buf)
	rss.NoError(err)

	rss.T().Log("sort etalon data and match with decoding data")
	rss.Equal(rss.etalonsData, rrs)
}
