package frames_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"testing/quick"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/odarix/odarix-core-go/util"
	"github.com/stretchr/testify/suite"
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

type FrameSuite struct {
	suite.Suite

	rw      *FileBuffer
	version uint8
}

func TestFrameSuite(t *testing.T) {
	suite.Run(t, new(FrameSuite))
}

func (s *FrameSuite) SetupSuite() {
	s.rw = NewFileBuffer()
	s.version = 4
}

func (s *FrameSuite) TearDownTest() {
	s.rw.Reset()
}

func (s *FrameSuite) TestFrame() {
	var (
		version   uint8            = 4
		typeFrame frames.TypeFrame = frames.AuthType
		shardID   uint16           = 1
	)
	body, err := frames.NewAuthMsg(
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		shardID,
	).MarshalBinary()
	s.Require().NoError(err)
	wm, err := frames.NewFrame(version, frames.ContentVersion1, typeFrame, body)
	s.Require().NoError(err)

	b := wm.EncodeBinary()
	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	rm := frames.NewFrameEmpty()
	err = rm.Read(context.Background(), s.rw)
	s.Require().NoError(err)

	err = rm.Validate()
	s.Require().NoError(err)

	s.Require().Equal(*wm, *rm)

	s.Require().Equal(wm.GetHeader(), rm.GetHeader())
	s.Require().Equal(version, rm.GetVersion())
	s.Require().Equal(typeFrame, rm.GetType())
	s.Require().Equal(wm.GetCreatedAt(), rm.GetCreatedAt())
}

func (s *FrameSuite) TestFrameQuick() {
	f := func(shardID uint16) bool {
		body, err := frames.NewAuthMsg(
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
			shardID,
		).MarshalBinary()
		s.Require().NoError(err)
		wm, err := frames.NewFrame(s.version, frames.ContentVersion1, frames.AuthType, body)
		s.Require().NoError(err)
		b := wm.EncodeBinary()

		rw := NewFileBuffer()
		_, err = rw.Write(b)
		s.Require().NoError(err)

		_, err = rw.Seek(0, 0)
		s.Require().NoError(err)

		rm := frames.NewFrameEmpty()
		err = rm.Read(context.Background(), rw)
		s.Require().NoError(err)

		err = rm.Validate()
		s.Require().NoError(err)

		s.Require().Equal(wm.GetHeader(), rm.GetHeader())
		s.Require().Equal(s.version, rm.GetVersion())
		s.Require().Equal(wm.GetType(), rm.GetType())
		s.Require().Equal(wm.GetCreatedAt(), rm.GetCreatedAt())

		return s.Equal(*wm, *rm)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

type authMsgTest struct {
	Token         string
	AgentUUID     string
	ProductName   string
	AgentHostname string
}

func newAuthMsgTest(token, agentUUID, productName, agentHostname string) *authMsgTest {
	return &authMsgTest{
		Token:         token,
		AgentUUID:     agentUUID,
		ProductName:   productName,
		AgentHostname: agentHostname,
	}
}

// MarshalBinary - encoding to byte.
func (am *authMsgTest) MarshalBinary() ([]byte, error) {
	//revive:disable-next-line:add-constant this not constant
	length := 4 + len(am.Token) + len(am.AgentUUID) + len(am.ProductName) + len(am.AgentHostname)
	buf := make([]byte, 0, length)

	buf = binary.AppendUvarint(buf, uint64(len(am.Token)))
	buf = append(buf, am.Token...)

	buf = binary.AppendUvarint(buf, uint64(len(am.AgentUUID)))
	buf = append(buf, am.AgentUUID...)

	buf = binary.AppendUvarint(buf, uint64(len(am.ProductName)))
	buf = append(buf, am.ProductName...)

	buf = binary.AppendUvarint(buf, uint64(len(am.AgentHostname)))
	buf = append(buf, am.AgentHostname...)

	return buf, nil
}

func (s *FrameSuite) TestAuthMsg() {
	wm := frames.NewAuthMsg(uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), 1)

	b, _ := wm.MarshalBinary()
	rm := new(frames.AuthMsg)
	err := rm.UnmarshalBinary(b)
	s.Require().NoError(err)

	err = rm.Validate()
	s.Require().NoError(err)

	s.Require().Equal(*wm, *rm)
}

func (s *FrameSuite) TestAuthMsgOld() {
	token := uuid.NewString()
	agentUUID := uuid.NewString()
	productName := uuid.NewString()
	agentHostname := uuid.NewString()
	wm := newAuthMsgTest(token, agentUUID, productName, agentHostname)

	b, _ := wm.MarshalBinary()
	rm := new(frames.AuthMsg)
	err := rm.UnmarshalBinary(b)
	s.Require().NoError(err)

	err = rm.Validate()
	s.Require().NoError(err)

	s.Require().Equal(token, rm.Token)
	s.Require().Equal(agentUUID, rm.AgentUUID)
	s.Require().Equal(productName, rm.ProductName)
	s.Require().Equal(agentHostname, rm.AgentHostname)
}

func (s *FrameSuite) TestAuthMsgQuick() {
	f := func(token, agentUUID, productName, agentHostname, blockID string, shardID uint16) bool {
		wm := frames.NewAuthMsg(token, agentUUID, productName, agentHostname, blockID, shardID)

		b, _ := wm.MarshalBinary()
		rm := new(frames.AuthMsg)
		err := rm.UnmarshalBinary(b)
		s.Require().NoError(err)

		return s.Equal(*wm, *rm)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestAuthMsgError() {
	wm := frames.NewAuthMsg("", uuid.NewString(), "", "", "", 0)
	err := wm.Validate()
	s.Require().ErrorIs(err, frames.ErrTokenEmpty)

	wm = frames.NewAuthMsg(uuid.NewString(), "", "", "", "", 0)
	err = wm.Validate()
	s.Require().ErrorIs(err, frames.ErrUUIDEmpty)
}

func (s *FrameSuite) TestAuthMsgFrameAt() {
	ctx := context.Background()
	token := uuid.NewString()
	agentUUID := uuid.NewString()
	productName := uuid.NewString()
	agentHostname := uuid.NewString()
	blockID := uuid.NewString()
	shardID := uint16(faker.Longitude())
	wm, err := frames.NewAuthFrame(s.version, token, agentUUID, productName, agentHostname, blockID, shardID)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.AuthType, h.GetType())

	rm, err := frames.ReadAtAuthMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(token, rm.Token)
	s.Require().Equal(agentUUID, rm.AgentUUID)
	s.Require().Equal(productName, rm.ProductName)
	s.Require().Equal(agentHostname, rm.AgentHostname)
	s.Require().Equal(blockID, rm.BlockID)
	s.Require().Equal(shardID, rm.ShardID)
}

func (s *FrameSuite) TestAuthMsgFrame() {
	ctx := context.Background()
	token := uuid.NewString()
	agentUUID := uuid.NewString()
	productName := uuid.NewString()
	agentHostname := uuid.NewString()
	blockID := uuid.NewString()
	shardID := uint16(faker.Longitude())
	wm, err := frames.NewAuthFrame(s.version, token, agentUUID, productName, agentHostname, blockID, shardID)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.AuthType, h.GetType())

	rm, err := frames.ReadAuthMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(token, rm.Token)
	s.Require().Equal(agentUUID, rm.AgentUUID)
	s.Require().Equal(productName, rm.ProductName)
	s.Require().Equal(agentHostname, rm.AgentHostname)
	s.Require().Equal(blockID, rm.BlockID)
	s.Require().Equal(shardID, rm.ShardID)
}

func (s *FrameSuite) TestAuthMsgFrameAtWithMsg() {
	ctx := context.Background()
	token := uuid.NewString()
	agentUUID := uuid.NewString()
	productName := uuid.NewString()
	agentHostname := uuid.NewString()
	blockID := uuid.NewString()
	shardID := uint16(faker.Longitude())
	msg := frames.NewAuthMsg(token, agentUUID, productName, agentHostname, blockID, shardID)
	wm, err := frames.NewAuthFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.AuthType, h.GetType())

	rm, err := frames.ReadAtAuthMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(token, rm.Token)
	s.Require().Equal(agentUUID, rm.AgentUUID)
	s.Require().Equal(productName, rm.ProductName)
	s.Require().Equal(agentHostname, rm.AgentHostname)
	s.Require().Equal(blockID, rm.BlockID)
	s.Require().Equal(shardID, rm.ShardID)
}

func (s *FrameSuite) TestAuthMsgFrameWithMsg() {
	ctx := context.Background()
	token := uuid.NewString()
	agentUUID := uuid.NewString()
	productName := uuid.NewString()
	agentHostname := uuid.NewString()
	blockID := uuid.NewString()
	shardID := uint16(faker.Longitude())
	msg := frames.NewAuthMsg(token, agentUUID, productName, agentHostname, blockID, shardID)
	wm, err := frames.NewAuthFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.AuthType, h.GetType())

	rm, err := frames.ReadAuthMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(token, rm.Token)
	s.Require().Equal(agentUUID, rm.AgentUUID)
	s.Require().Equal(productName, rm.ProductName)
	s.Require().Equal(agentHostname, rm.AgentHostname)
	s.Require().Equal(blockID, rm.BlockID)
	s.Require().Equal(shardID, rm.ShardID)
}

func (s *FrameSuite) TestResponseMsg() {
	wm := frames.NewResponseMsg("ok", 200, 45000, 1688452727260423481)

	b, err := wm.MarshalBinary()
	s.Require().NoError(err)
	rm := new(frames.ResponseMsg)
	err = rm.UnmarshalBinary(b)
	s.Require().NoError(err)

	s.Equal(*wm, *rm)
}

func (s *FrameSuite) TestResponseMsgQuick() {
	f := func(text string, code, segmentID uint32, sendAt int64) bool {
		wm := frames.NewResponseMsg(text, code, segmentID, sendAt)

		b, err := wm.MarshalBinary()
		s.Require().NoError(err)
		rm := new(frames.ResponseMsg)
		err = rm.UnmarshalBinary(b)
		s.Require().NoError(err)

		return s.Equal(*wm, *rm)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestResponseMsgFrameAt() {
	ctx := context.Background()
	var (
		text             = "ok"
		code      uint32 = 200
		segmentID uint32 = 45000
		sendAt    int64  = 1688452727260423481
	)
	wm, err := frames.NewResponseFrame(s.version, text, code, segmentID, sendAt)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.ResponseType, h.GetType())

	rm, err := frames.ReadAtResponseMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(text, rm.Text)
	s.Require().Equal(code, rm.Code)
	s.Require().Equal(segmentID, rm.SegmentID)
	s.Require().Equal(sendAt, rm.SendAt)
}

func (s *FrameSuite) TestResponseMsgFrame() {
	ctx := context.Background()
	var (
		text             = "ok"
		code      uint32 = 200
		segmentID uint32 = 45000
		sendAt    int64  = 1688452727260423481
	)
	wm, err := frames.NewResponseFrame(s.version, text, code, segmentID, sendAt)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.ResponseType, h.GetType())

	rm, err := frames.ReadResponseMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(text, rm.Text)
	s.Require().Equal(code, rm.Code)
	s.Require().Equal(segmentID, rm.SegmentID)
	s.Require().Equal(sendAt, rm.SendAt)
}

func (s *FrameSuite) TestResponseMsgFrameWithMsgAt() {
	ctx := context.Background()
	var (
		text             = "ok"
		code      uint32 = 200
		segmentID uint32 = 45000
		sendAt    int64  = 1688452727260423481
	)
	msg := frames.NewResponseMsg(text, code, segmentID, sendAt)
	wm, err := frames.NewResponseFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.ResponseType, h.GetType())

	rm, err := frames.ReadAtResponseMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(text, rm.Text)
	s.Require().Equal(code, rm.Code)
	s.Require().Equal(segmentID, rm.SegmentID)
	s.Require().Equal(sendAt, rm.SendAt)
}

func (s *FrameSuite) TestResponseMsgFrameWithMsg() {
	ctx := context.Background()
	var (
		text             = "ok"
		code      uint32 = 200
		segmentID uint32 = 45000
		sendAt    int64  = 1688452727260423481
	)
	msg := frames.NewResponseMsg(text, code, segmentID, sendAt)
	wm, err := frames.NewResponseFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.ResponseType, h.GetType())

	rm, err := frames.ReadResponseMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(text, rm.Text)
	s.Require().Equal(code, rm.Code)
	s.Require().Equal(segmentID, rm.SegmentID)
	s.Require().Equal(sendAt, rm.SendAt)
}

func (s *FrameSuite) TestRefillMsg() {
	wm := frames.NewRefillMsg(
		[]frames.MessageData{
			{ID: 0, Size: 5, Typemsg: 2},
			{ID: 1, Size: 6, Typemsg: 3},
			{ID: 2, Size: 12, Typemsg: 4},
			{ID: 3, Size: 4, Typemsg: 5},
			{ID: 4294967294, Size: 4294967294, Typemsg: 5},
		},
	)

	b, _ := wm.MarshalBinary()
	rm := new(frames.RefillMsg)
	err := rm.UnmarshalBinary(b)
	s.Require().NoError(err)

	s.Require().Equal(len(wm.Messages), len(rm.Messages))

	for i := range wm.Messages {
		s.Require().Equal(wm.Messages[i], rm.Messages[i])
	}
}

func (s *FrameSuite) TestRefillMsgQuick() {
	f := func(id, size uint32, tmsg int8) bool {
		wm := frames.NewRefillMsg(
			[]frames.MessageData{
				{ID: id, Size: size, Typemsg: frames.TypeFrame(tmsg)},
			},
		)

		b, _ := wm.MarshalBinary()
		rm := new(frames.RefillMsg)
		err := rm.UnmarshalBinary(b)
		s.Require().NoError(err)

		if !s.Equal(len(wm.Messages), len(rm.Messages)) {
			return false
		}

		for i := range wm.Messages {
			if !s.Equal(wm.Messages[i], rm.Messages[i]) {
				return false
			}
		}

		return true
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestRefillMsgFrameAt() {
	ctx := context.Background()
	var (
		msgs = []frames.MessageData{
			{ID: 0, Size: 5, Typemsg: 2},
			{ID: 1, Size: 6, Typemsg: 3},
			{ID: 2, Size: 12, Typemsg: 4},
			{ID: 3, Size: 4, Typemsg: 5},
			{ID: 4294967294, Size: 4294967294, Typemsg: 5},
		}
	)

	wm, err := frames.NewRefillFrame(s.version, msgs)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.RefillType, h.GetType())

	rm, err := frames.ReadAtRefillMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(msgs, rm.Messages)
}

func (s *FrameSuite) TestRefillMsgFrame() {
	ctx := context.Background()
	var (
		msgs = []frames.MessageData{
			{ID: 0, Size: 5, Typemsg: 2},
			{ID: 1, Size: 6, Typemsg: 3},
			{ID: 2, Size: 12, Typemsg: 4},
			{ID: 3, Size: 4, Typemsg: 5},
			{ID: 4294967294, Size: 4294967294, Typemsg: 5},
		}
	)

	wm, err := frames.NewRefillFrame(s.version, msgs)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.RefillType, h.GetType())

	rm, err := frames.ReadRefillMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(msgs, rm.Messages)
}

func (s *FrameSuite) TestRefillMsgFrameAtWithMsg() {
	ctx := context.Background()
	var (
		msgs = []frames.MessageData{
			{ID: 0, Size: 5, Typemsg: 2},
			{ID: 1, Size: 6, Typemsg: 3},
			{ID: 2, Size: 12, Typemsg: 4},
			{ID: 3, Size: 4, Typemsg: 5},
			{ID: 4294967294, Size: 4294967294, Typemsg: 5},
		}
	)
	msg := frames.NewRefillMsg(msgs)
	wm, err := frames.NewRefillFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.RefillType, h.GetType())

	rm, err := frames.ReadAtRefillMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(msgs, rm.Messages)
}

func (s *FrameSuite) TestRefillMsgFrameWithMsg() {
	ctx := context.Background()
	var (
		msgs = []frames.MessageData{
			{ID: 0, Size: 5, Typemsg: 2},
			{ID: 1, Size: 6, Typemsg: 3},
			{ID: 2, Size: 12, Typemsg: 4},
			{ID: 3, Size: 4, Typemsg: 5},
			{ID: 4294967294, Size: 4294967294, Typemsg: 5},
		}
	)
	msg := frames.NewRefillMsg(msgs)
	wm, err := frames.NewRefillFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.RefillType, h.GetType())

	rm, err := frames.ReadRefillMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(msgs, rm.Messages)
}

func (s *FrameSuite) TestDestinationsNames() {
	names := []string{
		"www.bcollector.com",
		"www.ucollector-dev.com",
		"www.fcollector-prod.com",
		"www.ncollector-replica.com",
	}
	wm := frames.NewDestinationsNames(names...)
	b, err := wm.MarshalBinary()
	s.Require().NoError(err)

	rm := frames.NewDestinationsNamesEmpty()
	err = rm.UnmarshalBinary(b)
	s.Require().NoError(err)

	s.Require().Equal(wm.ToString(), rm.ToString())
	s.Require().Equal(len(names), rm.Len())
	s.Require().True(rm.Equal(names...))

	for i := range names {
		s.Require().Equal(names[i], rm.IDToString(int32(i)))
		s.Require().Equal(int32(i), rm.StringToID(names[i]))
	}

	rm.Range(func(name string, id int) bool {
		return s.Equal(names[id], name)
	})
}

func (s *FrameSuite) TestDestinationsNamesQuick() {
	f := func(names string) bool {
		wm := frames.NewDestinationsNames(names)
		b, err := wm.MarshalBinary()
		s.Require().NoError(err)

		rm := frames.NewDestinationsNamesEmpty()
		err = rm.UnmarshalBinary(b)
		s.Require().NoError(err)

		return s.Equal(wm.ToString(), rm.ToString())
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestDestinationsNamesFrameAt() {
	ctx := context.Background()
	var (
		names = []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	)
	wm, err := frames.NewDestinationsNamesFrame(s.version, names...)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.DestinationNamesType, h.GetType())

	rm, err := frames.ReadAtDestinationsNames(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(names, rm.ToString())
}

func (s *FrameSuite) TestDestinationsNamesFrame() {
	ctx := context.Background()
	var (
		names = []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	)
	wm, err := frames.NewDestinationsNamesFrame(s.version, names...)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.DestinationNamesType, h.GetType())

	rm, err := frames.ReadDestinationsNames(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(names, rm.ToString())
}

func (s *FrameSuite) TestDestinationsNamesFrameAtWithMsg() {
	ctx := context.Background()
	var (
		names = []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	)
	msg := frames.NewDestinationsNames(names...)
	wm, err := frames.NewDestinationsNamesFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.DestinationNamesType, h.GetType())

	rm, err := frames.ReadAtDestinationsNames(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(names, rm.ToString())
}

func (s *FrameSuite) TestDestinationsNamesFrameWithMsg() {
	ctx := context.Background()
	var (
		names = []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	)
	msg := frames.NewDestinationsNames(names...)
	wm, err := frames.NewDestinationsNamesFrameWithMsg(s.version, msg)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.DestinationNamesType, h.GetType())

	rm, err := frames.ReadDestinationsNames(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(names, rm.ToString())
}

func (s *FrameSuite) TestStatuses() {
	wm := frames.Statuses([]uint32{1, 2, 3, 4, 5})
	b, err := wm.MarshalBinary()
	s.Require().NoError(err)

	var rm frames.Statuses
	err = rm.UnmarshalBinary(b)
	s.Require().NoError(err)
	s.Require().True(rm.Equal(wm))
}

func (s *FrameSuite) TestStatusesQuick() {
	f := func(data []uint32) bool {
		wm := frames.Statuses(data)
		b, err := wm.MarshalBinary()
		s.Require().NoError(err)

		var rm frames.Statuses
		err = rm.UnmarshalBinary(b)
		s.Require().NoError(err)
		return s.True(rm.Equal(wm))
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestStatusesFrameAt() {
	ctx := context.Background()
	var (
		data frames.Statuses = []uint32{1, 2, 3, 4, 5}
	)
	wm, err := frames.NewStatusesFrame(data)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.StatusType, h.GetType())

	rm, err := frames.ReadAtFrameStatuses(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().True(rm.Equal(data))
}

func (s *FrameSuite) TestStatusesFrame() {
	ctx := context.Background()
	var (
		data frames.Statuses = []uint32{1, 2, 3, 4, 5}
	)
	wm, err := frames.NewStatusesFrame(data)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.StatusType, h.GetType())

	rm, err := frames.ReadFrameStatuses(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().True(rm.Equal(data))
}

func (s *FrameSuite) TestRejectStatuses() {
	wm := frames.RejectStatuses{
		frames.Reject{
			NameID:  10,
			Segment: 15,
			ShardID: 1,
		},
	}
	b, err := wm.MarshalBinary()
	s.Require().NoError(err)

	var rm frames.RejectStatuses
	err = rm.UnmarshalBinary(b)
	s.Require().NoError(err)
	s.Require().Equal(wm, rm)
}

func (s *FrameSuite) TestRejectStatusesQuick() {
	f := func(mameID, segment uint32, shardID uint16) bool {
		wm := frames.RejectStatuses{
			frames.Reject{
				NameID:  mameID,
				Segment: segment,
				ShardID: shardID,
			},
		}
		b, err := wm.MarshalBinary()
		s.Require().NoError(err)

		var rm frames.RejectStatuses
		err = rm.UnmarshalBinary(b)
		s.Require().NoError(err)
		return s.Equal(wm, rm)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestRejectStatusesFrameAt() {
	ctx := context.Background()
	var (
		data = frames.RejectStatuses{
			frames.Reject{
				NameID:  10,
				Segment: 15,
				ShardID: 1,
			},
		}
	)
	wm, err := frames.NewRejectStatusesFrame(data)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.RejectStatusType, h.GetType())

	rm, err := frames.ReadAtFrameRejectStatuses(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(data, rm)
}

func (s *FrameSuite) TestRejectStatusesFrame() {
	ctx := context.Background()
	var (
		data = frames.RejectStatuses{
			frames.Reject{
				NameID:  10,
				Segment: 15,
				ShardID: 1,
			},
		}
	)
	wm, err := frames.NewRejectStatusesFrame(data)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.RejectStatusType, h.GetType())

	rm, err := frames.ReadFrameRejectStatuses(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(data, rm)
}

func (s *FrameSuite) TestRefillShardEOF() {
	wm := frames.NewRefillShardEOF(1, 1)
	b, err := wm.MarshalBinary()
	s.Require().NoError(err)

	rm := frames.NewRefillShardEOFEmpty()
	err = rm.UnmarshalBinary(b)
	s.Require().NoError(err)
	s.Require().Equal(*wm, *rm)
}

func (s *FrameSuite) TestRefillShardEOFQuick() {
	f := func(mameID, shardID uint16) bool {
		wm := frames.NewRefillShardEOF(1, 1)
		b, err := wm.MarshalBinary()
		s.Require().NoError(err)

		rm := frames.NewRefillShardEOFEmpty()
		err = rm.UnmarshalBinary(b)
		s.Require().NoError(err)
		return s.Equal(*wm, *rm)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestRefillShardEOFFrameAt() {
	ctx := context.Background()
	var (
		nameID  uint32 = 10
		shardID uint16 = 1
	)
	wm, err := frames.NewRefillShardEOFFrame(nameID, shardID)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.RefillShardEOFType, h.GetType())

	rm, err := frames.ReadAtFrameRefillShardEOF(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(nameID, rm.NameID)
	s.Require().Equal(shardID, rm.ShardID)
}

func (s *FrameSuite) TestRefillShardEOFFrame() {
	ctx := context.Background()
	var (
		nameID  uint32 = 10
		shardID uint16 = 1
	)
	wm, err := frames.NewRefillShardEOFFrame(nameID, shardID)
	s.Require().NoError(err)
	b := wm.EncodeBinary()

	_, err = s.rw.Write(b)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.RefillShardEOFType, h.GetType())

	rm, err := frames.ReadFrameRefillShardEOF(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(nameID, rm.NameID)
	s.Require().Equal(shardID, rm.ShardID)
}

func (s *FrameSuite) TestFinalMsg() {
	wm := frames.NewFinalMsg(true)
	b, err := wm.MarshalBinary()
	s.Require().NoError(err)

	rm := frames.NewFinalMsgEmpty()
	err = rm.UnmarshalBinary(b)
	s.Require().NoError(err)
	s.Require().Equal(*wm, *rm)
}

func (s *FrameSuite) TestFinalMsgQuick() {
	f := func(hasRefill bool) bool {
		wm := frames.NewFinalMsg(hasRefill)
		b, err := wm.MarshalBinary()
		s.Require().NoError(err)

		rm := frames.NewFinalMsgEmpty()
		err = rm.UnmarshalBinary(b)
		s.Require().NoError(err)
		return s.Equal(*wm, *rm)
	}

	err := quick.Check(f, nil)
	s.NoError(err)
}

func (s *FrameSuite) TestFinalMsgFrameAt() {
	ctx := context.Background()
	var (
		hasRefill = true
	)
	wm, err := frames.NewFinalMsgFrame(s.version, hasRefill)
	s.Require().NoError(err)

	_, err = wm.WriteTo(s.rw)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.FinalType, h.GetType())

	rm, err := frames.ReadAtFrameFinalMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(hasRefill, rm.HasRefill())
	s.Require().Equal(int64(1), rm.Size())
}

func (s *FrameSuite) TestFinalMsgWriteFrameAt() {
	ctx := context.Background()
	var (
		hasRefill            = true
		contentVersion uint8 = 1
	)
	wm, err := frames.NewWriteFrame(s.version, contentVersion, frames.FinalType, frames.NewFinalMsg(hasRefill))
	s.Require().NoError(err)

	_, err = wm.WriteTo(s.rw)
	s.Require().NoError(err)

	var off int64
	h, err := frames.ReadHeader(ctx, util.NewOffsetReader(s.rw, off))
	s.Require().NoError(err)
	off += int64(h.SizeOf())
	s.Require().Equal(frames.FinalType, h.GetType())

	rm, err := frames.ReadAtFrameFinalMsg(ctx, s.rw, off, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(hasRefill, rm.HasRefill())
	s.Require().Equal(int64(1), rm.Size())
}

func (s *FrameSuite) TestFinalMsgFrame() {
	ctx := context.Background()
	var (
		hasRefill = true
	)
	wm, err := frames.NewFinalMsgFrame(s.version, hasRefill)
	s.Require().NoError(err)

	_, err = wm.WriteTo(s.rw)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.FinalType, h.GetType())

	rm, err := frames.ReadFrameFinalMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(hasRefill, rm.HasRefill())
	s.Require().Equal(int64(1), rm.Size())
}

func (s *FrameSuite) TestFinalMsgWriteFrame() {
	ctx := context.Background()
	var (
		hasRefill            = true
		contentVersion uint8 = 1
	)
	wm, err := frames.NewWriteFrame(s.version, contentVersion, frames.FinalType, frames.NewFinalMsg(hasRefill))
	s.Require().NoError(err)

	_, err = wm.WriteTo(s.rw)
	s.Require().NoError(err)

	_, err = s.rw.Seek(0, 0)
	s.Require().NoError(err)

	h, err := frames.ReadHeader(ctx, s.rw)
	s.Require().NoError(err)
	s.Require().Equal(frames.FinalType, h.GetType())

	rm, err := frames.ReadFrameFinalMsg(ctx, s.rw, int(h.GetSize()))
	s.Require().NoError(err)

	s.Require().Equal(hasRefill, rm.HasRefill())
	s.Require().Equal(int64(1), rm.Size())
}
