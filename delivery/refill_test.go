package delivery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames/framestest"
)

type RefillSuite struct {
	suite.Suite

	etalonNewFileName    string
	etalonsNames         []string
	etalonNumberOfShards uint8
	etalonSEVersion      uint8
	etalonBlockID        uuid.UUID
	etalonsData          *dataTest
	workingDir           string
	mr                   *delivery.Refill
	ctx                  context.Context
}

func TestRefillSuite(t *testing.T) {
	suite.Run(t, new(RefillSuite))
}

func (s *RefillSuite) SetupSuite() {
	var err error
	s.ctx = context.Background()
	s.etalonNewFileName = "blablalblaUUID"

	s.workingDir, err = os.MkdirTemp("", filepath.Clean("refill-"))
	s.Require().NoError(err)

	s.etalonsNames = []string{
		"www.collector.com",
		"www.collector-dev.com",
		"www.collector-prod.com",
		"www.collector-replica.com",
	}
	s.etalonNumberOfShards = 1
	s.etalonSEVersion = 1
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

func (s *RefillSuite) SetupTest() {
	var err error
	s.mr, err = delivery.NewRefill(
		s.workingDir,
		s.etalonNumberOfShards,
		s.etalonSEVersion,
		s.etalonBlockID,
		false,
		nil,
		s.etalonsNames...,
	)
	s.NoError(err)
	s.Equal(s.etalonBlockID.String(), s.mr.BlockID().String())
	s.True(s.mr.IsContinuable())
}

func (s *RefillSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.workingDir))
}

func (s *RefillSuite) TestManagerInitIsContinuable() {
	segKey := cppbridge.SegmentKey{
		ShardID: 0,
		Segment: 1,
	}

	err := s.mr.WriteSegment(s.ctx, segKey, s.etalonsData)
	s.NoError(err)
	err = s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)

	mr1, err := delivery.NewRefill(
		s.workingDir,
		2,
		s.etalonSEVersion,
		s.etalonBlockID,
		false,
		nil,
		s.etalonsNames...,
	)
	s.NoError(err)
	s.False(mr1.IsContinuable())

	mr2, err := delivery.NewRefill(
		s.workingDir,
		s.etalonNumberOfShards,
		s.etalonSEVersion,
		s.etalonBlockID,
		false,
		nil,
		s.etalonsNames[:2]...,
	)
	s.NoError(err)
	s.False(mr2.IsContinuable())

	err = mr1.Shutdown(s.ctx)
	s.NoError(err)

	err = mr2.Shutdown(s.ctx)
	s.NoError(err)

	err = s.mr.Shutdown(s.ctx)
	s.NoError(err)
}

func (s *RefillSuite) TestSegment() {
	segKey := cppbridge.SegmentKey{
		ShardID: 0,
		Segment: 0,
	}

	s.mr.WriteSegment(
		s.ctx,
		segKey,
		s.etalonsData,
	)

	actualSeg, err := s.mr.Get(s.ctx, segKey)
	s.NoError(err)
	if buf, err := framestest.ReadPayload(actualSeg); s.NoError(err) {
		s.Equal(s.etalonsData.Bytes(), buf)
	}
}

func (s *RefillSuite) TestAckStatus() {
	s.T().Log("write ack status and check file not exist")
	err := s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)
	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillFileName+refillExt))
	s.Error(err, "File not deleted")

	s.T().Log("write segment, ack status and check file exist")
	err = s.mr.WriteSegment(s.ctx, cppbridge.SegmentKey{ShardID: 0, Segment: 0}, s.etalonsData)
	s.NoError(err)
	err = s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)
	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillDir, delivery.RefillFileName+refillExt))
	s.NoError(err, "File deleted")

	s.T().Log("ack segments for all name and check file deleted")
	for _, name := range s.etalonsNames {
		s.mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 0}, name)
		s.mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 1}, name)
	}
	err = s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)
	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillFileName+refillExt))
	s.Error(err, "File not deleted")

	err = s.mr.Shutdown(s.ctx)
	s.NoError(err)
}

func (s *RefillSuite) TestAckStatusWithReject() {
	s.T().Log("write ack status and check file not exist")
	err := s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)
	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillFileName+refillExt))
	s.Error(err, "File not deleted")

	s.T().Log("write segment, ack status and check file exist")
	err = s.mr.WriteSegment(s.ctx, cppbridge.SegmentKey{ShardID: 0, Segment: 0}, s.etalonsData)
	s.NoError(err)
	err = s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)
	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillDir, delivery.RefillFileName+refillExt))
	s.NoError(err, "File deleted")

	s.T().Log("ack segments for all name and 1 reject and check file not deleted")
	for _, name := range s.etalonsNames {
		s.mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 0}, name)
		s.mr.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 1}, name)
	}
	s.mr.Reject(cppbridge.SegmentKey{ShardID: 0, Segment: 3}, s.etalonsNames[0])
	err = s.mr.WriteAckStatus(s.ctx)
	s.NoError(err)
	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillDir, delivery.RefillFileName+refillExt))
	s.NoError(err, "File deleted")

	err = s.mr.Shutdown(s.ctx)
	s.NoError(err)
}

func (s *RefillSuite) TestRename() {
	segKey := cppbridge.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	err := s.mr.WriteSegment(
		s.ctx,
		segKey,
		s.etalonsData,
	)
	s.NoError(err)

	s.NoError(s.mr.IntermediateRename())
	s.NoError(s.mr.Shutdown(s.ctx))

	_, err = os.Stat(filepath.Join(s.workingDir, delivery.RefillDir, delivery.RefillFileName+refillExt))
	s.Error(err, "File not rotated")
}
