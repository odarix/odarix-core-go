package delivery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/delivery"
)

type RefillSuite struct {
	suite.Suite

	etalonNewFileName    string
	etalonsNames         []string
	etalonNumberOfShards uint8
	etalonBlockID        uuid.UUID
	etalonsData          *dataTest
	cfg                  *delivery.FileStorageConfig
	mr                   *delivery.Refill
	ctx                  context.Context
}

func TestRefillSuite(t *testing.T) {
	suite.Run(t, new(RefillSuite))
}

func (rs *RefillSuite) SetupSuite() {
	var err error
	rs.ctx = context.Background()
	rs.etalonNewFileName = "blablalblaUUID"
	rs.cfg = &delivery.FileStorageConfig{
		Dir:      "/tmp/refill",
		FileName: "current",
	}
	rs.etalonsNames = []string{
		"www.collector.com",
		"www.collector-dev.com",
		"www.collector-prod.com",
		"www.collector-replica.com",
	}
	rs.etalonNumberOfShards = 1
	rs.etalonBlockID, err = uuid.NewRandom()
	rs.NoError(err)
	rs.etalonsData = newDataTest([]byte{
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

func (rs *RefillSuite) SetupTest() {
	var err error
	rs.mr, err = delivery.NewRefill(
		rs.cfg,
		rs.etalonNumberOfShards,
		rs.etalonBlockID,
		rs.etalonsNames...,
	)
	rs.NoError(err)
	rs.Equal(rs.etalonBlockID.String(), rs.mr.BlockID().String())
	rs.True(rs.mr.IsContinuable())
}

func (rs *RefillSuite) TearDownTest() {
	err := os.RemoveAll(rs.cfg.Dir)
	rs.NoError(err)

	err = rs.mr.Shutdown(rs.ctx)
	rs.NoError(err)
}

func (rs *RefillSuite) TestManagerInitIsContinuable() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 1,
	}

	err := rs.mr.WriteSnapshot(rs.ctx, segKey, rs.etalonsData)
	rs.NoError(err)
	err = rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)
	rs.NoError(err)
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)

	err = rs.mr.Shutdown(rs.ctx)
	rs.NoError(err)

	mr, err := delivery.NewRefill(
		rs.cfg,
		2,
		rs.etalonBlockID,
		rs.etalonsNames...,
	)
	rs.NoError(err)
	rs.False(mr.IsContinuable())

	mr, err = delivery.NewRefill(
		rs.cfg,
		rs.etalonNumberOfShards,
		rs.etalonBlockID,
		rs.etalonsNames[:2]...,
	)
	rs.NoError(err)
	rs.False(mr.IsContinuable())
}

func (rs *RefillSuite) TestSegment() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 0,
	}

	rs.mr.WriteSegment(
		rs.ctx,
		segKey,
		rs.etalonsData,
	)

	actualSeg, err := rs.mr.Get(rs.ctx, segKey)
	rs.NoError(err)

	rs.Equal(rs.etalonsData.Bytes(), actualSeg.Bytes())
}

func (rs *RefillSuite) TestRestoreWithSegment() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 1,
	}

	rs.mr.WriteSnapshot(rs.ctx, segKey, rs.etalonsData)
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)
	segKey.Segment++
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)
	segKey.Segment++
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)

	actualSnap, actSegments, err := rs.mr.Restore(rs.ctx, segKey)
	rs.NoError(err)
	rs.Equal(3, len(actSegments))
	rs.Equal(rs.etalonsData.Bytes(), actualSnap.Bytes())
	rs.Equal(rs.etalonsData.Bytes(), actSegments[0].Bytes())
}

func (rs *RefillSuite) TestRestoreWithoutSegment() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 1,
	}

	rs.mr.WriteSnapshot(rs.ctx, segKey, rs.etalonsData)
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)

	actualSnap, actSegments, err := rs.mr.Restore(rs.ctx, segKey)
	rs.NoError(err)
	rs.Equal(0, len(actSegments))
	rs.Equal(rs.etalonsData.Bytes(), actualSnap.Bytes())
}

func (rs *RefillSuite) TestRestoreError() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	rs.mr.WriteSnapshot(rs.ctx, segKey, rs.etalonsData)
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)

	segKey.Segment = 1

	actualSnap, actSegments, err := rs.mr.Restore(rs.ctx, segKey)
	rs.Error(err)
	rs.ErrorIs(err, delivery.ErrSegmentNotFoundRefill)
	rs.Equal(0, len(actSegments))
	rs.Nil(actualSnap)
}

func (rs *RefillSuite) TestRestoreError_2() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 1,
	}

	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)
	segKey.Segment++
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)
	segKey.Segment++
	rs.mr.WriteSegment(rs.ctx, segKey, rs.etalonsData)

	actualSnap, actSegments, err := rs.mr.Restore(rs.ctx, segKey)
	rs.Error(err)
	rs.ErrorIs(err, delivery.ErrSegmentNotFoundRefill)
	rs.Equal(0, len(actSegments))
	rs.Nil(actualSnap)
}

func (rs *RefillSuite) TestWriteSegmentError() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	errSegKey := delivery.SegmentKey{
		ShardID: segKey.ShardID,
		Segment: segKey.Segment + 2,
	}

	err := rs.mr.WriteSnapshot(rs.ctx, segKey, rs.etalonsData)
	rs.NoError(err)
	err = rs.mr.WriteSegment(rs.ctx, errSegKey, rs.etalonsData)
	rs.ErrorIs(err, delivery.ErrSnapshotRequired)
}

func (rs *RefillSuite) TestAckStatus() {
	rs.T().Log("write ack status and check file not exist")
	err := rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.Error(err, "File not deleted")

	rs.T().Log("write segment, ack status and check file exist")
	err = rs.mr.WriteSegment(rs.ctx, delivery.SegmentKey{0, 0}, rs.etalonsData)
	rs.NoError(err)
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.NoError(err, "File deleted")

	rs.T().Log("ack segments for all name and check file deleted")
	for _, name := range rs.etalonsNames {
		rs.mr.Ack(delivery.SegmentKey{0, 0}, name)
		rs.mr.Ack(delivery.SegmentKey{0, 1}, name)
	}
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.Error(err, "File not deleted")
}

func (rs *RefillSuite) TestAckStatusWithReject() {
	rs.T().Log("write ack status and check file not exist")
	err := rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.Error(err, "File not deleted")

	rs.T().Log("write segment, ack status and check file exist")
	err = rs.mr.WriteSegment(rs.ctx, delivery.SegmentKey{0, 0}, rs.etalonsData)
	rs.NoError(err)
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.NoError(err, "File deleted")

	rs.T().Log("ack segments for all name and 1 reject and check file not deleted")
	for _, name := range rs.etalonsNames {
		rs.mr.Ack(delivery.SegmentKey{0, 0}, name)
		rs.mr.Ack(delivery.SegmentKey{0, 1}, name)
	}
	rs.mr.Reject(delivery.SegmentKey{0, 3}, rs.etalonsNames[0])
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.NoError(err, "File deleted")
}

func (rs *RefillSuite) TestAckStatusWithSnapshot() {
	rs.T().Log("init segKey")
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	rs.T().Log("write snapshot")
	err := rs.mr.WriteSnapshot(
		rs.ctx,
		segKey,
		rs.etalonsData,
	)
	rs.NoError(err)

	rs.T().Log("check file exist")
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.NoError(err, "File exist")

	rs.T().Log("write ack status")
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)

	rs.T().Log("check file exist")
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.NoError(err, "File exist")

	rs.T().Log("acked status for 0 shard for all name")
	for _, name := range rs.etalonsNames {
		rs.mr.Ack(delivery.SegmentKey{0, 0}, name)
		rs.mr.Ack(delivery.SegmentKey{0, 1}, name)
		rs.mr.Ack(delivery.SegmentKey{0, 2}, name)
	}

	rs.T().Log("write ack status")
	err = rs.mr.WriteAckStatus(rs.ctx)
	rs.NoError(err)

	rs.T().Log("check file does not exist, file must be deleted")
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.Error(err, "File not deleted")

	rs.T().Log("write again snapshot")
	err = rs.mr.WriteSnapshot(
		rs.ctx,
		segKey,
		rs.etalonsData,
	)
	rs.NoError(err)

	rs.T().Log("check file exist")
	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.cfg.FileName+".refill")))
	rs.NoError(err, "File exist")
}

func (rs *RefillSuite) TestRotate() {
	segKey := delivery.SegmentKey{
		ShardID: 0,
		Segment: 2,
	}

	err := rs.mr.WriteSnapshot(
		rs.ctx,
		segKey,
		rs.etalonsData,
	)
	rs.NoError(err)

	err = rs.mr.Rotate()
	rs.NoError(err)

	_, err = os.Stat(filepath.Clean(filepath.Join(rs.cfg.Dir, rs.etalonBlockID.String()+".refill")))
	rs.NoError(err, "File not rotated")
}
