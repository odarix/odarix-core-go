package delivery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/odarix/odarix-core-go/frames"
	"github.com/stretchr/testify/suite"
)

type MarkupFileSuite struct {
	suite.Suite

	cfg                     delivery.FileStorageConfig
	etalonNewFileName       string
	etalonsNames            []string
	etalonShardsNumberPower uint8
	etalonBlockID           uuid.UUID
	etalonsData             *dataTest
}

func TestMarkupFileSuite(t *testing.T) {
	suite.Run(t, new(MarkupFileSuite))
}

func (s *MarkupFileSuite) SetupSuite() {
	var err error
	s.etalonNewFileName = "blaUUID"

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

func (s *MarkupFileSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.cfg.Dir))
}

func (s *MarkupFileSuite) TestSegment() {
	ctx := context.Background()
	sm, err := delivery.NewStorageManager(
		s.cfg,
		s.etalonShardsNumberPower,
		s.etalonBlockID,
		nil,
		s.etalonsNames...,
	)
	s.Require().NoError(err)

	segKey := cppbridge.SegmentKey{
		ShardID: 0,
		Segment: 0,
	}
	err = sm.WriteSegment(ctx, segKey, s.etalonsData)
	s.Require().NoError(err)

	ok, err := sm.FileExist()
	s.Require().NoError(err)
	s.True(ok)

	segKey.Segment++
	err = sm.WriteSegment(ctx, segKey, s.etalonsData)
	s.Require().NoError(err)

	expectAckStatus := sm.GetAckStatus()
	expectAckStatus.Ack(cppbridge.SegmentKey{ShardID: 0, Segment: 0}, s.etalonsNames[0])
	err = sm.WriteAckStatus(ctx)
	s.NoError(err)

	err = sm.Close()
	s.Require().NoError(err)

	reader, err := delivery.NewFileStorage(s.cfg)
	s.Require().NoError(err)
	err = reader.OpenFile()
	s.Require().NoError(err)

	dnameID := expectAckStatus.GetNames().StringToID(s.etalonsNames[1])
	s.Require().NotEqual(frames.NotFoundName, dnameID)
	fe, err := frames.NewRefillShardEOFFrame(uint32(dnameID), segKey.ShardID)
	s.Require().NoError(err)
	size, err := reader.Size()
	s.Require().NoError(err)
	_, err = fe.WriteTo(reader.Writer(ctx, size))
	s.Require().NoError(err)

	m, err := delivery.NewMarkupReader(reader).ReadFile(ctx)
	s.Require().NoError(err)

	actualStatuses := m.GetCopyAckStatuses()
	s.Equal(expectAckStatus.GetCopyAckStatuses(), actualStatuses)
	s.Equal(s.etalonBlockID, m.BlockID())
	s.Equal(s.etalonsNames, m.DestinationsNames().ToString())
	s.Equal(uint8(1), m.EncodersVersion())
	s.Equal(s.etalonShardsNumberPower, m.ShardsNumberPower())

	m.RangeOnDestinationsEOF(
		func(dname string, shardID int) bool {
			s.Equal(s.etalonsNames[1], dname)
			s.Equal(segKey.ShardID, uint16(shardID))
			return true
		},
	)
}
