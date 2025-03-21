package cppbridge_test

import (
	"testing"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/model"
	"github.com/stretchr/testify/suite"
)

type HeadSuite struct {
	suite.Suite
	lss         *cppbridge.LabelSetStorage
	dataStorage *cppbridge.HeadDataStorage
	encoder     *cppbridge.HeadEncoder
}

func TestHeadSuite(t *testing.T) {
	suite.Run(t, new(HeadSuite))
}

func (s *HeadSuite) SetupTest() {
	s.lss = cppbridge.NewQueryableLssStorage()
	s.dataStorage = cppbridge.NewHeadDataStorage()
	s.encoder = cppbridge.NewHeadEncoderWithDataStorage(s.dataStorage)
}

func (s *HeadSuite) TestChunkRecoder() {
	// Arrange
	s.lss.FindOrEmplace(model.NewLabelSetBuilder().Set("job", "1").Build())
	s.lss.FindOrEmplace(model.NewLabelSetBuilder().Set("job", "2").Build())

	s.encoder.Encode(0, 1, 1.0)
	s.encoder.Encode(0, 2, 1.0)
	s.encoder.Encode(1, 3, 2.0)
	s.encoder.Encode(1, 4, 2.0)
	recoder := cppbridge.NewChunkRecoder(s.lss, s.dataStorage, cppbridge.TimeInterval{MinT: 0, MaxT: 5})

	// Act
	chunk2 := recoder.RecodeNextChunk()
	chunk2.ChunkData = append([]byte(nil), chunk2.ChunkData...)
	chunk4 := recoder.RecodeNextChunk()

	// Assert
	s.Equal(cppbridge.RecodedChunk{
		TimeInterval: cppbridge.TimeInterval{
			MinT: 1,
			MaxT: 2,
		},
		SamplesCount: 2,
		SeriesId:     0,
		HasMoreData:  true,
		ChunkData:    []byte{0x00, 0x02, 0x02, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
	}, chunk2)
	s.Equal(cppbridge.RecodedChunk{
		TimeInterval: cppbridge.TimeInterval{
			MinT: 3,
			MaxT: 4,
		},
		SamplesCount: 2,
		SeriesId:     1,
		HasMoreData:  false,
		ChunkData:    []byte{0x00, 0x02, 0x06, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
	}, chunk4)
}

func (s *HeadSuite) TestTimeInterval() {
	// Arrange
	dataStorage := cppbridge.NewHeadDataStorage()
	encoder := cppbridge.NewHeadEncoderWithDataStorage(dataStorage)
	encoder.Encode(0, 1, 1.0)
	encoder.Encode(0, 2, 1.0)
	encoder.Encode(1, 2, 1.0)
	encoder.Encode(1, 3, 1.0)

	// Act
	time_interval := dataStorage.TimeInterval()

	// Assert
	s.Equal(cppbridge.TimeInterval{MinT: 1, MaxT: 3}, time_interval)
}
