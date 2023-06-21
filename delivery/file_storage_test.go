package delivery_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odarix/odarix-core-go/delivery"
)

type FileManagerSuite struct {
	suite.Suite

	etalonNewFileName string
	etalonsData       []byte
	cfg               *delivery.FileStorageConfig
	fm                *delivery.FileStorage
}

func TestFileManagerSuite(t *testing.T) {
	suite.Run(t, new(FileManagerSuite))
}

func (s *FileManagerSuite) SetupSuite() {
	var err error
	s.etalonNewFileName = "blablalbla1UUID"

	dir, err := os.MkdirTemp("", filepath.Clean("refill-"))
	s.Require().NoError(err)

	s.cfg = &delivery.FileStorageConfig{
		Dir:      dir,
		FileName: "current",
	}

	s.etalonsData = []byte{
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

	s.fm, err = delivery.NewFileStorage(s.cfg)
	s.Require().NoError(err)
}

func (s *FileManagerSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.cfg.Dir))
}

func (s *FileManagerSuite) TestOpenCloseFile() {
	ok, err := s.fm.FileExist()
	s.NoError(err)
	s.False(ok)

	err = s.fm.OpenFile()
	s.NoError(err)

	n, err := s.fm.WriteAt(context.Background(), s.etalonsData, io.SeekStart)
	s.NoError(err)
	s.Equal(len(s.etalonsData), n)

	_, err = s.fm.Seek(0, io.SeekStart)
	s.NoError(err)

	data := make([]byte, len(s.etalonsData))
	_, err = s.fm.Read(data)
	s.NoError(err)
	s.ElementsMatch(s.etalonsData, data)

	s.NoError(s.fm.Close())

	ok, err = s.fm.FileExist()
	s.NoError(err)
	s.True(ok)
}

func (s *FileManagerSuite) TestRename() {
	ok, err := s.fm.FileExist()
	s.NoError(err)
	s.False(ok)

	err = s.fm.OpenFile()
	s.NoError(err)

	err = s.fm.Close()
	s.NoError(err)

	ok, err = s.fm.FileExist()
	s.NoError(err)
	s.True(ok)

	err = s.fm.IntermediateRename(s.etalonNewFileName)
	s.NoError(err)

	_, err = os.Stat(filepath.Join(s.cfg.Dir, "current.refill"))
	s.Error(err)

	ok, err = s.fm.FileExist()
	s.NoError(err)
	s.True(ok)

	err = s.fm.Close()
	s.NoError(err)

	_, err = os.Stat(filepath.Join(s.cfg.Dir, s.etalonNewFileName+".tmprefill"))
	s.NoError(err)

	err = s.fm.Rename(s.etalonNewFileName)
	s.NoError(err)

	_, err = os.Stat(filepath.Join(s.cfg.Dir, s.etalonNewFileName+".refill"))
	s.NoError(err)
}

func (s *FileManagerSuite) TestDeleteCurrentFile() {
	ok, err := s.fm.FileExist()
	s.NoError(err)
	s.False(ok)

	err = s.fm.OpenFile()
	s.NoError(err)

	err = s.fm.Close()
	s.NoError(err)

	ok, err = s.fm.FileExist()
	s.NoError(err)
	s.True(ok)

	err = s.fm.DeleteCurrentFile()
	s.NoError(err)

	ok, err = s.fm.FileExist()
	s.NoError(err)
	s.False(ok)
}
