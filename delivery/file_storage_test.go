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

func (fms *FileManagerSuite) SetupSuite() {
	var err error
	fms.etalonNewFileName = "blablalbla1UUID"
	fms.cfg = &delivery.FileStorageConfig{
		Dir:      "/tmp/refill",
		FileName: "current",
	}

	fms.etalonsData = []byte{
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

	fms.fm, err = delivery.NewFileStorage(fms.cfg)
	fms.NoError(err)
}

func (fms *FileManagerSuite) TearDownTest() {
	err := os.RemoveAll(fms.cfg.Dir)
	fms.NoError(err)
}

func (fms *FileManagerSuite) TestOpenCloseFile() {
	ok, err := fms.fm.FileExist()
	fms.NoError(err)
	fms.False(ok)

	err = fms.fm.OpenFile()
	fms.NoError(err)

	n, err := fms.fm.WriteAt(context.Background(), fms.etalonsData, io.SeekStart)
	fms.NoError(err)
	fms.Equal(len(fms.etalonsData), n)

	_, err = fms.fm.Seek(0, io.SeekStart)
	fms.NoError(err)

	data := make([]byte, len(fms.etalonsData))
	_, err = fms.fm.Read(data)
	fms.NoError(err)
	fms.ElementsMatch(fms.etalonsData, data)

	err = fms.fm.Close()
	fms.NoError(err)

	ok, err = fms.fm.FileExist()
	fms.NoError(err)
	fms.True(ok)
}

func (fms *FileManagerSuite) TestRotate() {
	ok, err := fms.fm.FileExist()
	fms.NoError(err)
	fms.False(ok)

	err = fms.fm.OpenFile()
	fms.NoError(err)

	err = fms.fm.Close()
	fms.NoError(err)

	ok, err = fms.fm.FileExist()
	fms.NoError(err)
	fms.True(ok)

	err = fms.fm.Rotate(fms.etalonNewFileName)
	fms.NoError(err)

	ok, err = fms.fm.FileExist()
	fms.NoError(err)
	fms.False(ok)

	_, err = os.Stat(filepath.Clean(filepath.Join(fms.cfg.Dir, fms.etalonNewFileName+".refill")))
	fms.NoError(err)
}

func (fms *FileManagerSuite) TestDeleteCurrentFile() {
	ok, err := fms.fm.FileExist()
	fms.NoError(err)
	fms.False(ok)

	err = fms.fm.OpenFile()
	fms.NoError(err)

	err = fms.fm.Close()
	fms.NoError(err)

	ok, err = fms.fm.FileExist()
	fms.NoError(err)
	fms.True(ok)

	err = fms.fm.DeleteCurrentFile()
	fms.NoError(err)

	ok, err = fms.fm.FileExist()
	fms.NoError(err)
	fms.False(ok)
}
