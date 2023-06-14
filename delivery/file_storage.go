package delivery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// fileExtension - file extension.
const fileExtension = ".refill"

// FileStorageConfig - config for FileStorage.
type FileStorageConfig struct {
	Dir      string
	FileName string
}

// FileStorage - wrapper for worker with file.
type FileStorage struct {
	fileDescriptor *os.File
	dir            string
	fileName       string
}

// NewFileStorage - init new FileStorage.
func NewFileStorage(cfg *FileStorageConfig) (*FileStorage, error) {
	return &FileStorage{
		dir:      cfg.Dir,
		fileName: cfg.FileName + fileExtension,
	}, nil
}

// OpenFile - open or create new file.
func (fs *FileStorage) OpenFile() error {
	//revive:disable-next-line:add-constant file permissions simple readable as octa-number
	if err := os.MkdirAll(fs.dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(fs.fileName), err)
	}

	f, err := os.OpenFile(
		fs.GetPath(),
		os.O_CREATE|os.O_RDWR|os.O_SYNC,
		//revive:disable-next-line:add-constant file permissions simple readable as octa-number
		0o600,
	)
	if err != nil {
		return fmt.Errorf("open file %s: %w", fs.fileName, err)
	}

	fs.fileDescriptor = f

	return nil
}

// WriteAt wirtes data to file and flush it
func (fs *FileStorage) WriteAt(ctx context.Context, b []byte, off int64) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = fs.fileDescriptor.SetWriteDeadline(deadline)
	} else {
		_ = fs.fileDescriptor.SetWriteDeadline(time.Time{})
	}
	n, err := fs.fileDescriptor.WriteAt(b, off)
	if err != nil {
		return n, err
	}
	return n, fs.fileDescriptor.Sync()
}

// Seek - implements Seek of reader.
func (fs *FileStorage) Seek(offset int64, whence int) (int64, error) {
	return fs.fileDescriptor.Seek(offset, whence)
}

// Read - implements io.Read.
func (fs *FileStorage) Read(data []byte) (int, error) {
	return fs.fileDescriptor.Read(data)
}

// ReadAt - implements io.ReadAt.
func (fs *FileStorage) ReadAt(b []byte, off int64) (int, error) {
	return fs.fileDescriptor.ReadAt(b, off)
}

// Close - implements os.Close
func (fs *FileStorage) Close() error {
	if fs.fileDescriptor == nil {
		return nil
	}

	err := fs.fileDescriptor.Close()
	if err != nil {
		return err
	}

	fs.fileDescriptor = nil

	return nil
}

// FileExist - check file exist.
func (fs *FileStorage) FileExist() (bool, error) {
	_, err := os.Stat(fs.GetPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// GetPath - return path to current file.
func (fs *FileStorage) GetPath() string {
	return filepath.Clean(filepath.Join(fs.dir, fs.fileName))
}

// Rotate - rename the current file to blockID for further conversion to refill.
func (fs *FileStorage) Rotate(name string) error {
	if err := fs.Close(); err != nil {
		return err
	}
	return os.Rename(fs.GetPath(), filepath.Clean(filepath.Join(fs.dir, name+fileExtension)))
}

// DeleteCurrentFile - delete current file.
func (fs *FileStorage) DeleteCurrentFile() error {
	if err := fs.Close(); err != nil {
		return err
	}

	return os.Remove(fs.GetPath())
}
