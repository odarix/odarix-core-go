package delivery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// refillExtension - file for refill extension.
	refillExtension = ".refill"
	// refillTmpExtension - file for temporary refill extension.
	refillTmpExtension = ".tmprefill"
)

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
		fileName: cfg.FileName + refillExtension,
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
	return filepath.Join(fs.dir, fs.fileName)
}

// TemporarilyRename - rename the current file to blockID with temporary
// extension for further conversion to refill.
func (fs *FileStorage) TemporarilyRename(name string) error {
	if ok, err := fs.FileExist(); !ok {
		return err
	}

	if err := os.Rename(fs.GetPath(), filepath.Join(fs.dir, name+refillTmpExtension)); err != nil {
		return err
	}

	fs.fileName = name + refillTmpExtension

	return nil
}

// StatefulRename - change extension the current file for further conversion to refill.
func (fs *FileStorage) StatefulRename() error {
	if ok, err := fs.FileExist(); !ok {
		return err
	}

	newName := strings.TrimSuffix(fs.GetPath(), refillTmpExtension) + refillExtension
	if err := os.Rename(fs.GetPath(), filepath.Clean(newName)); err != nil {
		return err
	}

	fs.fileName = newName

	return nil
}

// DeleteCurrentFile - delete current file.
func (fs *FileStorage) DeleteCurrentFile() error {
	if err := fs.Close(); err != nil {
		return err
	}

	return os.Remove(fs.GetPath())
}

// Truncate - changes the size of the file.
func (fs *FileStorage) Truncate() error {
	if err := fs.fileDescriptor.Truncate(0); err != nil {
		return err
	}

	if _, err := fs.fileDescriptor.Seek(0, 0); err != nil {
		return err
	}

	return nil
}
