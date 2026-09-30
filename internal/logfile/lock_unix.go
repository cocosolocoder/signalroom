//go:build !windows

package logfile

import (
	"errors"
	"os"
	"syscall"
)

var errLocked = errors.New("directory lock held by another process")

// tryLock acquires an exclusive, non-blocking flock on the data directory.
// The kernel releases it automatically on exit or if the process is killed,
// so a later instance can take over immediately.
func tryLock(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return errLocked
		}
		return err
	}
	return nil
}

func unlock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

// syncDir flushes directory entry creation so the log file itself is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
