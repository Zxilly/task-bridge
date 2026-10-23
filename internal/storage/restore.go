package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
)

// RestoreNew restores into a new database path, leaving both the live database
// and the original backup recoverable. The staged image is validated, fenced,
// and stripped of device credentials before it becomes visible at destination.
func RestoreNew(ctx context.Context, input, destination string) error {
	destination, e := filepath.Abs(destination)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(destination), 0700); e != nil {
		return e
	}
	lock := flock.New(destination + ".lock")
	ok, e := lock.TryLock()
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("destination database is in use")
	}
	defer lock.Unlock()
	if _, e = os.Stat(destination); !os.IsNotExist(e) {
		return errors.New("restore destination must be a new database path; preserve the previous database separately")
	}
	in, e := os.Open(input)
	if e != nil {
		return e
	}
	defer in.Close()
	staged := destination + ".restore-" + uuid.NewString()
	out, e := os.OpenFile(staged, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer os.Remove(staged)
	if _, e = io.Copy(out, in); e != nil {
		out.Close()
		return e
	}
	if e = out.Sync(); e != nil {
		out.Close()
		return e
	}
	if e = out.Close(); e != nil {
		return e
	}
	s, e := Open(ctx, staged)
	if e != nil {
		return e
	}
	defer os.Remove(staged + ".lock")
	if e = s.Integrity(ctx); e == nil {
		e = s.ResetRestore(ctx)
	}
	closeErr := s.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(staged, destination)
}
