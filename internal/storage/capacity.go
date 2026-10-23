package storage

import (
	"github.com/Zxilly/task-bridge/internal/model"
	"os"
	"path/filepath"
)

const ReservedBytes int64 = 16 << 20

func (s *Store) Usage() int64 {
	var total int64
	for _, path := range []string{s.Path, s.Path + "-wal"} {
		if v, e := os.Stat(path); e == nil {
			total += v.Size()
		}
	}
	return total
}

// Admission reserves room for terminal records. It never acknowledges an
// uncommitted terminal event; an external disk failure remains retryable at B.
func (s *Store) Admission() error {
	return s.admission(s.Usage())
}
func (s *Store) AdmissionIn(tx *Tx) error {
	var free, size int64
	if e := tx.QueryRowContext(tx.ctx, "PRAGMA freelist_count").Scan(&free); e != nil {
		return e
	}
	if e := tx.QueryRowContext(tx.ctx, "PRAGMA page_size").Scan(&size); e != nil {
		return e
	}
	return s.admission(max(int64(0), s.Usage()-free*size))
}
func (s *Store) admission(used int64) error {
	if s.MaxBytes > 0 && used >= s.MaxBytes-ReservedBytes {
		return model.Err("storage_full", "database capacity reserved for existing command results")
	}
	free, e := freeBytes(filepath.Dir(s.Path))
	if e != nil {
		return e
	}
	if free < uint64(2*ReservedBytes) {
		return model.Err("storage_full", "insufficient free disk space for new work")
	}
	return nil
}
func (t *Tx) PruneOutput(before int64) error {
	r, e := t.QueryContext(t.ctx, `SELECT commands.payload FROM commands JOIN tasks ON commands.task=tasks.id WHERE commands.state IN ('exited','cancelled','timed_out','not_started') AND tasks.state IN ('succeeded','failed','cancelled','timed_out','abandoned') AND json_extract(commands.payload,'$.finished_at')>0 AND json_extract(commands.payload,'$.finished_at')<? AND json_extract(commands.payload,'$.output_bytes')>0 LIMIT 25`, before)
	if e != nil {
		return e
	}
	cs, e := scanList[model.Command](r)
	if e != nil {
		return e
	}
	for _, c := range cs {
		if _, e = t.ExecContext(t.ctx, "DELETE FROM command_chunks WHERE command=?", c.ID); e != nil {
			return e
		}
		c.DroppedBytes += c.OutputBytes
		c.OutputBytes = 0
		c.Truncated = true
		if e = t.SaveCommand(c); e != nil {
			return e
		}
	}
	return nil
}
