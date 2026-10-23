package client

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// Journal is the durable local record of command admission and unacknowledged
// events. The lock prevents two clients from executing from the same journal.
type Journal struct {
	db       *sql.DB
	lock     *flock.Flock
	mu       sync.Mutex
	streamID string
	path     string
	maxBytes int64
}

func OpenJournal(path string) (*Journal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("another client holds this journal lock")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	j := &Journal{db: db, lock: lock, path: path, maxBytes: 1 << 30}
	_, err = db.Exec(`PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS commands(id TEXT PRIMARY KEY,dispatch BLOB NOT NULL,state TEXT NOT NULL,pid INTEGER,result BLOB,finished_acked_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS cancellations(command_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS received(message_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pending(seq INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT UNIQUE NOT NULL,type TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS chunks(command_id TEXT NOT NULL,seq INTEGER NOT NULL,stream TEXT NOT NULL,data BLOB NOT NULL,PRIMARY KEY(command_id,seq));
CREATE TABLE IF NOT EXISTS reconciliation_history(id TEXT PRIMARY KEY,old_epoch TEXT NOT NULL,new_epoch TEXT NOT NULL,reason TEXT NOT NULL,created_at INTEGER NOT NULL,known_count INTEGER NOT NULL,orphan_count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS reconciliation_pending(reconciliation_id TEXT NOT NULL,seq INTEGER NOT NULL,message_id TEXT NOT NULL,type TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(reconciliation_id,seq));
CREATE TABLE IF NOT EXISTS reconciliation_records(reconciliation_id TEXT NOT NULL,command_id TEXT NOT NULL,outcome TEXT NOT NULL,PRIMARY KEY(reconciliation_id,command_id));
CREATE TABLE IF NOT EXISTS reconciliation_evidence(reconciliation_id TEXT NOT NULL,command_id TEXT NOT NULL,dispatch BLOB NOT NULL,state TEXT NOT NULL,pid INTEGER,result BLOB,finished_acked_at INTEGER NOT NULL,PRIMARY KEY(reconciliation_id,command_id));
CREATE TABLE IF NOT EXISTS reconciliation_chunks(reconciliation_id TEXT NOT NULL,command_id TEXT NOT NULL,seq INTEGER NOT NULL,stream TEXT NOT NULL,data BLOB NOT NULL,PRIMARY KEY(reconciliation_id,command_id,seq));
CREATE TABLE IF NOT EXISTS reconciliation_server_results(reconciliation_id TEXT NOT NULL,command_id TEXT NOT NULL,result BLOB NOT NULL,PRIMARY KEY(reconciliation_id,command_id));`)
	if err != nil {
		j.Close()
		return nil, err
	}
	var ackColumn int
	if err = db.QueryRow(`SELECT count(*) FROM pragma_table_info('commands') WHERE name='finished_acked_at'`).Scan(&ackColumn); err == nil && ackColumn == 0 {
		_, err = db.Exec(`ALTER TABLE commands ADD COLUMN finished_acked_at INTEGER NOT NULL DEFAULT 0`)
	}
	if err != nil {
		j.Close()
		return nil, err
	}
	_, err = db.Exec(`INSERT OR IGNORE INTO metadata(key,value) VALUES('stream_id',?)`, uuid.NewString())
	if err == nil {
		err = db.QueryRow(`SELECT value FROM metadata WHERE key='stream_id'`).Scan(&j.streamID)
	}
	if err != nil {
		j.Close()
		return nil, err
	}
	return j, nil
}
func (j *Journal) Close() error {
	err := j.db.Close()
	unlock := j.lock.Close()
	if err != nil {
		return err
	}
	return unlock
}
func (j *Journal) StreamID() string { return j.streamID }

// Capacity conservatively counts allocated database and WAL bytes, preserving
// room for terminal results and acknowledgements before accepting more output.
func (j *Journal) Capacity() error {
	var used int64
	for _, path := range []string{j.path, j.path + "-wal"} {
		info, err := os.Stat(path)
		if err == nil {
			used += info.Size()
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if used >= j.maxBytes-(16<<20) {
		return errors.New("local journal capacity reached; retain the journal and free or increase storage before new work")
	}
	return nil
}

// PruneOutputs preserves command deduplication records, unresolved output and
// unacknowledged events. Only server-acknowledged final output can expire.
func (j *Journal) PruneOutputs(before int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, err := j.db.Exec(`DELETE FROM chunks WHERE command_id IN (SELECT id FROM commands WHERE finished_acked_at>0 AND finished_acked_at<? AND state IN ('exited','cancelled','timed_out','not_started'))`, before)
	if err != nil {
		return err
	}
	if _, err = j.db.Exec(`PRAGMA incremental_vacuum`); err != nil {
		return err
	}
	_, err = j.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

func addPending(tx *sql.Tx, typ string, event model.CommandEvent) error {
	_, err := tx.Exec(`INSERT INTO pending(message_id,type,payload) VALUES(?,?,?)`, uuid.NewString(), typ, model.JSON(event))
	return err
}

// Admit records a dispatch before acknowledging delivery. A recovered or
// duplicate command is never automatically run again.
func (j *Journal) Admit(id string, dispatch model.Dispatch) (bool, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	tx, err := j.db.Begin()
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback()
	var existing []byte
	err = tx.QueryRow(`SELECT dispatch FROM commands WHERE id=?`, dispatch.Command.ID).Scan(&existing)
	if err == nil {
		var old model.Dispatch
		if err = json.Unmarshal(existing, &old); err != nil {
			return false, false, err
		}
		if old.Command.Command != dispatch.Command.Command || old.WorkspaceID != dispatch.WorkspaceID || old.Command.TaskID != dispatch.Command.TaskID || old.Command.Cwd != dispatch.Command.Cwd {
			return false, false, errors.New("command id conflicts with its durable journal record")
		}
		if id != "" {
			_, err = tx.Exec(`INSERT OR IGNORE INTO received(message_id) VALUES(?)`, id)
			if err != nil {
				return false, false, err
			}
		}
		return false, false, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, false, err
	}
	if err = j.Capacity(); err != nil {
		return false, false, err
	}
	var cancelled int
	if err = tx.QueryRow(`SELECT count(*) FROM cancellations WHERE command_id=?`, dispatch.Command.ID).Scan(&cancelled); err != nil {
		return false, false, err
	}
	state := "accepted"
	if cancelled > 0 {
		state = "not_started"
	}
	if _, err = tx.Exec(`INSERT INTO commands(id,dispatch,state) VALUES(?,?,?)`, dispatch.Command.ID, model.JSON(dispatch), state); err != nil {
		return false, false, err
	}
	if id != "" {
		if _, err = tx.Exec(`INSERT OR IGNORE INTO received(message_id) VALUES(?)`, id); err != nil {
			return false, false, err
		}
	}
	typ := "command.accepted"
	event := model.CommandEvent{CommandID: dispatch.Command.ID, State: state}
	if cancelled > 0 {
		typ = "command.finished"
		event.FinishedAt = model.Now()
		event.Error = commandError("cancelled_before_start", "the command was cancelled before local admission")
		if _, err = tx.Exec(`UPDATE commands SET result=? WHERE id=?`, model.JSON(event), dispatch.Command.ID); err != nil {
			return false, false, err
		}
	}
	if err = addPending(tx, typ, event); err != nil {
		return false, false, err
	}
	return cancelled == 0, cancelled > 0, tx.Commit()
}

func (j *Journal) Cancel(messageID, commandID string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT OR IGNORE INTO cancellations(command_id) VALUES(?)`, commandID); err != nil {
		return err
	}
	if messageID != "" {
		if _, err = tx.Exec(`INSERT OR IGNORE INTO received(message_id) VALUES(?)`, messageID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (j *Journal) Cancelled(commandID string) (bool, error) {
	var count int
	err := j.db.QueryRow(`SELECT count(*) FROM cancellations WHERE command_id=?`, commandID).Scan(&count)
	return count > 0, err
}
func (j *Journal) MarkStarting(commandID string) error {
	_, err := j.db.Exec(`UPDATE commands SET state='starting' WHERE id=? AND state='accepted'`, commandID)
	return err
}

func (j *Journal) Record(typ string, event model.CommandEvent, pid int) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if event.Chunk != nil {
		if err := j.Capacity(); err != nil {
			return err
		}
	}
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if event.Chunk != nil {
		chunk := event.Chunk
		if _, err = tx.Exec(`INSERT OR IGNORE INTO chunks(command_id,seq,stream,data) VALUES(?,?,?,?)`, chunk.CommandID, chunk.Seq, chunk.Stream, chunk.Data); err != nil {
			return err
		}
	} else {
		if _, err = tx.Exec(`UPDATE commands SET state=?,pid=?,result=? WHERE id=?`, event.State, pid, model.JSON(event), event.CommandID); err != nil {
			return err
		}
	}
	if err = addPending(tx, typ, event); err != nil {
		return err
	}
	return tx.Commit()
}

// Recover marks all in-flight records unknown. PID alone is not evidence that
// the original process is still alive, and is never used to kill on recovery.
func (j *Journal) Recover() error {
	rows, err := j.db.Query(`SELECT id FROM commands WHERE state IN ('accepted','starting','running')`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = j.Record("command.finished", model.CommandEvent{CommandID: id, State: "unknown", FinishedAt: model.Now(), Error: commandError("execution_unknown", "the client restarted before a durable terminal result; prior process termination is unconfirmed")}, 0); err != nil {
			return err
		}
	}
	return nil
}

func (j *Journal) Pending(limit int) ([]model.Envelope, error) {
	rows, err := j.db.Query(`SELECT seq,message_id,type,payload FROM pending ORDER BY seq LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var envelopes []model.Envelope
	for rows.Next() {
		e := model.Envelope{Version: model.ProtocolVersion, StreamID: j.streamID}
		if err = rows.Scan(&e.Seq, &e.ID, &e.Type, &e.Payload); err != nil {
			return nil, err
		}
		envelopes = append(envelopes, e)
	}
	return envelopes, rows.Err()
}
func (j *Journal) Ack(messageID string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var typ string
	var payload []byte
	err = tx.QueryRow(`SELECT type,payload FROM pending WHERE message_id=?`, messageID).Scan(&typ, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if typ == "command.finished" {
		var event model.CommandEvent
		if err = json.Unmarshal(payload, &event); err != nil {
			return err
		}
		if _, err = tx.Exec(`UPDATE commands SET finished_acked_at=? WHERE id=?`, model.Now(), event.CommandID); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`DELETE FROM pending WHERE message_id=?`, messageID); err != nil {
		return err
	}
	return tx.Commit()
}
func (j *Journal) SetServerEpoch(epoch string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	var old string
	err := j.db.QueryRow(`SELECT value FROM metadata WHERE key='server_epoch'`).Scan(&old)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if old != "" && old != epoch {
		return fmt.Errorf("server recovery epoch changed; keep this journal and reconcile before enrolling again")
	}
	_, err = j.db.Exec(`INSERT OR REPLACE INTO metadata(key,value) VALUES('server_epoch',?)`, epoch)
	return err
}
