// Package storage owns the SQLite schema and short, durable transactions.
package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	DB       *sql.DB
	lock     *flock.Flock
	Epoch    string
	Path     string
	MaxBytes int64
}
type Tx struct {
	*sql.Tx
	ctx context.Context
}

func Open(ctx context.Context, path string) (s *Store, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, model.Err("database_in_use", "broker or offline operation already holds database lock")
	}
	defer func() {
		if err != nil {
			_ = lock.Unlock()
		}
	}()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	db.SetMaxOpenConns(1)
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			return nil, err
		}
	}
	fsys, err := subMigrations()
	if err != nil {
		return nil, err
	}
	p, err := goose.NewProvider(goose.DialectSQLite3, db, fsys)
	if err != nil {
		return nil, err
	}
	if _, err = p.Up(ctx); err != nil {
		return nil, err
	}
	s = &Store{DB: db, lock: lock, Path: path, MaxBytes: 2 << 30}
	err = s.Update(ctx, func(tx *Tx) error {
		var e error
		s.Epoch, e = tx.Meta("server_epoch")
		if errors.Is(e, sql.ErrNoRows) {
			s.Epoch = uuid.NewString()
			return tx.SetMeta("server_epoch", s.Epoch)
		}
		return e
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error {
	err := s.DB.Close()
	unlock := s.lock.Unlock()
	if err != nil {
		return err
	}
	return unlock
}
func (s *Store) Update(ctx context.Context, fn func(*Tx) error) error {
	conn, e := s.DB.Conn(ctx)
	if e != nil {
		return e
	}
	// A canceled transaction can roll back in database/sql's goroutine.
	// Close waits for that rollback to release the connection before returning.
	defer conn.Close()
	t, e := conn.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer t.Rollback()
	if e = fn(&Tx{t, ctx}); e != nil {
		return e
	}
	return t.Commit()
}
func (s *Store) View(ctx context.Context, fn func(*Tx) error) error { return s.Update(ctx, fn) }
func (t *Tx) Meta(key string) (string, error) {
	var v string
	e := t.QueryRowContext(t.ctx, "SELECT value FROM metadata WHERE key=?", key).Scan(&v)
	return v, e
}
func (t *Tx) SetMeta(key, value string) error {
	_, e := t.ExecContext(t.ctx, "INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return e
}
func Hash(s string) string { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
func decode[T any](row *sql.Row) (T, error) {
	var v T
	var b []byte
	e := row.Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e
}
func scanList[T any](rows *sql.Rows) ([]T, error) {
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		var b []byte
		var v T
		if e := rows.Scan(&b); e != nil {
			return nil, e
		}
		if e := json.Unmarshal(b, &v); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *Tx) Principal(id string) (model.Principal, error) {
	p, e := decode[model.Principal](t.QueryRowContext(t.ctx, "SELECT payload FROM principals WHERE id=?", id))
	return p, e
}
func (t *Tx) Authenticate(token string) (model.Principal, error) {
	p, e := decode[model.Principal](t.QueryRowContext(t.ctx, "SELECT payload FROM principals WHERE token_hash=?", Hash(token)))
	if e != nil || p.Disabled {
		return p, model.Err("unauthorized", "invalid or revoked device credential")
	}
	return p, nil
}
func (t *Tx) SavePrincipal(p model.Principal) error {
	if p.TokenHash == "" {
		_, e := t.ExecContext(t.ctx, "UPDATE principals SET payload=? WHERE id=?", model.JSON(p), p.ID)
		return e
	}
	_, e := t.ExecContext(t.ctx, "INSERT INTO principals(id,token_hash,payload) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", p.ID, p.TokenHash, model.JSON(p))
	return e
}
func (t *Tx) Principals() ([]model.Principal, error) {
	r, e := t.QueryContext(t.ctx, "SELECT payload FROM principals ORDER BY id")
	if e != nil {
		return nil, e
	}
	return scanList[model.Principal](r)
}
func (t *Tx) Workspace(id string) (model.Workspace, error) {
	return decode[model.Workspace](t.QueryRowContext(t.ctx, "SELECT payload FROM workspaces WHERE id=?", id))
}
func (t *Tx) SaveWorkspace(w model.Workspace) error {
	_, e := t.ExecContext(t.ctx, "INSERT INTO workspaces(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", w.ID, model.JSON(w))
	return e
}
func (t *Tx) Task(id string) (model.Task, error) {
	var v model.Task
	var b []byte
	var h string
	e := t.QueryRowContext(t.ctx, "SELECT payload,request_hash FROM tasks WHERE id=?", id).Scan(&b, &h)
	if e == nil {
		e = json.Unmarshal(b, &v)
		v.RequestHash = h
	}
	return v, e
}
func (t *Tx) TaskByKey(owner, key string) (model.Task, error) {
	var id string
	e := t.QueryRowContext(t.ctx, "SELECT id FROM tasks WHERE requester=? AND idem=?", owner, key).Scan(&id)
	if e != nil {
		return model.Task{}, e
	}
	return t.Task(id)
}
func (t *Tx) Tasks() ([]model.Task, error) {
	r, e := t.QueryContext(t.ctx, "SELECT payload FROM tasks ORDER BY created,id")
	if e != nil {
		return nil, e
	}
	return scanList[model.Task](r)
}
func (t *Tx) ActiveTasks() ([]model.Task, error) {
	r, e := t.QueryContext(t.ctx, "SELECT payload FROM tasks WHERE state IN ('queued','offered','running','interrupted','cancel_requested') ORDER BY created,id")
	if e != nil {
		return nil, e
	}
	return scanList[model.Task](r)
}
func (t *Tx) TaskPage(executor, workspace string, cursor int64, limit int) ([]model.Task, int64, error) {
	r, e := t.QueryContext(t.ctx, "SELECT rowid,payload FROM tasks WHERE json_extract(payload,'$.executor_id')=? AND (?='' OR workspace=?) AND rowid>? ORDER BY rowid LIMIT ?", executor, workspace, workspace, cursor, limit)
	if e != nil {
		return nil, 0, e
	}
	defer r.Close()
	out := []model.Task{}
	next := cursor
	for r.Next() {
		var raw []byte
		var v model.Task
		if e = r.Scan(&next, &raw); e != nil {
			return nil, 0, e
		}
		if e = json.Unmarshal(raw, &v); e != nil {
			return nil, 0, e
		}
		out = append(out, v)
	}
	return out, next, r.Err()
}
func (t *Tx) SaveTask(v model.Task) error {
	v.Revision++
	v.UpdatedAt = model.Now()
	_, e := t.ExecContext(t.ctx, "INSERT INTO tasks(id,requester,idem,request_hash,workspace,state,created,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,payload=excluded.payload", v.ID, v.RequesterID, v.IdempotencyKey, v.RequestHash, v.WorkspaceID, v.State, v.CreatedAt, model.JSON(v))
	if e != nil {
		return e
	}
	if v.AssignmentID != "" {
		_, e = t.ExecContext(t.ctx, "INSERT INTO assignments(id,task,agent,connection,epoch,lease_until) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until", v.AssignmentID, v.ID, v.AgentID, v.ConnectionID, v.LeaseEpoch, v.LeaseUntil)
	}
	return e
}
func (t *Tx) Command(id string) (model.Command, error) {
	var v model.Command
	var b []byte
	var h string
	e := t.QueryRowContext(t.ctx, "SELECT payload,request_hash FROM commands WHERE id=?", id).Scan(&b, &h)
	if e == nil {
		e = json.Unmarshal(b, &v)
		v.RequestHash = h
	}
	return v, e
}
func (t *Tx) CommandByKey(task, key string) (model.Command, error) {
	var id string
	e := t.QueryRowContext(t.ctx, "SELECT id FROM commands WHERE task=? AND operation_key=?", task, key).Scan(&id)
	if e != nil {
		return model.Command{}, e
	}
	return t.Command(id)
}
func (t *Tx) Commands(task string) ([]model.Command, error) {
	r, e := t.QueryContext(t.ctx, "SELECT payload FROM commands WHERE task=? ORDER BY rowid", task)
	if e != nil {
		return nil, e
	}
	return scanList[model.Command](r)
}
func (t *Tx) SaveCommand(v model.Command) error {
	_, e := t.ExecContext(t.ctx, "INSERT INTO commands(id,task,operation_key,request_hash,state,payload) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,payload=excluded.payload", v.ID, v.TaskID, v.OperationKey, v.RequestHash, v.State, model.JSON(v))
	return e
}
func (t *Tx) AddChunk(c model.Chunk) (bool, error) {
	return t.AddChunkWithDigest(c, ChunkDigest(c))
}
func ChunkDigest(c model.Chunk) string              { return Hash(c.Stream + "\x00" + string(c.Data)) }
func (t *Tx) ChunkSeen(c model.Chunk) (bool, error) { return t.chunkSeen(c, ChunkDigest(c)) }
func (t *Tx) chunkSeen(c model.Chunk, digest string) (bool, error) {
	var old, stream string
	var data []byte
	e := t.QueryRowContext(t.ctx, "SELECT digest,stream,data FROM command_chunks WHERE command=? AND seq=?", c.CommandID, c.Seq).Scan(&old, &stream, &data)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if old == "" {
		old = Hash(stream + "\x00" + string(data))
	}
	if old != digest {
		return false, model.Err("chunk_conflict", "chunk sequence reused with different bytes")
	}
	return true, nil
}
func (t *Tx) AddChunkWithDigest(c model.Chunk, digest string) (bool, error) {
	seen, e := t.chunkSeen(c, digest)
	if e != nil || seen {
		return false, e
	}
	r, e := t.ExecContext(t.ctx, "INSERT INTO command_chunks(command,seq,stream,data,digest) VALUES(?,?,?,?,?)", c.CommandID, c.Seq, c.Stream, c.Data, digest)
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n != 0, e
}
func (t *Tx) Chunks(command string, after int64, limit int) ([]model.Chunk, error) {
	r, e := t.QueryContext(t.ctx, "SELECT seq,stream,data FROM command_chunks WHERE command=? AND seq>? ORDER BY seq LIMIT ?", command, after, limit)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := []model.Chunk{}
	for r.Next() {
		c := model.Chunk{CommandID: command}
		if e = r.Scan(&c.Seq, &c.Stream, &c.Data); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, r.Err()
}

// BytePage applies the byte cursor in SQLite. Small chunks cannot strand a
// cursor beyond a fixed first-page scan, and only a bounded page enters memory.
func (t *Tx) BytePage(command string, cursor int64) ([]model.Chunk, []int64, error) {
	r, e := t.QueryContext(t.ctx, `SELECT seq,stream,data,end_offset-length(data) FROM (
	 SELECT seq,stream,data,SUM(length(data)) OVER (ORDER BY seq) AS end_offset
	 FROM command_chunks WHERE command=?) WHERE end_offset>? ORDER BY seq LIMIT 16`, command, cursor)
	if e != nil {
		return nil, nil, e
	}
	defer r.Close()
	var chunks []model.Chunk
	var offsets []int64
	for r.Next() {
		c := model.Chunk{CommandID: command}
		var offset int64
		if e = r.Scan(&c.Seq, &c.Stream, &c.Data, &offset); e != nil {
			return nil, nil, e
		}
		chunks = append(chunks, c)
		offsets = append(offsets, offset)
	}
	return chunks, offsets, r.Err()
}
func (t *Tx) Event(task, kind string, payload model.EventPayload) error {
	_, e := t.ExecContext(t.ctx, "INSERT INTO events(task,type,payload,created) VALUES(?,?,?,?)", task, kind, model.JSON(payload), model.Now())
	return e
}
func (t *Tx) Events(task string, after int64, limit int) (model.EventList, error) {
	r, e := t.QueryContext(t.ctx, "SELECT seq,type,payload,created FROM events WHERE task=? AND seq>? ORDER BY seq LIMIT ?", task, after, limit)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := model.EventList{}
	for r.Next() {
		v := model.Event{TaskID: task}
		if e = r.Scan(&v.Seq, &v.Type, &v.Payload, &v.CreatedAt); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, r.Err()
}
func (t *Tx) Enqueue(peer, kind string, payload model.OutboxPayload) error {
	_, e := t.ExecContext(t.ctx, "INSERT INTO outbox(peer,message_id,type,payload) VALUES(?,?,?,?)", peer, uuid.NewString(), kind, model.JSON(payload))
	return e
}
func (t *Tx) Outbox(peer string) ([]model.Envelope, error) {
	r, e := t.QueryContext(t.ctx, "SELECT seq,message_id,type,payload FROM outbox WHERE peer=? AND acked=0 ORDER BY CASE WHEN type='command.cancel' THEN 0 ELSE 1 END,seq LIMIT 128", peer)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := []model.Envelope{}
	for r.Next() {
		v := model.Envelope{Version: 1}
		if e = r.Scan(&v.Seq, &v.ID, &v.Type, &v.Payload); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, r.Err()
}
func (t *Tx) Ack(peer, id string) error {
	_, e := t.ExecContext(t.ctx, "UPDATE outbox SET acked=1 WHERE peer=? AND message_id=?", peer, id)
	return e
}
func (t *Tx) Receive(peer, id, digest string) (bool, error) {
	var old string
	e := t.QueryRowContext(t.ctx, "SELECT digest FROM inbox WHERE peer=? AND message_id=?", peer, id).Scan(&old)
	if e == nil {
		if old != digest {
			return false, model.Err("message_conflict", "reused message id has different payload")
		}
		return false, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return false, e
	}
	_, e = t.ExecContext(t.ctx, "INSERT INTO inbox(peer,message_id,digest) VALUES(?,?,?)", peer, id, digest)
	return true, e
}
func (t *Tx) Resolution(target, outcome, reason, actor string) error {
	_, e := t.ExecContext(t.ctx, "INSERT INTO resolutions(target,outcome,reason,actor,created) VALUES(?,?,?,?,?)", target, outcome, reason, actor, model.Now())
	return e
}
func (s *Store) Backup(ctx context.Context, path string) error {
	path, e := filepath.Abs(path)
	if e != nil {
		return e
	}
	if path == s.Path {
		return errors.New("backup cannot overwrite live database")
	}
	if _, e = os.Stat(path); !os.IsNotExist(e) {
		return errors.New("backup output must be a new path")
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	_, e = s.DB.ExecContext(ctx, "VACUUM INTO '"+strings.ReplaceAll(path, "'", "''")+"'")
	return e
}
func (s *Store) Integrity(ctx context.Context) error {
	var v string
	if e := s.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&v); e != nil {
		return e
	}
	if v != "ok" {
		return fmt.Errorf("integrity_check: %s", v)
	}
	return nil
}
func (s *Store) ResetRestore(ctx context.Context) error {
	epoch := uuid.NewString()
	e := s.Update(ctx, func(tx *Tx) error {
		if e := tx.SetMeta("server_epoch", epoch); e != nil {
			return e
		}
		ps, e := tx.Principals()
		if e != nil {
			return e
		}
		for _, p := range ps {
			p.Disabled = true
			if e = tx.SavePrincipal(p); e != nil {
				return e
			}
		}
		tasks, e := tx.Tasks()
		if e != nil {
			return e
		}
		for _, v := range tasks {
			if !v.Terminal() {
				v.State = "interrupted"
				v.LeaseUntil = 0
				if e = tx.FailTaskResume(&v, "broker_restored", "the broker snapshot was restored; reconcile execution before requesting another attempt", "pending", "accepted"); e != nil {
					return e
				}
				if e = tx.SaveTask(v); e != nil {
					return e
				}
				w, e := tx.Workspace(v.WorkspaceID)
				if e != nil {
					return e
				}
				w.Quarantined = true
				w.QuarantineReason = "restored snapshot requires executor journal reconciliation"
				if e = tx.SaveWorkspace(w); e != nil {
					return e
				}
				cs, e := tx.Commands(v.ID)
				if e != nil {
					return e
				}
				for _, c := range cs {
					if !c.Terminal() {
						c.State = "unknown"
						if e = tx.SaveCommand(c); e != nil {
							return e
						}
					}
				}
			}
		}
		_, e = tx.ExecContext(ctx, "DELETE FROM outbox; DELETE FROM peer_cursors;")
		return e
	})
	if e == nil {
		s.Epoch = epoch
	}
	return e
}
