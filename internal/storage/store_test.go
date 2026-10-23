package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

func testStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenEnforcesDurabilityForeignKeysAndSingleWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bridge.db")
	s := testStore(t, path)
	var mode string
	if err := s.DB.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal mode = %q, error = %v", mode, err)
	}
	for query, want := range map[string]int{"PRAGMA synchronous": 2, "PRAGMA foreign_keys": 1} {
		var got int
		if err := s.DB.QueryRowContext(ctx, query).Scan(&got); err != nil || got != want {
			t.Fatalf("%s = %d, want %d; error = %v", query, got, want, err)
		}
	}
	err := s.Update(ctx, func(tx *Tx) error {
		return tx.SaveCommand(model.Command{ID: "orphan", TaskID: "missing-task", OperationKey: "orphan", State: "dispatched"})
	})
	if err == nil {
		t.Fatal("an orphan command bypassed the foreign key constraint")
	}
	other, err := Open(ctx, path)
	if err == nil {
		_ = other.Close()
		t.Fatal("a second store acquired the live database")
	}
	var domainError *model.Error
	if !errors.As(err, &domainError) || domainError.Code != "database_in_use" {
		t.Fatalf("second opener error = %v, want database_in_use", err)
	}
	if err := s.Integrity(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBackupIsConsistentDuringLiveWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	s := testStore(t, filepath.Join(root, "live.db"))
	writerCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	started := make(chan struct{})
	finished := make(chan error, 1)
	var committed atomic.Int64
	go func() {
		for value := int64(1); ; value++ {
			if writerCtx.Err() != nil {
				finished <- nil
				return
			}
			err := s.Update(writerCtx, func(tx *Tx) error {
				if err := tx.SetMeta("backup-left", strconv.FormatInt(value, 10)); err != nil {
					return err
				}
				return tx.SetMeta("backup-right", strconv.FormatInt(value, 10))
			})
			if err != nil {
				if writerCtx.Err() != nil {
					finished <- nil
				} else {
					finished <- err
				}
				return
			}
			committed.Store(value)
			if value == 1 {
				close(started)
			}
		}
	}()
	select {
	case <-started:
	case err := <-finished:
		t.Fatalf("writer stopped before its first commit: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	backupPath := filepath.Join(root, "snapshots", "bridge's backup.db")
	backupErr := s.Backup(ctx, backupPath)
	stopWriter()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if backupErr != nil {
		t.Fatal(backupErr)
	}
	if err := s.Update(ctx, func(tx *Tx) error { return tx.SetMeta("after-backup", "live-only") }); err != nil {
		t.Fatal(err)
	}
	backup := testStore(t, backupPath)
	if err := backup.Integrity(ctx); err != nil {
		t.Fatal(err)
	}
	if backup.Epoch != s.Epoch {
		t.Fatal("an ordinary backup changed the server epoch")
	}
	if err := backup.View(ctx, func(tx *Tx) error {
		left, err := tx.Meta("backup-left")
		if err != nil {
			return err
		}
		right, err := tx.Meta("backup-right")
		if err != nil {
			return err
		}
		if left != right {
			return fmt.Errorf("torn backup transaction: left=%q, right=%q", left, right)
		}
		value, err := strconv.ParseInt(left, 10, 64)
		if err != nil || value < 1 || value > committed.Load() {
			return fmt.Errorf("backup counter %q outside committed range 1..%d: %v", left, committed.Load(), err)
		}
		if _, err := tx.Meta("after-backup"); !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("backup includes later writes, or lookup failed: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(ctx, backupPath); err == nil {
		t.Fatal("backup overwrote an existing snapshot")
	}
}

func TestRestoreNewFencesDevicesAndPreservesOriginals(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	live := testStore(t, filepath.Join(root, "live.db"))
	const executorToken = "test-executor-token"
	const agentToken = "test-agent-token"
	final := model.Task{
		ID: "complete-task", RequesterID: "executor", WorkspaceID: "demo", ExecutorID: "laptop",
		IdempotencyKey: "complete-key", State: "succeeded", CreatedAt: model.Now(),
		Summary: "durable result", Result: model.JSON(map[string]any{"answer": "你好", "value": 42}),
	}
	running := model.Task{
		ID: "running-task", RequesterID: "executor", WorkspaceID: "demo", ExecutorID: "laptop",
		IdempotencyKey: "running-key", State: "running", CreatedAt: model.Now(),
		AssignmentID: "assignment", AgentID: "claude-demo", ConnectionID: "connection", LeaseEpoch: 4,
		LeaseUntil: model.Now() + 60000,
	}
	queued := []model.Task{
		{ID: "queued-task-1", RequesterID: "executor", WorkspaceID: "demo", ExecutorID: "laptop", IdempotencyKey: "queued-key-1", State: "queued", CreatedAt: model.Now()},
		{ID: "queued-task-2", RequesterID: "executor", WorkspaceID: "demo", ExecutorID: "laptop", IdempotencyKey: "queued-key-2", State: "queued", CreatedAt: model.Now()},
	}
	if err := live.Update(ctx, func(tx *Tx) error {
		for _, p := range []model.Principal{
			{ID: "executor", Kind: "executor", ExecutorID: "laptop", WorkspaceID: "demo", TokenHash: Hash(executorToken)},
			{ID: "agent", Kind: "claude", AgentID: "claude-demo", WorkspaceID: "demo", TokenHash: Hash(agentToken)},
		} {
			if err := tx.SavePrincipal(p); err != nil {
				return err
			}
		}
		if err := tx.SaveWorkspace(model.Workspace{ID: "demo", ExecutorID: "laptop"}); err != nil {
			return err
		}
		if err := tx.SaveTask(final); err != nil {
			return err
		}
		if err := tx.SaveTask(running); err != nil {
			return err
		}
		for _, task := range queued {
			if err := tx.SaveTask(task); err != nil {
				return err
			}
		}
		if err := tx.SaveCommand(model.Command{ID: "unfinished-command", TaskID: running.ID, AssignmentID: running.AssignmentID, OperationKey: "unfinished-operation", State: "running"}); err != nil {
			return err
		}
		if err := tx.Enqueue("laptop", "command.dispatch", model.Dispatch{WorkspaceID: "demo"}); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO peer_cursors(peer,stream,last_ack) VALUES('laptop','old-stream',20)")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	backupPath := filepath.Join(root, "backup.db")
	if err := live.Backup(ctx, backupPath); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	restoredPath := filepath.Join(root, "restored", "bridge.db")
	if err := RestoreNew(ctx, backupPath, restoredPath); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("restore modified its input snapshot")
	}
	restored := testStore(t, restoredPath)
	if restored.Epoch == "" || restored.Epoch == live.Epoch {
		t.Fatal("restore did not establish a new epoch")
	}
	if err := restored.Integrity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restored.View(ctx, func(tx *Tx) error {
		principals, err := tx.Principals()
		if err != nil {
			return err
		}
		if len(principals) != 2 {
			return fmt.Errorf("restored %d principals, want 2", len(principals))
		}
		for _, p := range principals {
			if !p.Disabled {
				return fmt.Errorf("restored device %s is enabled", p.ID)
			}
		}
		for _, token := range []string{executorToken, agentToken} {
			if _, err := tx.Authenticate(token); err == nil {
				return errors.New("a token restored from an old snapshot authenticated")
			}
		}
		got, err := tx.Task(final.ID)
		if err != nil {
			return err
		}
		if got.State != final.State || got.Summary != final.Summary || !bytes.Equal(got.Result, final.Result) {
			return fmt.Errorf("restore lost the completed result: %+v", got)
		}
		got, err = tx.Task(running.ID)
		if err != nil {
			return err
		}
		if got.State != "interrupted" || got.LeaseUntil != 0 {
			return fmt.Errorf("restored execution retains authorization: %+v", got)
		}
		for _, previous := range queued {
			got, err := tx.Task(previous.ID)
			if err != nil {
				return err
			}
			if got.State != "interrupted" || got.LeaseUntil != 0 {
				return fmt.Errorf("restored queued task can be scheduled automatically: %+v", got)
			}
		}
		workspace, err := tx.Workspace("demo")
		if err != nil {
			return err
		}
		if !workspace.Quarantined || workspace.QuarantineReason == "" {
			return fmt.Errorf("restored workspace was not quarantined: %+v", workspace)
		}
		command, err := tx.Command("unfinished-command")
		if err != nil {
			return err
		}
		if command.State != "unknown" {
			return fmt.Errorf("restored command has a falsely known outcome: %+v", command)
		}
		outbox, err := tx.Outbox("laptop")
		if err != nil || len(outbox) != 0 {
			return fmt.Errorf("old outbox still replays: length=%d, error=%v", len(outbox), err)
		}
		var cursors int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM peer_cursors").Scan(&cursors); err != nil || cursors != 0 {
			return fmt.Errorf("old cursors remain: count=%d, error=%v", cursors, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, original := range []*Store{live, testStore(t, backupPath)} {
		if err := original.Integrity(ctx); err != nil {
			t.Fatal(err)
		}
		if original.Epoch != live.Epoch {
			t.Fatal("restore changed an original database epoch")
		}
		if err := original.View(ctx, func(tx *Tx) error {
			for _, token := range []string{executorToken, agentToken} {
				if _, err := tx.Authenticate(token); err != nil {
					return fmt.Errorf("restore disabled original token: %w", err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := live.Update(ctx, func(tx *Tx) error { return tx.SetMeta("after-restore", "still-writable") }); err != nil {
		t.Fatal(err)
	}
	existingPath := filepath.Join(root, "existing.db")
	sentinel := []byte("preserve this existing destination")
	if err := os.WriteFile(existingPath, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreNew(ctx, backupPath, existingPath); err == nil {
		t.Fatal("restore accepted an existing destination")
	}
	preserved, err := os.ReadFile(existingPath)
	if err != nil || !bytes.Equal(preserved, sentinel) {
		t.Fatalf("restore altered an existing destination: %q, error=%v", preserved, err)
	}
}

func TestOrdinaryRestartPreservesEpochCredentialsAndFinalResult(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bridge.db")
	first := testStore(t, path)
	epoch := first.Epoch
	const token = "restart-test-token"
	task := model.Task{
		ID: "completed", RequesterID: "executor", WorkspaceID: "demo", ExecutorID: "laptop",
		IdempotencyKey: "same-submission", RequestHash: Hash("original request"), State: "succeeded",
		CreatedAt: model.Now(), Summary: "finished before restart", Result: model.JSON(map[string]any{"answer": 42}),
	}
	if err := first.Update(ctx, func(tx *Tx) error {
		if err := tx.SavePrincipal(model.Principal{ID: "executor", Kind: "executor", ExecutorID: "laptop", WorkspaceID: "demo", TokenHash: Hash(token)}); err != nil {
			return err
		}
		if err := tx.SaveWorkspace(model.Workspace{ID: "demo", ExecutorID: "laptop"}); err != nil {
			return err
		}
		return tx.SaveTask(task)
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := testStore(t, path)
	if second.Epoch != epoch {
		t.Fatalf("ordinary restart changed epoch from %q to %q", epoch, second.Epoch)
	}
	if err := second.View(ctx, func(tx *Tx) error {
		if _, err := tx.Authenticate(token); err != nil {
			return fmt.Errorf("ordinary restart revoked a device: %w", err)
		}
		got, err := tx.TaskByKey(task.RequesterID, task.IdempotencyKey)
		if err != nil {
			return err
		}
		if got.ID != task.ID || got.State != task.State || got.Summary != task.Summary || !bytes.Equal(got.Result, task.Result) || got.RequestHash != task.RequestHash {
			return fmt.Errorf("completed task changed across restart: %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
