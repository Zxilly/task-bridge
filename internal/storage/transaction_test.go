package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestUpdateWaitsForCanceledTransactionRollback(t *testing.T) {
	tx := &heldRollbackTx{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	release := sync.OnceFunc(func() { close(tx.release) })
	db := sql.OpenDB(heldRollbackConnector{tx})
	defer db.Close()
	defer release()
	s := &Store{DB: db}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callbackReturned := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- s.Update(ctx, func(*Tx) error {
			cancel()
			// Force database/sql's cancellation goroutine to own rollback
			// before Update's deferred Rollback can observe sql.ErrTxDone.
			<-tx.started
			close(callbackReturned)
			return ctx.Err()
		})
	}()
	select {
	case <-callbackReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("transaction cancellation did not start rollback")
	}
	select {
	case err := <-finished:
		release()
		<-tx.finished
		t.Fatalf("Update returned before the driver finished rollback: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Update error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Update did not return after rollback finished")
	}
}

type heldRollbackConnector struct{ tx *heldRollbackTx }

func (c heldRollbackConnector) Connect(context.Context) (driver.Conn, error) {
	return heldRollbackConn{c.tx}, nil
}
func (c heldRollbackConnector) Driver() driver.Driver { return c }
func (c heldRollbackConnector) Open(string) (driver.Conn, error) {
	return c.Connect(context.Background())
}

type heldRollbackConn struct{ tx *heldRollbackTx }

func (c heldRollbackConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c heldRollbackConn) Close() error                       { return nil }
func (c heldRollbackConn) Begin() (driver.Tx, error)          { return c.tx, nil }
func (c heldRollbackConn) ResetSession(context.Context) error { return nil }
func (c heldRollbackConn) IsValid() bool                      { return true }

type heldRollbackTx struct {
	started, release, finished chan struct{}
}

func (tx *heldRollbackTx) Commit() error { return errors.New("unexpected commit") }
func (tx *heldRollbackTx) Rollback() error {
	close(tx.started)
	<-tx.release
	close(tx.finished)
	return nil
}
