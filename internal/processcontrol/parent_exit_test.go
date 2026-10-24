package processcontrol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const parentExitHelper = "TASK_BRIDGE_PARENT_EXIT_HELPER"

func init() {
	if mode := os.Getenv(parentExitHelper); mode != "" {
		if err := runParentExitHelper(mode); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(99)
		}
		os.Exit(0)
	}
}

func runParentExitHelper(mode string) error {
	if mode == "child" {
		if err := os.WriteFile(os.Getenv("TASK_BRIDGE_PARENT_EXIT_READY"), []byte("ready"), 0600); err != nil {
			return err
		}
		time.Sleep(1500 * time.Millisecond)
		return os.WriteFile(os.Getenv("TASK_BRIDGE_PARENT_EXIT_MARKER"), []byte("child survived"), 0600)
	}
	if mode != "owner" {
		return errors.New("unknown parent-exit helper mode")
	}
	child := exec.Command(os.Args[0])
	child.Env = append(os.Environ(), parentExitHelper+"=child")
	child.Stdout, child.Stderr = io.Discard, io.Discard
	control, err := PrepareForParentExit(child)
	if err != nil {
		return err
	}
	defer control.Close()
	if err := control.Start(child); err != nil {
		return err
	}
	if err := control.Resume(); err != nil {
		control.Terminate(child, true)
		_ = child.Wait()
		return err
	}
	return child.Wait()
}

func parentExitCommand(t *testing.T, mode string) *exec.Cmd {
	t.Helper()
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("parent-exit ownership is supported on Windows and Linux")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	t.Setenv(parentExitHelper, mode)
	t.Setenv("TASK_BRIDGE_PARENT_EXIT_READY", filepath.Join(directory, "ready"))
	t.Setenv("TASK_BRIDGE_PARENT_EXIT_MARKER", filepath.Join(directory, "orphan-marker"))
	command := exec.Command(executable)
	command.Stdout, command.Stderr = io.Discard, io.Discard
	return command
}

func TestParentExitStopsOwnedCLIWithoutDeferredCleanup(t *testing.T) {
	owner := parentExitCommand(t, "owner")
	if err := owner.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- owner.Wait() }()
	defer owner.Process.Kill()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(os.Getenv("TASK_BRIDGE_PARENT_EXIT_READY")); err == nil {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("owner exited before its CLI child started: %v", err)
		case <-deadline.C:
			t.Fatal("owned CLI child did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Kill only the worker process. Its cleanup code cannot run, and the child
	// must be stopped by kernel ownership rather than the test killing a PID.
	if err := owner.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("forcibly terminated owner was not reaped")
	}
	time.Sleep(1700 * time.Millisecond)
	if _, err := os.Stat(os.Getenv("TASK_BRIDGE_PARENT_EXIT_MARKER")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CLI continued after its worker was killed: %v", err)
	}
}

func TestParentExitControlAllowsOrdinaryWaitAndRepeatedClose(t *testing.T) {
	child := parentExitCommand(t, "child")
	control, err := PrepareForParentExit(child)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := control.Start(child); err != nil {
		t.Fatal(err)
	}
	if err := control.Resume(); err != nil {
		control.Terminate(child, true)
		_ = child.Wait()
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	control.Close()
	if _, err := os.Stat(os.Getenv("TASK_BRIDGE_PARENT_EXIT_MARKER")); err != nil {
		t.Fatalf("normal owner lifetime killed its child early: %v", err)
	}
}

func TestParentExitControlFailedStartClosesWithoutLeakingThread(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("parent-exit ownership is supported on Windows and Linux")
	}
	command := exec.CommandContext(context.Background(), filepath.Join(t.TempDir(), "does-not-exist"))
	control, err := PrepareForParentExit(command)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := control.Start(command); err == nil {
		t.Fatal("nonexistent CLI executable started")
	}
	control.Close()
}
