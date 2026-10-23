package executor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func testBash(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("BRIDGE_TEST_BASH"); configured != "" {
		return configured
	}
	if runtime.GOOS == "windows" {
		path := `C:\Program Files\Git\bin\bash.exe`
		if _, err := os.Stat(path); err == nil {
			return path
		}
		t.Skip("set BRIDGE_TEST_BASH to a Git Bash executable")
	}
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash unavailable")
	}
	return path
}

func TestRunUsesWorkspaceAndPreservesOutputAndExit(t *testing.T) {
	root := t.TempDir()
	streams := map[string]string{}
	result := Run(context.Background(), Config{Root: root, BashPath: testBash(t)}, Request{Command: "printf '你好 世界'; printf 'failure detail' >&2; printf local > marker.txt; exit 7"}, nil, func(chunk Chunk) error { streams[chunk.Stream] += string(chunk.Data); return nil })
	if result.State != "exited" || result.ExitCode != 7 {
		t.Fatalf("result: %+v", result)
	}
	if streams["stdout"] != "你好 世界" || streams["stderr"] != "failure detail" {
		t.Fatalf("streams: %+v", streams)
	}
	if data, err := os.ReadFile(filepath.Join(root, "marker.txt")); err != nil || string(data) != "local" {
		t.Fatalf("workspace marker: %q %v", data, err)
	}
}

func TestTimeoutStopsOrdinaryProcessTree(t *testing.T) {
	root := t.TempDir()
	begin := time.Now()
	result := Run(context.Background(), Config{Root: root, BashPath: testBash(t), KillGrace: 100 * time.Millisecond}, Request{Command: "(sleep 2; printf orphan > orphan.txt) & wait", Timeout: 250 * time.Millisecond}, nil, nil)
	if result.State != "timed_out" {
		t.Fatalf("result: %+v", result)
	}
	if time.Since(begin) > 5*time.Second {
		t.Fatal("timeout did not promptly stop process tree")
	}
	time.Sleep(2200 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(root, "orphan.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child process survived cancellation: %v", err)
	}
}

func TestOutputBoundDrainsAndCountsBytes(t *testing.T) {
	var kept int
	result := Run(context.Background(), Config{Root: t.TempDir(), BashPath: testBash(t), MaxOutputBytes: 100}, Request{Command: "printf '%10000s' ''"}, nil, func(chunk Chunk) error { kept += len(chunk.Data); return nil })
	if result.State != "exited" || kept != 100 || result.DroppedBytes != 9900 || !result.Truncated {
		t.Fatalf("kept=%d result=%+v", kept, result)
	}
}

func TestJournalFailureStopsExecution(t *testing.T) {
	begin := time.Now()
	result := Run(context.Background(), Config{Root: t.TempDir(), BashPath: testBash(t), KillGrace: 100 * time.Millisecond}, Request{Command: "printf ready; sleep 30"}, nil, func(Chunk) error { return errors.New("disk full") })
	if result.State != "failed" || !strings.Contains(result.Error, "disk full") {
		t.Fatalf("result: %+v", result)
	}
	if time.Since(begin) > 5*time.Second {
		t.Fatal("journal failure did not cancel execution")
	}
}

func TestResolveCwdRejectsEscape(t *testing.T) {
	root := t.TempDir()
	if _, err := ResolveCwd(root, ".."); err == nil {
		t.Fatal("accepted parent directory")
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "escape")); err == nil {
		if _, err := ResolveCwd(root, "escape"); err == nil {
			t.Fatal("accepted symlink escape")
		}
	}
}

func TestAlreadyCancelledNeverStarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := Run(ctx, Config{Root: t.TempDir(), BashPath: testBash(t)}, Request{Command: "printf no"}, func(int) error { t.Error("started cancelled command"); return nil }, nil)
	if result.State != "cancelled" {
		t.Fatalf("result: %+v", result)
	}
}
