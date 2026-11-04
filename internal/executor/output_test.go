package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestInheritedPipesPreserveKnownShellExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix background shell semantics")
	}
	for _, tc := range []struct {
		name, command string
		exit          int
	}{{"success", "true && sleep 3 > background.log 2>&1 & printf ready; exit 0", 0}, {"failure", "sleep 3 & printf ready; printf detail >&2; exit 7", 7}} {
		t.Run(tc.name, func(t *testing.T) {
			output := map[string]string{}
			promoted := false
			result := Run(context.Background(), Config{Root: t.TempDir(), BashPath: testBash(t), Backgrounded: func(code int) error {
				promoted = true
				if code != tc.exit {
					t.Errorf("shell code=%d", code)
				}
				return nil
			}}, Request{Command: tc.command, Timeout: 5 * time.Second}, nil, func(c Chunk) error { output[c.Stream] += string(c.Data); return nil })
			if result.State != "exited" || result.ExitCode != tc.exit || result.Error != "" {
				t.Fatalf("known exit lost: %+v", result)
			}
			if !promoted || !result.Background || result.OutputIncomplete || result.ShellExitCode == nil || *result.ShellExitCode != tc.exit {
				t.Fatalf("missing background tracking: %+v", result)
			}
			if !strings.Contains(output["stdout"], "ready") {
				t.Fatalf("early output lost: %v", output)
			}
		})
	}
}

func TestTimeoutKillsDescendantEvenWhenShellExitsOnTerm(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix signal semantics")
	}
	root := t.TempDir()
	result := Run(context.Background(), Config{Root: root, BashPath: testBash(t), KillGrace: 100 * time.Millisecond}, Request{Command: "trap 'exit 0' TERM; (trap '' TERM; sleep 0.8; printf orphan > survivor) & wait", Timeout: 150 * time.Millisecond}, nil, nil)
	if result.State != "timed_out" {
		t.Fatalf("result: %+v", result)
	}
	time.Sleep(900 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(root, "survivor")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("TERM-ignoring child survived: %v", err)
	}
}

func TestDrainRetainsOutputAfterShellExit(t *testing.T) {
	var output string
	result := Run(context.Background(), Config{Root: t.TempDir(), BashPath: testBash(t)}, Request{Command: "(sleep 0.1; printf late) & printf early"}, nil, func(c Chunk) error { output += string(c.Data); return nil })
	if result.State != "exited" || result.ExitCode != 0 || result.OutputIncomplete || output != "earlylate" {
		t.Fatalf("output=%q result=%+v", output, result)
	}
}

func TestAutomaticBackgroundStillHonorsCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix background shell semantics")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	r := Run(ctx, Config{Root: root, BashPath: testBash(t), Backgrounded: func(int) error { cancel(); return nil }}, Request{Command: "(sleep 3; printf survived > survivor) & exit 0", Timeout: 5 * time.Second}, nil, nil)
	if r.State != "cancelled" || !r.Background || r.ShellExitCode == nil || *r.ShellExitCode != 0 {
		t.Fatalf("cancellation lost: %+v", r)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(root, "survivor")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("background descendant survived: %v", err)
	}
}
