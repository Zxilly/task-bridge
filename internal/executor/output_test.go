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
			begin := time.Now()
			result := Run(context.Background(), Config{Root: t.TempDir(), BashPath: testBash(t)}, Request{Command: tc.command, Timeout: 5 * time.Second}, nil, func(c Chunk) error { output[c.Stream] += string(c.Data); return nil })
			if result.State != "exited" || result.ExitCode != tc.exit || result.Error != "" {
				t.Fatalf("known exit lost: %+v", result)
			}
			if !result.OutputIncomplete {
				t.Fatalf("missing output warning: %+v", result)
			}
			if !strings.Contains(output["stdout"], "ready") {
				t.Fatalf("early output lost: %v", output)
			}
			if time.Since(begin) > 2800*time.Millisecond {
				t.Fatal("waited for inherited pipe EOF")
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
