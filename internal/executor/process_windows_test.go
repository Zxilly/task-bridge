//go:build windows

package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartupJournalFailureNeverRunsCommand(t *testing.T) {
	root := t.TempDir()
	result := Run(context.Background(), Config{Root: root, BashPath: testBash(t)}, Request{Command: "printf 'must-not-run' > startup-marker.txt"}, func(int) error {
		return errors.New("startup journal unavailable")
	}, nil)
	if result.State != "failed" || !strings.Contains(result.Error, "startup journal unavailable") {
		t.Fatalf("startup failure lost its diagnostic: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, "startup-marker.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("command ran before its startup journal committed: %v", err)
	}
}
