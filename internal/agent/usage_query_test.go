package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func usageReportFixture(rows string) string {
	return `{"type":"assistant","usage_report":{"session":{"total_cost_usd":0,"total_api_duration_ms":0,"model_usage":{}},"rate_limits":{"limits":` + rows + `}}}` + "\n" + `{"type":"result","is_error":false,"num_turns":0,"total_cost_usd":0}` + "\n"
}

func TestParseUsageReportKeepsInactiveAndModelWindows(t *testing.T) {
	data := usageReportFixture(`[{"kind":"session","percent":100,"resets_at":"2026-10-28T07:00:00.123+00:00","severity":"critical"},{"kind":"weekly_all","percent":0,"resets_at":null,"is_active":false},{"kind":"weekly_sonnet","percent":38,"resets_at":"2026-10-30T18:00:00Z"},{"kind":"unrecognized_future_row","percent":20}]`)
	now := time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
	u, err := parseUsageReport([]byte(data), now)
	if err != nil {
		t.Fatal(err)
	}
	if u.Source != model.AccountUsageCommandSource || !u.Available || u.Stale || u.Status != "rejected" || len(u.Windows) != 3 {
		t.Fatalf("wrong quota report: %+v", u)
	}
	if *u.Windows["seven_day"].UsedPercentage != 0 || u.Windows["seven_day"].ResetsAt != nil || *u.Windows["seven_day_sonnet"].UsedPercentage != 38 {
		t.Fatalf("inactive/unknown fields lost: %+v", u.Windows)
	}
	if err := u.Validate(now); err != nil {
		t.Fatal(err)
	}
}

func TestParseUsageReportRejectsUnsupportedAndModelTurns(t *testing.T) {
	valid := usageReportFixture(`[{"kind":"session","percent":24,"resets_at":"2030-01-01T00:00:00Z"}]`)
	for _, data := range []string{
		`{"type":"result","is_error":false,"num_turns":0,"total_cost_usd":0,"result":"/usage isn't available in this environment."}`,
		usageReportFixture(`null`),
		usageReportFixture(`[{"kind":"session","percent":101}]`),
		usageReportFixture(`[{"kind":"session","percent":null}]`),
		usageReportFixture(`[{"kind":"session","percent":1,"resets_at":"invalid-date"}]`),
		usageReportFixture(`[{"kind":"session","percent":1},{"kind":"session","percent":2}]`),
		strings.Replace(valid, `"num_turns":0`, `"num_turns":1`, 1),
		strings.Replace(valid, `"total_api_duration_ms":0`, `"total_api_duration_ms":100`, 1),
		strings.ReplaceAll(valid, `"total_cost_usd":0`, `"total_cost_usd":1`),
		`not-json`,
	} {
		if _, err := parseUsageReport([]byte(data), time.Now()); err == nil {
			t.Errorf("accepted unsupported or billed report: %s", data)
		}
	}
}

func TestCommandLauncherReadsLocalUsageWithoutTaskFlags(t *testing.T) {
	launcher, reportPath := launcherHelper(t, "usage-query")
	u, err := launcher.ReadUsage(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if u.Source != model.AccountUsageCommandSource || !u.Available || *u.Windows["five_hour"].UsedPercentage != 24 || *u.Windows["seven_day"].UsedPercentage != 38 {
		t.Fatalf("local command report: %+v", u)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	args := "\n" + string(data) + "\n"
	for _, required := range []string{"/usage", "--tools", "--strict-mcp-config", "--no-session-persistence"} {
		if !strings.Contains(args, "\n"+required+"\n") {
			t.Errorf("usage command omitted %s", required)
		}
	}
	for _, forbidden := range []string{"--disable-slash-commands", "--session-id", "--resume", "--model", "--effort"} {
		if strings.Contains(args, "\n"+forbidden+"\n") {
			t.Errorf("usage command includes %s", forbidden)
		}
	}
	output := &usageCommandOutput{}
	_, _ = output.Write([]byte(strings.Repeat("x", maxUsageEventBytes+1)))
	_, _ = output.Write([]byte("more"))
	if !output.truncated || len(output.data) != maxUsageEventBytes {
		t.Fatal("usage command output was not bounded")
	}
}

func TestUsageQueryCancellationStopsProcessTree(t *testing.T) {
	launcher, ready := launcherHelper(t, "tree")
	marker := filepath.Join(t.TempDir(), "orphan.txt")
	t.Setenv("TASK_BRIDGE_LAUNCH_TEST_MARKER", marker)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	directory := t.TempDir()
	go func() { _, err := launcher.ReadUsage(ctx, directory); done <- err }()
	defer cancel()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("usage process never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("usage cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("usage command failed to stop")
	}
	time.Sleep(2200 * time.Millisecond)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("usage descendant survived: %v", err)
	}
}

type usageReaderFunc func(context.Context, string) (model.AccountUsage, error)

func (f usageReaderFunc) ReadUsage(ctx context.Context, directory string) (model.AccountUsage, error) {
	return f(ctx, directory)
}

func TestIdleRefreshContinuesAfterFailureAndTaskCannotEraseCache(t *testing.T) {
	var calls atomic.Int32
	updates := make(chan model.AccountUsage, 10)
	upstream, _ := relayTestUpstream(t, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args model.AgentUsageUpdateArgs
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			return nil, err
		}
		updates <- args.Usage
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: `{"accepted":true}`}}}, nil
	})
	w := &Worker{cfg: Config{StateDir: t.TempDir()}, log: slog.New(slog.NewTextHandler(io.Discard, nil)), usageReader: usageReaderFunc(func(context.Context, string) (model.AccountUsage, error) {
		n := calls.Add(1)
		if n == 2 {
			return model.AccountUsage{}, errors.New("temporary usage refresh failure")
		}
		percentage := 24.0
		if n > 2 {
			percentage = 38
		}
		u := model.AccountUsage{Source: model.AccountUsageCommandSource, Status: "allowed", ObservedAt: time.Now().Unix(), Windows: map[string]model.UsageWindow{"five_hour": {UsedPercentage: &percentage, ObservedAt: time.Now().Unix()}}}
		return u.WithFreshness(time.Now()), nil
	})}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); w.refreshUsage(ctx, upstream, 20*time.Millisecond) }()
	defer func() { cancel(); <-done }()
	for _, want := range []float64{24, 38} {
		select {
		case u := <-updates:
			if !u.Available || *u.Windows["five_hour"].UsedPercentage != want {
				t.Fatalf("bad refresh after failure: %+v", u)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("idle refresh stopped")
		}
	}
	cancel()
	<-done
	// A task which has not received rate-limit headers must not replace the
	// controller's already known account data with an empty invocation snapshot.
	for len(updates) > 0 {
		<-updates
	}
	emptyCtx, stop := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer stop()
	w.reportUsage(emptyCtx, upstream, &accountUsage{})
	select {
	case u := <-updates:
		t.Fatalf("empty task overwrote quota: %+v", u)
	default:
	}
}
