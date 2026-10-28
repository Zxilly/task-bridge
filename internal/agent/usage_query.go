package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/processcontrol"
)

type usageReader interface {
	ReadUsage(context.Context, string) (model.AccountUsage, error)
}

// ReadUsage invokes Claude's built-in local command using the user's normal
// login. It reads the structured usage_report, never the localized display text.
func (l CommandLauncher) ReadUsage(ctx context.Context, directory string) (model.AccountUsage, error) {
	if err := ctx.Err(); err != nil {
		return model.AccountUsage{}, err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return model.AccountUsage{}, err
	}
	mcpConfig := filepath.Join(directory, "mcp.json")
	settings := filepath.Join(directory, "settings.json")
	if err := os.WriteFile(mcpConfig, []byte(`{"mcpServers":{}}`), 0600); err != nil {
		return model.AccountUsage{}, err
	}
	if err := os.WriteFile(settings, []byte(`{"autoMemoryEnabled":false,"disableAllHooks":true}`), 0600); err != nil {
		return model.AccountUsage{}, err
	}
	path := l.Path
	if path == "" {
		path = "claude"
	}
	// --disable-slash-commands also disables /usage in print mode. It belongs
	// on task invocations, but must not be used for this local command.
	cmd := exec.Command(path, "--print", "/usage", "--tools", "", "--strict-mcp-config", "--mcp-config", mcpConfig,
		"--settings", settings, "--setting-sources", "user", "--permission-prompts", "none",
		"--output-format", "stream-json", "--verbose", "--no-session-persistence")
	cmd.Dir, cmd.Env = directory, l.environment("", "")
	output := &usageCommandOutput{}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = output, io.Discard, 3*time.Second
	process, err := processcontrol.PrepareForParentExit(cmd)
	if err != nil {
		return model.AccountUsage{}, fmt.Errorf("prepare Claude usage command: %w", err)
	}
	defer process.Close()
	if err := process.Start(cmd); err != nil {
		return model.AccountUsage{}, fmt.Errorf("start Claude usage command: %w", err)
	}
	defer process.Terminate(cmd, true)
	if err := process.Resume(); err != nil {
		process.Terminate(cmd, true)
		_ = cmd.Wait()
		return model.AccountUsage{}, fmt.Errorf("resume Claude usage command: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		process.Terminate(cmd, true)
		<-done
		return model.AccountUsage{}, ctx.Err()
	}
	if err != nil {
		return model.AccountUsage{}, fmt.Errorf("Claude usage command: %w", err)
	}
	if output.truncated {
		return model.AccountUsage{}, errors.New("Claude usage command output exceeded 1 MiB")
	}
	return parseUsageReport(output.data, time.Now())
}

type usageCommandOutput struct {
	data      []byte
	truncated bool
}

func (o *usageCommandOutput) Write(data []byte) (int, error) {
	n := len(data)
	remaining := maxUsageEventBytes - len(o.data)
	if len(data) > remaining {
		data = data[:remaining]
		o.truncated = true
	}
	o.data = append(o.data, data...)
	return n, nil
}

func parseUsageReport(data []byte, now time.Time) (model.AccountUsage, error) {
	type limit struct {
		Kind     string   `json:"kind"`
		Percent  *float64 `json:"percent"`
		ResetsAt *string  `json:"resets_at"`
		Severity string   `json:"severity"`
	}
	type report struct {
		Session struct {
			TotalCost   float64                    `json:"total_cost_usd"`
			APIDuration int64                      `json:"total_api_duration_ms"`
			Models      map[string]json.RawMessage `json:"model_usage"`
		} `json:"session"`
		Limits struct {
			Rows []limit `json:"limits"`
		} `json:"rate_limits"`
	}
	var found *report
	completed := false
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), maxUsageEventBytes)
	for scanner.Scan() {
		var event struct {
			Type    string   `json:"type"`
			Report  *report  `json:"usage_report"`
			IsError bool     `json:"is_error"`
			Turns   *int     `json:"num_turns"`
			Cost    *float64 `json:"total_cost_usd"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) != nil {
			return model.AccountUsage{}, errors.New("invalid Claude usage command output")
		}
		if event.Type == "assistant" && event.Report != nil {
			found = event.Report
		}
		if event.Type == "result" {
			if event.IsError || event.Turns == nil || *event.Turns != 0 || event.Cost == nil || *event.Cost != 0 {
				return model.AccountUsage{}, errors.New("Claude /usage did not complete as a zero-turn local command")
			}
			completed = true
		}
	}
	if scanner.Err() != nil || !completed || found == nil || found.Session.TotalCost != 0 || found.Session.APIDuration != 0 || len(found.Session.Models) > 0 {
		return model.AccountUsage{}, errors.New("Claude /usage structured report unavailable; check CLI version, subscription login and command support")
	}
	usage := model.AccountUsage{Source: model.AccountUsageCommandSource, Status: "allowed", ObservedAt: now.Unix(), Windows: map[string]model.UsageWindow{}}
	for _, row := range found.Limits.Rows {
		name := map[string]string{"session": "five_hour", "weekly_all": "seven_day", "weekly_opus": "seven_day_opus", "weekly_sonnet": "seven_day_sonnet"}[row.Kind]
		if name == "" {
			continue
		}
		if row.Percent == nil || math.IsNaN(*row.Percent) || math.IsInf(*row.Percent, 0) || *row.Percent < 0 || *row.Percent > 100 {
			return model.AccountUsage{}, errors.New("invalid Claude account usage percentage")
		}
		if _, duplicate := usage.Windows[name]; duplicate {
			return model.AccountUsage{}, errors.New("ambiguous Claude account usage windows")
		}
		window := model.UsageWindow{UsedPercentage: row.Percent, ObservedAt: now.Unix()}
		if row.ResetsAt != nil {
			reset, err := time.Parse(time.RFC3339Nano, *row.ResetsAt)
			if err != nil {
				return model.AccountUsage{}, errors.New("invalid Claude account usage reset time")
			}
			seconds := reset.Unix()
			window.ResetsAt = &seconds
		}
		usage.Windows[name] = window
		if *row.Percent >= 100 {
			usage.Status = "rejected"
		} else if usage.Status == "allowed" && (row.Severity == "warning" || row.Severity == "critical") {
			usage.Status = "allowed_warning"
		}
	}
	if len(usage.Windows) == 0 {
		return model.AccountUsage{}, errors.New("Claude /usage reported no account rate-limit windows")
	}
	return usage.WithFreshness(now), nil
}
