package agent

import (
	"encoding/json"
	"os"
	"testing"
)

func TestCommandLauncherTaskOptionsAndDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, defaultModel, taskModel, effort, wantModel string
	}{
		{name: "Claude defaults"},
		{name: "agent model default", defaultModel: "sonnet", wantModel: "sonnet"},
		{name: "task model override", defaultModel: "sonnet", taskModel: "opus", wantModel: "opus"},
		{name: "effort only", effort: "low"},
		{name: "both options", defaultModel: "sonnet", taskModel: "claude-opus-4-6", effort: "max", wantModel: "claude-opus-4-6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			launcher, reportPath := launcherHelper(t, "record")
			launcher.Model = tc.defaultModel
			t.Setenv("CLAUDE_CODE_EFFORT_LEVEL", "medium")
			session := launcherSession(t)
			session.Model, session.Effort = tc.taskModel, tc.effort
			if err := launcher.Launch(t.Context(), session); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(reportPath)
			if err != nil {
				t.Fatal(err)
			}
			var report launchReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			for flag, want := range map[string]string{"--model": tc.wantModel, "--effort": tc.effort} {
				var got string
				for i, arg := range report.Args {
					if arg == flag && i+1 < len(report.Args) {
						got = report.Args[i+1]
					}
				}
				if got != want {
					t.Errorf("%s = %q, want %q", flag, got, want)
				}
			}
			wantEffort := tc.effort
			if wantEffort == "" {
				wantEffort = "medium"
			}
			if got := report.Environment["CLAUDE_CODE_EFFORT_LEVEL"]; got != wantEffort {
				t.Errorf("effort environment = %q, want %q", got, wantEffort)
			}
		})
	}
}
