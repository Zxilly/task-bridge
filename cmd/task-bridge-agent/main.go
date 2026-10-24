package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Zxilly/task-bridge/internal/agent"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/spf13/cobra"
)

func main() {
	if err := command().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type doctorStatus struct {
	OK                bool   `json:"ok"`
	ClaudePath        string `json:"claude_path"`
	StateDir          string `json:"state_dir"`
	CredentialPresent bool   `json:"credential_present"`
	ClaudeStarted     bool   `json:"claude_started"`
	NetworkChecked    bool   `json:"network_checked"`
}

func command() *cobra.Command {
	root := &cobra.Command{Use: "task-bridge-agent", Short: "Run each bridge task in a fresh Claude Code session", Version: model.Version, Args: cobra.NoArgs, SilenceUsage: true, SilenceErrors: true}
	var configPath string
	root.PersistentFlags().StringVar(&configPath, "config", "agent.json", "Agent JSON configuration path")
	executeWorker := func(cmd *cobra.Command, taskID string) error {
		cfg, err := agent.LoadConfig(configPath)
		if err != nil {
			return err
		}
		logger := slog.New(slog.NewJSONHandler(cmd.ErrOrStderr(), nil))
		worker, err := agent.New(cfg, logger)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if taskID == "" {
			err = worker.Run(ctx)
		} else {
			err = worker.Resume(ctx, taskID)
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	run := func(cmd *cobra.Command, _ []string) error { return executeWorker(cmd, "") }
	root.RunE = run
	root.AddCommand(&cobra.Command{Use: "run", Short: "Connect to the broker and launch one Claude process per task", Args: cobra.NoArgs, RunE: run})
	var resumeTaskID string
	resume := &cobra.Command{Use: "resume", Short: "Resume one task's saved Claude conversation after checking broker recovery", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if resumeTaskID == "" {
			return errors.New("--task-id is required for resume")
		}
		return executeWorker(cmd, resumeTaskID)
	}}
	resume.Flags().StringVar(&resumeTaskID, "task-id", "", "Task ID whose original Claude session should resume")
	_ = resume.MarkFlagRequired("task-id")
	root.AddCommand(resume)
	root.AddCommand(&cobra.Command{Use: "doctor", Short: "Validate local configuration without starting Claude or connecting", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := agent.LoadConfig(configPath)
		if err != nil {
			return err
		}
		if _, err = cfg.Token(); err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(doctorStatus{OK: true, ClaudePath: cfg.ClaudePath, StateDir: cfg.StateDir, CredentialPresent: true})
	}})
	return root
}
