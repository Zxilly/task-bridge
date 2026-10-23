package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/server"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/spf13/cobra"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := command().ExecuteContext(ctx); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func command() *cobra.Command {
	var config string
	root := &cobra.Command{Use: "task-bridge-server", Short: "Self-hosted Claude task and remote Bash broker", Version: model.Version, SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().StringVar(&config, "config", "server.json", "Server configuration JSON")
	root.AddCommand(&cobra.Command{Use: "serve", Short: "Run the broker", RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, e := server.LoadConfig(config)
		if e != nil {
			return e
		}
		return server.Serve(cmd.Context(), cfg, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	}})
	root.AddCommand(&cobra.Command{Use: "migrate", Short: "Apply SQLite migrations while the broker is stopped", RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, e := server.LoadConfig(config)
		if e != nil {
			return e
		}
		s, e := storage.Open(cmd.Context(), cfg.Database)
		if e != nil {
			return e
		}
		defer s.Close()
		return s.Integrity(cmd.Context())
	}})
	var input string
	restore := &cobra.Command{Use: "restore", Short: "Restore a snapshot to a new database path; invalidate restored device tokens", RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, e := server.LoadConfig(config)
		if e != nil {
			return e
		}
		if input == "" {
			return errors.New("--input is required")
		}
		return storage.RestoreNew(cmd.Context(), input, cfg.Database)
	}}
	restore.Flags().StringVar(&input, "input", "", "Consistent SQLite snapshot path")
	root.AddCommand(restore)
	var healthURL string
	health := &cobra.Command{Use: "healthcheck", Short: "Check an HTTP readiness endpoint", RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
		defer cancel()
		req, e := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
		if e != nil {
			return e
		}
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("readiness returned HTTP %d", resp.StatusCode)
		}
		return nil
	}}
	health.Flags().StringVar(&healthURL, "url", "http://127.0.0.1:8080/readyz", "HTTP readiness URL")
	root.AddCommand(health)
	admin := &cobra.Command{Use: "admin", Short: "Manage the running broker over its authenticated loopback API"}
	root.AddCommand(admin)
	call := func(cmd *cobra.Command, method string, args any) error {
		cfg, e := server.LoadConfig(config)
		if e != nil {
			return e
		}
		token, e := cfg.AdminToken()
		if e != nil {
			return e
		}
		return adminCall(cmd.Context(), cfg, token, method, args, cmd.OutOrStdout())
	}
	executors := &cobra.Command{Use: "executors"}
	admin.AddCommand(executors)
	var resetReason string
	resetJournal := &cobra.Command{Use: "reset-journal ID", Short: "Reset a lost journal binding after verified stop; revoke old executor credentials", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return call(cmd, "executors.reset_journal", map[string]any{"id": args[0], "reason": resetReason})
	}}
	resetJournal.Flags().StringVar(&resetReason, "reason", "", "Evidence that all prior execution has stopped")
	executors.AddCommand(resetJournal)
	devices := &cobra.Command{Use: "devices"}
	admin.AddCommand(devices)
	var kind, executor, workspace, agent string
	create := &cobra.Command{Use: "create", Short: "Issue one device token, shown once", RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd, "devices.create", model.Principal{Kind: kind, ExecutorID: executor, WorkspaceID: workspace, AgentID: agent})
	}}
	create.Flags().StringVar(&kind, "kind", "", "executor or claude")
	create.Flags().StringVar(&executor, "executor", "", "Stable B executor ID")
	create.Flags().StringVar(&workspace, "workspace", "", "Bound workspace ID")
	create.Flags().StringVar(&agent, "agent", "", "Stable Claude slot ID")
	devices.AddCommand(create, &cobra.Command{Use: "list", RunE: func(cmd *cobra.Command, _ []string) error { return call(cmd, "devices.list", struct{}{}) }}, &cobra.Command{Use: "revoke ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return call(cmd, "devices.revoke", map[string]any{"id": args[0]})
	}})
	workspaces := &cobra.Command{Use: "workspaces"}
	admin.AddCommand(workspaces)
	var wsID, wsExecutor, description string
	wsCreate := &cobra.Command{Use: "create", RunE: func(cmd *cobra.Command, _ []string) error {
		return call(cmd, "workspaces.create", model.Workspace{ID: wsID, ExecutorID: wsExecutor, Description: description})
	}}
	wsCreate.Flags().StringVar(&wsID, "id", "", "Workspace ID")
	wsCreate.Flags().StringVar(&wsExecutor, "executor", "", "Owning executor ID")
	wsCreate.Flags().StringVar(&description, "description", "", "Workspace description")
	workspaces.AddCommand(wsCreate)
	var releaseReason string
	release := &cobra.Command{Use: "release ID", Short: "Release quarantine after verified process reconciliation", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return call(cmd, "workspaces.release", map[string]any{"id": args[0], "reason": releaseReason})
	}}
	release.Flags().StringVar(&releaseReason, "reason", "", "Evidence that execution has stopped")
	workspaces.AddCommand(release)
	tasks := &cobra.Command{Use: "tasks"}
	admin.AddCommand(tasks, &cobra.Command{Use: "commands"})
	tasks.AddCommand(&cobra.Command{Use: "inspect ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return call(cmd, "tasks.inspect", map[string]any{"id": args[0]})
	}})
	var abandonReason string
	abandon := &cobra.Command{Use: "abandon ID", Short: "Audit an unresolved task and quarantine its workspace", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return call(cmd, "tasks.abandon", map[string]any{"id": args[0], "reason": abandonReason})
	}}
	abandon.Flags().StringVar(&abandonReason, "reason", "", "Audit reason")
	tasks.AddCommand(abandon)
	var outcome, resolveReason string
	resolve := &cobra.Command{Use: "resolve ID", Short: "Resolve an unknown command using verified evidence", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return call(cmd, "commands.resolve", map[string]any{"id": args[0], "outcome": outcome, "reason": resolveReason})
	}}
	resolve.Flags().StringVar(&outcome, "outcome", "", "not_started or stopped")
	resolve.Flags().StringVar(&resolveReason, "reason", "", "Verification evidence")
	for _, c := range admin.Commands() {
		if c.Name() == "commands" {
			c.AddCommand(resolve)
		}
	}
	var output string
	backup := &cobra.Command{Use: "backup", Short: "Write a consistent snapshot to a new path on the broker machine", RunE: func(cmd *cobra.Command, _ []string) error {
		if output == "" {
			return errors.New("--output is required")
		}
		return call(cmd, "backup", map[string]any{"output": output})
	}}
	backup.Flags().StringVar(&output, "output", "", "New snapshot path on the broker")
	admin.AddCommand(backup)
	return root
}
func adminCall(ctx context.Context, cfg server.Config, token, method string, args any, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	body := model.JSON(model.RPC{Method: method, Args: model.JSON(args)})
	req, e := http.NewRequestWithContext(ctx, "POST", "http://"+cfg.AdminListen+"/admin/rpc", bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	data, e := io.ReadAll(io.LimitReader(resp.Body, model.MaxMessageBytes))
	if e != nil {
		return e
	}
	var v map[string]json.RawMessage
	if e = json.Unmarshal(data, &v); e != nil {
		return e
	}
	if resp.StatusCode != 200 {
		if raw := v["error"]; len(raw) > 0 {
			var remote model.Error
			if json.Unmarshal(raw, &remote) == nil {
				return &remote
			}
		}
		return fmt.Errorf("administrative HTTP %d", resp.StatusCode)
	}
	var pretty bytes.Buffer
	if e = json.Indent(&pretty, v["result"], "", "  "); e != nil {
		return e
	}
	_, e = fmt.Fprintln(out, pretty.String())
	return e
}
