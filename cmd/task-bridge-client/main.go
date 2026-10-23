package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/Zxilly/task-bridge/internal/client"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

func main() {
	if err := command().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func command() *cobra.Command {
	root := &cobra.Command{Use: "task-bridge-client", Short: "Local MCP task client and Bash executor", Version: model.Version, SilenceUsage: true, SilenceErrors: true}
	var configPath string
	root.PersistentFlags().StringVar(&configPath, "config", "client.json", "Client JSON configuration path")
	root.AddCommand(&cobra.Command{Use: "mcp", Short: "Serve task tools over stdio and connect to the broker", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := client.LoadConfig(configPath)
		if err != nil {
			return err
		}
		logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
		bridge, err := client.New(cfg, logger)
		if err != nil {
			return err
		}
		defer bridge.Close()
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		connected := make(chan error, 1)
		served := make(chan error, 1)
		go func() { connected <- bridge.Run(ctx) }()
		go func() {
			served <- bridge.MCPServer().Run(ctx, &mcp.StdioTransport{MaxLineLength: model.MaxMessageBytes})
		}()
		select {
		case err = <-served:
			cancel()
			<-connected
		case err = <-connected:
			cancel()
			<-served
		case <-ctx.Done():
			cancel()
			<-connected
			<-served
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}})
	var connect bool
	doctor := &cobra.Command{Use: "doctor", Short: "Validate configuration; does not execute a shell", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := client.LoadConfig(configPath)
		if err != nil {
			return err
		}
		if _, err = cfg.Token(); err != nil {
			return err
		}
		ids := make([]string, 0, len(cfg.Workspaces))
		for id := range cfg.Workspaces {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		status := map[string]any{"ok": true, "executor_id": cfg.ExecutorID, "workspaces": ids, "credential_present": true, "shell_executed": false, "network_checked": false}
		if connect {
			if err = probe(cmd.Context(), cfg.ServerURL); err != nil {
				return err
			}
			status["network_checked"] = true
			status["network_check"] = "TCP/TLS only; does not register or replace an executor connection"
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	}}
	doctor.Flags().BoolVar(&connect, "connect", false, "Check TCP/TLS reachability without registering an executor")
	root.AddCommand(doctor)
	var reason string
	reconcile := &cobra.Command{Use: "reconcile", Short: "Reconcile retained local evidence after server recovery; never execute commands", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := client.LoadConfig(configPath)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		result, err := client.Reconcile(ctx, cfg, reason)
		if err != nil {
			return fmt.Errorf("reconciliation did not commit a new local epoch; retain this journal and retry after reviewing the failure: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}}
	reconcile.Flags().StringVar(&reason, "reason", "", "Reviewed recovery evidence recorded in both server and local audit logs")
	_ = reconcile.MarkFlagRequired("reason")
	root.AddCommand(reconcile)
	return root
}
func probe(ctx context.Context, address string) error {
	u, err := url.Parse(address)
	if err != nil {
		return err
	}
	host := u.Host
	if u.Port() == "" {
		port := "443"
		if u.Scheme == "ws" {
			port = "80"
		}
		host = net.JoinHostPort(u.Hostname(), port)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	var conn net.Conn
	if u.Scheme == "wss" {
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}}).DialContext(ctx, "tcp", host)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", host)
	}
	if err != nil {
		return err
	}
	return conn.Close()
}
