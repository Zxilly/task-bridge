package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/Zxilly/task-bridge/internal/executor"
)

type WorkspaceConfig struct {
	Root     string            `json:"root"`
	BashPath string            `json:"bash_path"`
	Env      map[string]string `json:"env,omitempty"`
}

type Config struct {
	ServerURL       string                     `json:"server_url"`
	ExecutorID      string                     `json:"executor_id"`
	JournalPath     string                     `json:"journal_path"`
	TokenEnv        string                     `json:"token_env,omitempty"`
	TokenFile       string                     `json:"token_file,omitempty"`
	Workspaces      map[string]WorkspaceConfig `json:"workspaces"`
	MaxOutputBytes  int64                      `json:"max_output_bytes,omitempty"`
	MaxJournalBytes int64                      `json:"max_journal_bytes,omitempty"`
	AllowInsecureWS bool                       `json:"allow_insecure_ws,omitempty"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1024*1024))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("client config: %w", err)
	}
	if err = decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return cfg, errors.New("client config must contain one JSON object")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return cfg, err
	}
	base := filepath.Dir(path)
	resolve := func(p string) string {
		if p != "" && !filepath.IsAbs(p) {
			return filepath.Join(base, p)
		}
		return p
	}
	cfg.JournalPath = resolve(cfg.JournalPath)
	cfg.TokenFile = resolve(cfg.TokenFile)
	for id, ws := range cfg.Workspaces {
		ws.Root = resolve(ws.Root)
		if strings.ContainsAny(ws.BashPath, `/\`) {
			ws.BashPath = resolve(ws.BashPath)
		}
		cfg.Workspaces[id] = ws
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	u, err := url.Parse(c.ServerURL)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" {
		return errors.New("server_url must be a ws/wss URL without credentials or fragment")
	}
	if u.Scheme == "ws" && !c.AllowInsecureWS {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("non-loopback server_url requires wss (or explicit allow_insecure_ws)")
		}
	}
	if c.ExecutorID == "" || c.JournalPath == "" {
		return errors.New("executor_id and journal_path are required")
	}
	if (c.TokenEnv == "") == (c.TokenFile == "") {
		return errors.New("configure exactly one of token_env or token_file")
	}
	if len(c.Workspaces) == 0 {
		return errors.New("configure at least one workspace")
	}
	if c.MaxOutputBytes < 0 || c.MaxOutputBytes > 50*1024*1024 {
		return errors.New("max_output_bytes must be between 0 and 52428800")
	}
	if c.MaxJournalBytes != 0 && c.MaxJournalBytes < 32*1024*1024 {
		return errors.New("max_journal_bytes must be at least 33554432")
	}
	for id, ws := range c.Workspaces {
		if id == "" || ws.Root == "" || ws.BashPath == "" {
			return errors.New("each workspace requires an id, root and bash_path")
		}
		if _, err := executor.ResolveCwd(ws.Root, ""); err != nil {
			return fmt.Errorf("workspace %s: %w", id, err)
		}
		if _, err := exec.LookPath(ws.BashPath); err != nil {
			return fmt.Errorf("workspace %s bash_path: %w", id, err)
		}
		for key, value := range ws.Env {
			if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') {
				return fmt.Errorf("workspace %s has invalid environment entry", id)
			}
		}
	}
	return nil
}

func (c Config) Token() (string, error) {
	value := ""
	if c.TokenFile != "" {
		data, err := os.ReadFile(c.TokenFile)
		if err != nil {
			return "", fmt.Errorf("read token_file: %w", err)
		}
		value = string(data)
	} else {
		value = os.Getenv(c.TokenEnv)
	}
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("configured credential is empty or invalid")
	}
	return value, nil
}
