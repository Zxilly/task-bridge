// Package agent runs each broker task in a fresh Claude Code process.
package agent

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
)

type Config struct {
	ServerURL       string `json:"server_url"`
	TokenEnv        string `json:"token_env,omitempty"`
	TokenFile       string `json:"token_file,omitempty"`
	ClaudePath      string `json:"claude_path,omitempty"`
	StateDir        string `json:"state_dir,omitempty"`
	Model           string `json:"model,omitempty"`
	AllowInsecureWS bool   `json:"allow_insecure_ws,omitempty"`
}

func LoadConfig(path string) (Config, error) {
	cfg := Config{ClaudePath: "claude", StateDir: "agent-state"}
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("agent config: %w", err)
	}
	if err = decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return cfg, errors.New("agent config must contain one JSON object")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return cfg, err
	}
	resolve := func(value string) string {
		if value != "" && !filepath.IsAbs(value) {
			return filepath.Join(filepath.Dir(path), value)
		}
		return value
	}
	cfg.StateDir, cfg.TokenFile = resolve(cfg.StateDir), resolve(cfg.TokenFile)
	if strings.ContainsAny(cfg.ClaudePath, `/\`) {
		cfg.ClaudePath = resolve(cfg.ClaudePath)
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
	if (c.TokenEnv == "") == (c.TokenFile == "") {
		return errors.New("configure exactly one of token_env or token_file")
	}
	if c.StateDir == "" || c.ClaudePath == "" {
		return errors.New("state_dir and claude_path are required")
	}
	if _, err := exec.LookPath(c.ClaudePath); err != nil {
		return fmt.Errorf("claude_path: %w", err)
	}
	return nil
}

func (c Config) Token() (string, error) {
	value := os.Getenv(c.TokenEnv)
	if c.TokenFile != "" {
		data, err := os.ReadFile(c.TokenFile)
		if err != nil {
			return "", fmt.Errorf("read token_file: %w", err)
		}
		value = string(data)
	}
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("configured credential is empty or invalid")
	}
	return value, nil
}
