package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

type Config struct {
	Listen           string `json:"listen"`
	Database         string `json:"database"`
	AdminListen      string `json:"admin_listen"`
	AdminTokenEnv    string `json:"admin_token_env"`
	MaxQueuedTasks   int    `json:"max_queued_tasks"`
	MaxDatabaseBytes int64  `json:"max_database_bytes,omitempty"`
}

func LoadConfig(path string) (Config, error) {
	c := Config{Listen: "127.0.0.1:8080", Database: "data/bridge.db", AdminListen: "127.0.0.1:8081", AdminTokenEnv: "BRIDGE_ADMIN_TOKEN", MaxQueuedTasks: 1000, MaxDatabaseBytes: 2 << 30}
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return c, errors.New("configuration must contain one JSON object")
	}
	if !filepath.IsAbs(c.Database) {
		abs, e := filepath.Abs(path)
		if e != nil {
			return c, e
		}
		c.Database = filepath.Join(filepath.Dir(abs), c.Database)
	}
	if c.Database == "" || c.AdminTokenEnv == "" || c.MaxQueuedTasks < 1 || c.MaxQueuedTasks > 100000 {
		return c, errors.New("invalid database, admin credential, or task limit")
	}
	if c.MaxDatabaseBytes < 32<<20 {
		return c, errors.New("max_database_bytes must reserve at least 32 MiB")
	}
	if _, _, e = net.SplitHostPort(c.Listen); e != nil {
		return c, e
	}
	host, _, e := net.SplitHostPort(c.AdminListen)
	if e != nil {
		return c, e
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return c, errors.New("admin_listen must use an explicit loopback IP address")
	}
	return c, nil
}
func (c Config) AdminToken() (string, error) {
	v := os.Getenv(c.AdminTokenEnv)
	if len(v) < 32 {
		return "", fmt.Errorf("%s must contain a random admin token of at least 32 characters", c.AdminTokenEnv)
	}
	return v, nil
}
