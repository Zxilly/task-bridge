package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/google/uuid"
)

// ErrSessionExists prevents a new task launch from replacing recoverable context.
var ErrSessionExists = errors.New("a saved session already exists for this task")

const (
	sessionManifestVersion = 1
	sessionManifestName    = "session.json"
	maxSessionManifest     = 16 << 10
)

type savedSession struct {
	ID          string `json:"session_id"`
	TaskID      string `json:"task_id"`
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	BrokerKey   string `json:"broker_key"`
	Directory   string `json:"-"`
	MCPConfig   string `json:"-"`
}

type sessionManifest struct {
	Version int `json:"version"`
	savedSession
}

type sessionStore struct {
	directory string
	brokerKey string
}

// newSessionStore binds state to the exact configured broker endpoint. It never
// persists that URL or the device credential, and resolves the configured base
// directory before deriving any owned child paths.
func newSessionStore(stateDir, serverURL string) (*sessionStore, error) {
	if stateDir == "" || serverURL == "" {
		return nil, errors.New("session state directory and broker URL are required")
	}
	base, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(base, 0700); err != nil {
		return nil, err
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		return nil, err
	}
	directory := filepath.Join(base, "sessions")
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if err := checkSessionDirectory(directory); err != nil {
		return nil, err
	}
	if err := syncSessionDirectory(base); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(serverURL))
	return &sessionStore{directory: directory, brokerKey: hex.EncodeToString(hash[:])}, nil
}

func (s *sessionStore) Create(task model.Task) (savedSession, error) {
	if err := validateSessionTask(task); err != nil {
		return savedSession{}, err
	}
	if err := checkSessionDirectory(s.directory); err != nil {
		return savedSession{}, err
	}
	record := s.paths(savedSession{
		ID: uuid.NewString(), TaskID: task.ID, WorkspaceID: task.WorkspaceID,
		AgentID: task.AgentID, BrokerKey: s.brokerKey,
	})
	// Claim the whole directory rather than replacing an existing manifest.
	// A crash remnant also requires explicit recovery; it is never overwritten.
	if err := os.Mkdir(record.Directory, 0700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return savedSession{}, ErrSessionExists
		}
		return savedSession{}, err
	}
	if err := s.checkPaths(record); err != nil {
		return savedSession{}, err
	}
	data, err := json.Marshal(sessionManifest{Version: sessionManifestVersion, savedSession: record})
	if err != nil {
		return savedSession{}, err
	}
	file, err := os.CreateTemp(record.Directory, ".session-*.tmp")
	if err != nil {
		return savedSession{}, err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(append(data, '\n')); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return savedSession{}, err
	}
	manifestPath := filepath.Join(record.Directory, sessionManifestName)
	if _, err := os.Lstat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return savedSession{}, ErrSessionExists
		}
		return savedSession{}, err
	}
	if err := os.Rename(name, manifestPath); err != nil {
		return savedSession{}, err
	}
	if err := syncSessionDirectory(record.Directory); err != nil {
		return savedSession{}, err
	}
	if err := syncSessionDirectory(s.directory); err != nil {
		return savedSession{}, err
	}
	return record, nil
}

func (s *sessionStore) Load(task model.Task) (savedSession, error) {
	if err := validateSessionTask(task); err != nil {
		return savedSession{}, err
	}
	record := s.paths(savedSession{TaskID: task.ID, BrokerKey: s.brokerKey})
	if err := s.checkPaths(record); err != nil {
		return savedSession{}, err
	}
	path := filepath.Join(record.Directory, sessionManifestName)
	if err := checkSessionFile(path, false); err != nil {
		return savedSession{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return savedSession{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSessionManifest+1))
	if err != nil {
		return savedSession{}, err
	}
	if len(data) > maxSessionManifest {
		return savedSession{}, errors.New("saved session manifest exceeds size limit")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var manifest sessionManifest
	if err := decoder.Decode(&manifest); err != nil {
		return savedSession{}, fmt.Errorf("saved session manifest: %w", err)
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return savedSession{}, errors.New("saved session manifest must contain one JSON object")
	}
	if manifest.Version != sessionManifestVersion {
		return savedSession{}, errors.New("unsupported saved session version")
	}
	record = manifest.savedSession
	if record.BrokerKey != s.brokerKey || record.TaskID != task.ID || record.WorkspaceID != task.WorkspaceID || record.AgentID != task.AgentID {
		return savedSession{}, errors.New("saved session does not match this broker, task, workspace and agent")
	}
	parsed, err := uuid.Parse(record.ID)
	if err != nil || parsed == uuid.Nil || parsed.String() != record.ID {
		return savedSession{}, errors.New("saved session ID must be a canonical nonzero UUID")
	}
	return s.paths(record), nil
}

func (s *sessionStore) Remove(record savedSession) error {
	if record.BrokerKey != s.brokerKey {
		return errors.New("saved session belongs to a different broker")
	}
	if err := validateSessionTask(model.Task{ID: record.TaskID, WorkspaceID: record.WorkspaceID, AgentID: record.AgentID}); err != nil {
		return err
	}
	expected := s.paths(record)
	if !sameSessionPath(record.Directory, expected.Directory) || !sameSessionPath(record.MCPConfig, expected.MCPConfig) {
		return errors.New("saved session paths do not match its task directory")
	}
	if err := checkSessionDirectory(s.directory); err != nil {
		return err
	}
	if _, err := os.Lstat(expected.Directory); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	current, err := s.Load(model.Task{ID: record.TaskID, WorkspaceID: record.WorkspaceID, AgentID: record.AgentID})
	if err != nil {
		return err
	}
	if current.ID != record.ID {
		return errors.New("saved session ID changed; refusing to remove its directory")
	}
	// Load checked the derived directory and its manifest. Never use a path
	// supplied by the caller or read from JSON as the recursive removal target.
	if err := os.RemoveAll(expected.Directory); err != nil {
		return err
	}
	return syncSessionDirectory(s.directory)
}

func (s *sessionStore) paths(record savedSession) savedSession {
	key := sha256.Sum256([]byte(s.brokerKey + "\x00" + record.TaskID))
	record.Directory = filepath.Join(s.directory, hex.EncodeToString(key[:]))
	record.MCPConfig = filepath.Join(record.Directory, "mcp.json")
	return record
}

func (s *sessionStore) checkPaths(record savedSession) error {
	if err := checkSessionDirectory(s.directory); err != nil {
		return err
	}
	expected := s.paths(record)
	if !sameSessionPath(record.Directory, expected.Directory) || !sameSessionPath(record.MCPConfig, expected.MCPConfig) {
		return errors.New("saved session path is outside its task directory")
	}
	if err := checkSessionDirectory(record.Directory); err != nil {
		return err
	}
	return checkSessionFile(record.MCPConfig, true)
}

func checkSessionDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("saved session directory must not be a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !sameSessionPath(path, resolved) {
		return errors.New("saved session directory resolves outside its derived path")
	}
	return nil
}

func checkSessionFile(path string, allowMissing bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		if allowMissing && errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("saved session file must be regular and not a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !sameSessionPath(path, resolved) {
		return errors.New("saved session file resolves outside its derived path")
	}
	return nil
}

func sameSessionPath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	relative, err := filepath.Rel(a, b)
	return err == nil && relative == "."
}

func validateSessionTask(task model.Task) error {
	for _, value := range []string{task.ID, task.WorkspaceID, task.AgentID} {
		if value == "" || len(value) > 128 || strings.ContainsRune(value, '\x00') {
			return errors.New("saved session requires bounded task, workspace and agent identifiers")
		}
	}
	return nil
}

func syncSessionDirectory(path string) error {
	// Windows does not expose directory fsync through os.File. The manifest
	// itself was synced before its same-directory rename on every platform.
	if runtime.GOOS == "windows" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
