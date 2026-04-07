// Package scope manages named filesystem mounts (scopes) for FSQL.
// Mounts map a logical name to a filesystem path and are persisted in
// a JSON file under the user's config directory.
package scope

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Mount represents a named filesystem mount point.
type Mount struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// Manager manages persistent named mounts.
type Manager struct {
	mu     sync.RWMutex
	mounts map[string]Mount
	path   string // path to the JSON config file
}

// configFilePath returns the path to the mounts config file.
func configFilePath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		// Fallback to home directory
		dir, err = os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot determine config directory: %w", err)
		}
		dir = filepath.Join(dir, ".fsql")
	} else {
		dir = filepath.Join(dir, "fsql")
	}
	return filepath.Join(dir, "mounts.json"), nil
}

// NewManager creates a new Manager backed by the default config file.
func NewManager() (*Manager, error) {
	cfgPath, err := configFilePath()
	if err != nil {
		return nil, err
	}
	m := &Manager{
		mounts: make(map[string]Mount),
		path:   cfgPath,
	}
	if err := m.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return m, nil
}

// load reads mounts from the config file.
func (m *Manager) load() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	var mounts []Mount
	if err := json.Unmarshal(data, &mounts); err != nil {
		return fmt.Errorf("parse mounts config: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mounts = make(map[string]Mount, len(mounts))
	for _, mt := range mounts {
		m.mounts[strings.ToLower(mt.Name)] = mt
	}
	return nil
}

// save writes mounts to the config file.
func (m *Manager) save() error {
	m.mu.RLock()
	mounts := make([]Mount, 0, len(m.mounts))
	for _, mt := range m.mounts {
		mounts = append(mounts, mt)
	}
	m.mu.RUnlock()

	data, err := json.MarshalIndent(mounts, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal mounts: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(m.path), 0700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	return os.WriteFile(m.path, data, 0600)
}

// Add registers a named mount and persists it.
func (m *Manager) Add(name, path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	if info, err := os.Stat(abs); err != nil {
		return fmt.Errorf("path %q: %w", abs, err)
	} else if !info.IsDir() {
		return fmt.Errorf("path %q is not a directory", abs)
	}

	m.mu.Lock()
	m.mounts[strings.ToLower(name)] = Mount{Name: name, Path: abs}
	m.mu.Unlock()

	return m.save()
}

// Remove deletes a named mount and persists the change.
func (m *Manager) Remove(name string) error {
	key := strings.ToLower(name)
	m.mu.Lock()
	if _, ok := m.mounts[key]; !ok {
		m.mu.Unlock()
		return fmt.Errorf("mount %q not found", name)
	}
	delete(m.mounts, key)
	m.mu.Unlock()
	return m.save()
}

// Get returns the Mount for a given name, or an error if not found.
func (m *Manager) Get(name string) (Mount, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mt, ok := m.mounts[strings.ToLower(name)]
	if !ok {
		return Mount{}, fmt.Errorf("mount %q not found", name)
	}
	return mt, nil
}

// List returns all registered mounts.
func (m *Manager) List() []Mount {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Mount, 0, len(m.mounts))
	for _, mt := range m.mounts {
		out = append(out, mt)
	}
	return out
}

// Resolve resolves a scope name to a filesystem path.
// If name is already an absolute or relative filesystem path, it is returned as-is.
// Otherwise it looks up the named mount.
func (m *Manager) Resolve(name string) (string, error) {
	// If name looks like a path (starts with / . ~), resolve it directly
	if strings.HasPrefix(name, "/") || strings.HasPrefix(name, "./") || strings.HasPrefix(name, "../") || strings.HasPrefix(name, "~") {
		if strings.HasPrefix(name, "~") {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			name = filepath.Join(home, name[1:])
		}
		return filepath.Abs(name)
	}
	mt, err := m.Get(name)
	if err != nil {
		return "", err
	}
	return mt.Path, nil
}
