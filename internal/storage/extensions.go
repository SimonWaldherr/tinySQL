package storage

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ExtensionCapability is a declared resource category an extension may need.
// Declarations are metadata only for now; callers can use them to review an
// extension before activation. Enforcement is intentionally left to a future
// capability policy rather than pretending that in-process Go code is
// sandboxed.
type ExtensionCapability string

const (
	CapabilityFilesystem ExtensionCapability = "filesystem"
	CapabilityNetwork    ExtensionCapability = "network"
	CapabilityWrite      ExtensionCapability = "write"
	CapabilitySecrets    ExtensionCapability = "secrets"
)

// ExtensionInfo describes one statically linked Go extension activated for a
// database instance. LoadedAt is set by DB.Use and is not supplied by the
// extension implementation.
type ExtensionInfo struct {
	Name         string
	Version      string
	Description  string
	Capabilities []ExtensionCapability
	LoadedAt     time.Time
}

// Extension is the stable entry point for statically linked Go extensions.
// An extension package is imported by the application and explicitly enabled
// with db.Use. tinySQL intentionally does not use Go's plugin package: its
// shared-object ABI is platform/toolchain-dependent and unavailable to many
// tinySQL targets, including WebAssembly and TinyGo.
//
// Register may register metadata, functions, table functions, storage helpers,
// or other supported public extension points on db. Returning an error leaves
// the extension absent from DB.Extensions and sys.extensions.
type Extension interface {
	ExtensionInfo() ExtensionInfo
	Register(db *DB) error
}

// Use activates one statically linked Go extension for this database. An
// extension name can be activated only once per DB instance. Extensions are
// not unloadable because their registration may affect executing queries or
// other database state.
func (db *DB) Use(extension Extension) error {
	if db == nil {
		return fmt.Errorf("cannot activate extension on nil database")
	}
	if extension == nil {
		return fmt.Errorf("extension is nil")
	}

	info, err := normalizeExtensionInfo(extension.ExtensionInfo())
	if err != nil {
		return err
	}
	key := strings.ToLower(info.Name)

	// Do not hold the registry lock while third-party code runs. Besides
	// avoiding lock inversions, this lets extensions inspect their DB during
	// registration. The loading reservation prevents duplicate concurrent uses.
	db.extensionsMu.Lock()
	if _, exists := db.extensions[key]; exists {
		db.extensionsMu.Unlock()
		return fmt.Errorf("extension %q is already active", info.Name)
	}
	if _, loading := db.loadingExtensions[key]; loading {
		db.extensionsMu.Unlock()
		return fmt.Errorf("extension %q is already being activated", info.Name)
	}
	db.loadingExtensions[key] = struct{}{}
	db.extensionsMu.Unlock()

	err = extension.Register(db)

	db.extensionsMu.Lock()
	defer db.extensionsMu.Unlock()
	delete(db.loadingExtensions, key)
	if err != nil {
		return fmt.Errorf("activate extension %q: %w", info.Name, err)
	}
	info.LoadedAt = time.Now().UTC()
	db.extensions[key] = cloneExtensionInfo(info)
	return nil
}

// Extensions returns a deterministic snapshot of extensions active for this
// DB. Mutating the returned values cannot change the database registry.
func (db *DB) Extensions() []ExtensionInfo {
	if db == nil {
		return nil
	}
	db.extensionsMu.RLock()
	extensions := make([]ExtensionInfo, 0, len(db.extensions))
	for _, info := range db.extensions {
		extensions = append(extensions, cloneExtensionInfo(info))
	}
	db.extensionsMu.RUnlock()
	sort.Slice(extensions, func(i, j int) bool {
		return strings.ToLower(extensions[i].Name) < strings.ToLower(extensions[j].Name)
	})
	return extensions
}

func normalizeExtensionInfo(info ExtensionInfo) (ExtensionInfo, error) {
	info.Name = strings.TrimSpace(info.Name)
	if info.Name == "" {
		return ExtensionInfo{}, fmt.Errorf("extension name is required")
	}
	info.Version = strings.TrimSpace(info.Version)
	if info.Version == "" {
		return ExtensionInfo{}, fmt.Errorf("extension %q version is required", info.Name)
	}
	info.Description = strings.TrimSpace(info.Description)
	info.LoadedAt = time.Time{}
	seen := make(map[ExtensionCapability]struct{}, len(info.Capabilities))
	capabilities := make([]ExtensionCapability, 0, len(info.Capabilities))
	for _, capability := range info.Capabilities {
		capability = ExtensionCapability(strings.ToLower(strings.TrimSpace(string(capability))))
		if capability == "" {
			continue
		}
		if _, exists := seen[capability]; exists {
			continue
		}
		seen[capability] = struct{}{}
		capabilities = append(capabilities, capability)
	}
	sort.Slice(capabilities, func(i, j int) bool { return capabilities[i] < capabilities[j] })
	info.Capabilities = capabilities
	return info, nil
}

func cloneExtensionInfo(info ExtensionInfo) ExtensionInfo {
	info.Capabilities = append([]ExtensionCapability(nil), info.Capabilities...)
	return info
}
