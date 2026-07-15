package tinysql_test

import (
	"context"
	"errors"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

type testExtension struct {
	info       tsql.ExtensionInfo
	register   func(*tsql.DB) error
	registries int
}

func (e *testExtension) ExtensionInfo() tsql.ExtensionInfo { return e.info }

func (e *testExtension) Register(db *tsql.DB) error {
	e.registries++
	if e.register != nil {
		return e.register(db)
	}
	return nil
}

func TestDBUseRegistersStaticExtensionAndExposesMetadata(t *testing.T) {
	db := tsql.NewDB()
	extension := &testExtension{info: tsql.ExtensionInfo{
		Name:        "example.audit",
		Version:     "1.2.3",
		Description: "  Audit helpers for example data.  ",
		Capabilities: []tsql.ExtensionCapability{
			tsql.CapabilityWrite,
			tsql.CapabilityNetwork,
			tsql.CapabilityWrite,
		},
	}}

	if err := db.Use(extension); err != nil {
		t.Fatalf("db.Use: %v", err)
	}
	if extension.registries != 1 {
		t.Fatalf("Register calls = %d, want 1", extension.registries)
	}

	infos := db.Extensions()
	if len(infos) != 1 {
		t.Fatalf("Extensions length = %d, want 1", len(infos))
	}
	if got, want := infos[0].Description, "Audit helpers for example data."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := len(infos[0].Capabilities), 2; got != want {
		t.Fatalf("capability count = %d, want %d", got, want)
	}
	if infos[0].LoadedAt.IsZero() {
		t.Fatal("LoadedAt was not set")
	}

	// The result is a copy, not a mutable handle to the DB registry.
	infos[0].Capabilities[0] = "changed"
	if got := string(db.Extensions()[0].Capabilities[0]); got == "changed" {
		t.Fatal("Extensions returned mutable registry data")
	}

	stmt, err := tsql.ParseSQL("SELECT name, version, language, linkage, capabilities FROM sys.extensions")
	if err != nil {
		t.Fatalf("parse sys.extensions query: %v", err)
	}
	rs, err := tsql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("query sys.extensions: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("sys.extensions rows = %#v", rs.Rows)
	}
	row := rs.Rows[0]
	if row["name"] != "example.audit" || row["version"] != "1.2.3" || row["language"] != "GO" || row["linkage"] != "STATIC" {
		t.Fatalf("unexpected sys.extensions row: %#v", row)
	}
	if got, want := row["capabilities"], "network,write"; got != want {
		t.Fatalf("capabilities = %#v, want %q", got, want)
	}

	objectStmt, err := tsql.ParseSQL("SELECT object_type, version FROM sys.objects WHERE name = 'example.audit'")
	if err != nil {
		t.Fatalf("parse sys.objects query: %v", err)
	}
	objects, err := tsql.Execute(context.Background(), db, "default", objectStmt)
	if err != nil {
		t.Fatalf("query sys.objects: %v", err)
	}
	if len(objects.Rows) != 1 || objects.Rows[0]["object_type"] != "EXTENSION" || objects.Rows[0]["version"] != "1.2.3" {
		t.Fatalf("unexpected extension object row: %#v", objects.Rows)
	}
}

func TestDBUseRejectsDuplicateAndFailedExtensions(t *testing.T) {
	db := tsql.NewDB()
	extension := &testExtension{info: tsql.ExtensionInfo{Name: "example.dupe", Version: "1.0.0"}}
	if err := db.Use(extension); err != nil {
		t.Fatalf("initial db.Use: %v", err)
	}
	if err := db.Use(extension); err == nil {
		t.Fatal("duplicate db.Use succeeded")
	}
	if extension.registries != 1 {
		t.Fatalf("duplicate extension Register calls = %d, want 1", extension.registries)
	}

	failed := &testExtension{
		info:     tsql.ExtensionInfo{Name: "example.broken", Version: "1.0.0"},
		register: func(*tsql.DB) error { return errors.New("test failure") },
	}
	if err := db.Use(failed); err == nil {
		t.Fatal("failing db.Use succeeded")
	}
	for _, info := range db.Extensions() {
		if info.Name == "example.broken" {
			t.Fatalf("failed extension remained active: %#v", info)
		}
	}
}
