package engine

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestSQLitePragmaAssignmentIsRejected pins the fix for a silent no-op: the
// parser accepts `PRAGMA x = y` and stores y, but nothing ever read it, so a
// SQLite durability preamble reported success and changed nothing. Accepting
// and then ignoring a durability setting is worse than refusing it, so the
// assignment form now errors and names the DSN option that owns the same knob.
func TestSQLitePragmaAssignmentIsRejected(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	tests := []struct {
		sql string
		// wantSubstrings must all appear in the error, so the test fails if the
		// DSN pointer is dropped rather than merely reworded.
		wantSubstrings []string
	}{
		{"PRAGMA journal_mode = WAL", []string{"read-only in tinySQL", "mode="}},
		{"PRAGMA synchronous = FULL", []string{"read-only in tinySQL", "wal_sync="}},
		{"PRAGMA foreign_keys = OFF", []string{"read-only in tinySQL", "unconditional"}},
		{"PRAGMA foreign_keys = ON", []string{"read-only in tinySQL", "unconditional"}},
		{"PRAGMA cache_size = 2000", []string{"read-only in tinySQL", "max_memory_bytes="}},
		// A pragma with no DSN equivalent still has to fail rather than
		// pretend; the generic message is enough for it.
		{"PRAGMA user_version = 7", []string{"read-only in tinySQL"}},
		// Rejected before the "unsupported PRAGMA" arm is reached, because
		// "you cannot set this" is the more useful of the two answers.
		{"PRAGMA locking_mode = EXCLUSIVE", []string{"read-only in tinySQL"}},
	}
	for _, tc := range tests {
		rs, err := Execute(ctx, db, "main", mustParseSys(tc.sql))
		if err == nil {
			t.Fatalf("%s unexpectedly succeeded: %#v", tc.sql, rs)
		}
		for _, want := range tc.wantSubstrings {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%s error = %q, want it to mention %q", tc.sql, err.Error(), want)
			}
		}
	}
}

// TestSQLitePragmaReadFormsStillWork guards the other half of that fix: only
// the `= value` form may error, every read form must be unaffected.
func TestSQLitePragmaReadFormsStillWork(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	for _, sql := range []string{
		"PRAGMA journal_mode",
		"PRAGMA foreign_keys",
		"PRAGMA user_version",
		"PRAGMA table_info(users)",
		"PRAGMA table_list",
		"PRAGMA database_list",
		"PRAGMA integrity_check",
		"PRAGMA compile_options",
	} {
		if _, err := Execute(ctx, db, "main", mustParseSys(sql)); err != nil {
			t.Fatalf("%s failed: %v", sql, err)
		}
	}
}

// TestSQLiteJournalModeMapping pins what journal_mode is allowed to promise.
// journal_mode is what tooling reads to decide whether a crash leaves a
// recoverable database behind, so no mode may report a stronger guarantee than
// it actually delivers.
func TestSQLiteJournalModeMapping(t *testing.T) {
	tests := []struct {
		mode storage.StorageMode
		want string
	}{
		{storage.ModeMemory, "memory"},
		{storage.ModeWAL, "wal"},
		{storage.ModeAdvancedWAL, "wal"},
		// ModeSQLite's file really is a WAL SQLite database -- the backend runs
		// PRAGMA journal_mode=WAL on it -- so reporting anything else was
		// simply wrong. It used to fall through to the default arm.
		{storage.ModeSQLite, "wal"},
		// These snapshot whole tables: no rollback journal, no crash-atomic
		// multi-table commit. The former "delete" claimed a journal that would
		// be replayed or rolled back on restart; "off" is SQLite's own
		// documented value for "no rollback journal, ROLLBACK undefined".
		{storage.ModeDisk, "off"},
		{storage.ModeJSON, "off"},
		{storage.ModeIndex, "off"},
		{storage.ModeHybrid, "off"},
		{storage.ModePagedIndex, "off"},
	}
	for _, tc := range tests {
		if got := sqliteJournalMode(tc.mode); got != tc.want {
			t.Fatalf("sqliteJournalMode(%v) = %q, want %q", tc.mode, got, tc.want)
		}
	}
}

// TestSQLiteMasterExposesOnlySQLiteColumns pins the column set of
// sqlite_master/sqlite_schema. It used to carry seven columns -- tinySQL's
// "schema" and "full_name" were mixed in among SQLite's five -- which shifted
// every ordinal a positional reader depends on. The richer form moved to
// tinysql_schema.
func TestSQLiteMasterExposesOnlySQLiteColumns(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	for _, table := range []string{"sqlite_master", "sqlite_schema"} {
		rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM "+table))
		if err != nil {
			t.Fatalf("SELECT * FROM %s failed: %v", table, err)
		}
		if len(rs.Rows) == 0 {
			t.Fatalf("%s returned no rows", table)
		}
		// The order of rs.Cols is deliberately not asserted here: star
		// expansion derives it by ranging over the Row map, so it is
		// randomized for every map-backed virtual source. The column *set* is
		// what sqlite_compat.go controls; TestSQLiteMasterExplicitColumnOrder
		// covers the order for an explicit projection.
		want := map[string]bool{"type": true, "name": true, "tbl_name": true, "rootpage": true, "sql": true}
		if len(rs.Cols) != len(want) {
			t.Fatalf("%s columns = %v, want exactly the 5 SQLite columns", table, rs.Cols)
		}
		for _, c := range rs.Cols {
			if !want[c] {
				t.Fatalf("%s exposes non-SQLite column %q (cols=%v)", table, c, rs.Cols)
			}
		}
		for _, row := range rs.Rows {
			if _, leaked := row["schema"]; leaked {
				t.Fatalf("%s row still carries tinySQL 'schema' column: %#v", table, row)
			}
			if _, leaked := row["full_name"]; leaked {
				t.Fatalf("%s row still carries tinySQL 'full_name' column: %#v", table, row)
			}
		}
	}
}

// TestSQLiteMasterExplicitColumnOrder documents the ordering that IS under
// sqlite_compat.go's control: an explicit projection of SQLite's five columns
// comes back in SQLite's ordinal order, rootpage at index 3 and sql at index 4
// (rootpage sat at index 5 while schema/full_name were interleaved).
func TestSQLiteMasterExplicitColumnOrder(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main",
		mustParseSys("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master WHERE name = 'users'"))
	if err != nil {
		t.Fatalf("SELECT sqlite_master failed: %v", err)
	}
	if want := []string{"type", "name", "tbl_name", "rootpage", "sql"}; !reflect.DeepEqual(rs.Cols, want) {
		t.Fatalf("sqlite_master columns = %v, want %v", rs.Cols, want)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected one users row, got %#v", rs.Rows)
	}
	if rs.Rows[0]["rootpage"] != 0 {
		t.Fatalf("rootpage = %#v, want 0", rs.Rows[0]["rootpage"])
	}

	// The alias qualifiers are attached after the narrowing projection, so an
	// aliased reference must still resolve against the five surviving columns.
	rs, err = Execute(ctx, db, "main",
		mustParseSys("SELECT m.type, m.name FROM sqlite_master m WHERE m.name = 'users'"))
	if err != nil {
		t.Fatalf("aliased sqlite_master failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["m.type"] != "table" {
		t.Fatalf("aliased sqlite_master rows = %#v", rs.Rows)
	}
}

// TestTinySQLSchemaKeepsRicherColumns is the other half of the sqlite_master
// narrowing: schema/full_name were moved, not deleted, so schema-qualified
// objects stay discoverable under a name that cannot be confused with SQLite's.
func TestTinySQLSchemaKeepsRicherColumns(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	for _, table := range []string{"tinysql_schema", "tinysql_master"} {
		rs, err := Execute(ctx, db, "main",
			mustParseSys("SELECT type, name, tbl_name, rootpage, sql, schema, full_name FROM "+table+" WHERE name = 'users'"))
		if err != nil {
			t.Fatalf("SELECT %s failed: %v", table, err)
		}
		if len(rs.Rows) != 1 {
			t.Fatalf("%s: expected one users row, got %#v", table, rs.Rows)
		}
		row := rs.Rows[0]
		if row["schema"] != "main" || row["full_name"] != "users" {
			t.Fatalf("%s: schema/full_name = %#v / %#v, want main / users", table, row["schema"], row["full_name"])
		}
		if row["type"] != "table" || row["tbl_name"] != "users" {
			t.Fatalf("%s: unexpected row %#v", table, row)
		}
	}
}

// TestPragmaTableListSurvivesSQLiteMasterNarrowing exists because PRAGMA
// table_list and its ncol column read "schema"/"full_name" off the same
// catalogue rows sqlite_master is built from. Narrowing sqlite_master must not
// narrow the rows those internal consumers see.
func TestPragmaTableListSurvivesSQLiteMasterNarrowing(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("PRAGMA table_list"))
	if err != nil {
		t.Fatalf("PRAGMA table_list failed: %v", err)
	}
	for _, row := range rs.Rows {
		if row["name"] != "users" {
			continue
		}
		if row["schema"] != "main" {
			t.Fatalf("table_list schema = %#v, want main", row["schema"])
		}
		if row["ncol"] != 3 {
			t.Fatalf("table_list ncol = %#v, want 3 (schema/full_name lookup broke)", row["ncol"])
		}
		return
	}
	t.Fatalf("users missing from table_list: %#v", rs.Rows)
}
