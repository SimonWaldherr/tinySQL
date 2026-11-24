package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A statement that changes no row must leave the table's version and
// structVersion alone: both feed the invalidation of the full-text and vector
// structures, and of every cache keyed on Version.
func TestNoOpDeleteKeepsVersions(t *testing.T) {
	cases := []struct {
		name  string
		setup []string
		stmt  string // matches nothing
		real  string // deletes exactly one row
	}{
		{
			name:  "point delete by primary key",
			setup: []string{`CREATE TABLE nd (id INT PRIMARY KEY, x INT)`},
			stmt:  `DELETE FROM nd WHERE id = 999`,
			real:  `DELETE FROM nd WHERE id = 2`,
		},
		{
			name:  "raw predicate fast path",
			setup: []string{`CREATE TABLE nd (id INT, x INT)`},
			stmt:  `DELETE FROM nd WHERE x = 999`,
			real:  `DELETE FROM nd WHERE x = 2`,
		},
		{
			name: "slow path with a trigger",
			setup: []string{
				`CREATE TABLE nd (id INT, x INT)`,
				`CREATE TABLE nd_log (id INT)`,
				`CREATE TRIGGER nd_del AFTER DELETE ON nd BEGIN INSERT INTO nd_log VALUES (OLD.id); END`,
			},
			stmt: `DELETE FROM nd WHERE x = 999`,
			real: `DELETE FROM nd WHERE x = 2`,
		},
		{
			name:  "complex predicate",
			setup: []string{`CREATE TABLE nd (id INT, x INT)`},
			stmt:  `DELETE FROM nd WHERE x + id = 999 OR x * 0 = 1`,
			real:  `DELETE FROM nd WHERE x + id = 4 OR x * 0 = 1`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			for _, stmt := range tc.setup {
				execSQL(t, db, stmt)
			}
			for i := 1; i <= 4; i++ {
				execSQL(t, db, fmt.Sprintf(`INSERT INTO nd VALUES (%d, %d)`, i, i))
			}
			table, err := db.Get("default", "nd")
			if err != nil {
				t.Fatal(err)
			}
			version, structVersion := table.Version, table.StructVersion()
			rs := execSQL(t, db, tc.stmt)
			if rs.Rows[0]["deleted"] != 0 {
				t.Fatalf("setup error: %s deleted %v rows", tc.stmt, rs.Rows[0]["deleted"])
			}
			if table.Version != version || table.StructVersion() != structVersion {
				t.Fatalf("a DELETE that matched nothing moved versions: (%d,%d) -> (%d,%d)",
					version, structVersion, table.Version, table.StructVersion())
			}
			if len(table.Rows) != 4 {
				t.Fatalf("rows = %d, want 4", len(table.Rows))
			}
			execSQL(t, db, tc.real)
			if len(table.Rows) != 3 || table.Version == version || table.StructVersion() == structVersion {
				t.Fatalf("a real DELETE must advance both versions: rows=%d version %d->%d struct %d->%d",
					len(table.Rows), version, table.Version, structVersion, table.StructVersion())
			}
		})
	}
}

// The FTS index and the vector column cache survive a no-op DELETE untouched.
func TestNoOpDeleteKeepsSearchStructures(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE nds (id INT, body TEXT, embedding VECTOR)`)
	for i := 0; i < 40; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO nds VALUES (%d, 'alpha term%d', '[%d, 1, 2]')`, i, i%5, i+1))
	}
	execSQL(t, db, `SELECT * FROM FTS_WARM('nds', 'body')`)
	execSQL(t, db, `SELECT * FROM VEC_WARM('nds', 'embedding')`)
	table, err := db.Get("default", "nds")
	if err != nil {
		t.Fatal(err)
	}
	index := table.FTSIndexes["1"]
	vecBefore := getVecColumnCache("default", table, 2, false)
	execSQL(t, db, `DELETE FROM nds WHERE id = 12345`)
	_ = execSQL(t, db, `SELECT id FROM FTS_SEARCH('nds', 'alpha', 3, 'body')`)
	vecAfter := getVecColumnCache("default", table, 2, false)
	if table.FTSIndexes["1"] != index {
		t.Fatal("a no-op DELETE replaced the FTS index")
	}
	if &vecAfter.segments[0].data[0] != &vecBefore.segments[0].data[0] {
		t.Fatal("a no-op DELETE rebuilt the vector column")
	}
}

// CREATE INDEX and DROP INDEX change no row: the table is dirty for the WAL and
// persistence as a whole, but the structural signal and the search structures
// stay.
func TestIndexDDLKeepsRowsStructure(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE idxd (id INT, body TEXT)`)
	for i := 0; i < 30; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO idxd VALUES (%d, 'alpha beta%d')`, i, i%4))
	}
	execSQL(t, db, `SELECT * FROM FTS_WARM('idxd', 'body')`)
	table, err := db.Get("default", "idxd")
	if err != nil {
		t.Fatal(err)
	}
	index := table.FTSIndexes["1"]
	version, structVersion := table.Version, table.StructVersion()

	execSQL(t, db, `CREATE INDEX idxd_id ON idxd (id)`)
	if table.Version == version {
		t.Fatal("CREATE INDEX must still advance the version so the WAL diff sees it")
	}
	if table.StructVersion() != structVersion {
		t.Fatalf("CREATE INDEX advanced structVersion %d -> %d", structVersion, table.StructVersion())
	}
	if table.DirtyFrom() != -1 {
		t.Fatalf("CREATE INDEX left dirtyFrom = %d, want the full-table sentinel", table.DirtyFrom())
	}
	if rows, exact := table.DirtyRows(); exact {
		t.Fatalf("CREATE INDEX left an exact row list %v", rows)
	}
	_ = execSQL(t, db, `SELECT id FROM FTS_SEARCH('idxd', 'alpha', 3, 'body')`)
	if table.FTSIndexes["1"] != index {
		t.Fatal("CREATE INDEX discarded the FTS index")
	}

	version = table.Version
	execSQL(t, db, `DROP INDEX idxd_id`)
	if table.Version == version || table.StructVersion() != structVersion || table.DirtyFrom() != -1 {
		t.Fatalf("DROP INDEX: version %d->%d, struct %d->%d, dirtyFrom %d", version, table.Version, structVersion, table.StructVersion(), table.DirtyFrom())
	}
	_ = execSQL(t, db, `SELECT id FROM FTS_SEARCH('idxd', 'alpha', 3, 'body')`)
	if table.FTSIndexes["1"] != index {
		t.Fatal("DROP INDEX discarded the FTS index")
	}
	// ALTER ... ADD COLUMN may still invalidate: row shapes changed.
	execSQL(t, db, `ALTER TABLE idxd ADD COLUMN extra INT`)
	if table.StructVersion() == structVersion {
		t.Fatal("ADD COLUMN must keep invalidating")
	}
}
