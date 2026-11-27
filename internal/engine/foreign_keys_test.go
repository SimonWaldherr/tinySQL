// Tests for foreign key referential actions (internal/engine/foreign_keys.go).
// Before this feature, DELETE/UPDATE on a referenced row silently orphaned
// any child rows — no RESTRICT, no CASCADE, no SET NULL, and the standard
// "col TYPE REFERENCES tbl(col)" syntax didn't even parse (only tinySQL's
// own "col TYPE FOREIGN KEY REFERENCES tbl(col)" spelling worked).
package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestForeignKeyStandardReferencesSyntaxParses(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	// This is the syntax that previously failed with a confusing
	// "expected symbol )" error, because parseReferencesConstraint used to
	// be a no-op for any column type other than POINTER.
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	rs, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO child VALUES (2, 99)`))
	if err == nil {
		t.Fatalf("expected FK violation inserting a non-existent parent_id, got rs=%+v", rs)
	}
}

func TestForeignKeyTableLevelSyntaxParses(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (
		id INT,
		parent_id INT,
		FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE
	)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)
	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM child`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 0 {
		t.Errorf("expected table-level FOREIGN KEY ON DELETE CASCADE to remove the child row, got %d rows left", got)
	}
}

func TestForeignKeyDefaultRestrictsDelete(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	stmt := mustParse(`DELETE FROM parent WHERE id = 1`)
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("expected default (no ON DELETE clause) to RESTRICT the delete, got no error")
	}

	// Confirm nothing was mutated: neither row was touched.
	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM parent`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 1 {
		t.Errorf("expected parent row to survive a RESTRICTed delete, got %d rows", got)
	}
	rs = execSQL(t, db, `SELECT COUNT(*) AS cnt FROM child`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 1 {
		t.Errorf("expected child row to be untouched by a RESTRICTed delete, got %d rows", got)
	}
}

func TestForeignKeyDeleteChecksColdChildTable(t *testing.T) {
	config := storage.StorageConfig{Mode: storage.ModeDisk, Path: t.TempDir()}
	db, err := storage.OpenDB(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (parent_id INT REFERENCES parent(id))`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1)`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = storage.OpenDB(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if child := storage.ResidentTable(db, "default", "child"); child != nil {
		t.Fatal("expected the child table to require a backend lookup")
	}
	stmt := mustParse(`DELETE FROM parent WHERE id = 1`)
	if _, err := Execute(t.Context(), db, "default", stmt); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("delete returned %v, want a foreign key violation", err)
	}
	rows := execSQL(t, db, `SELECT id FROM parent`).Rows
	if len(rows) != 1 || rows[0]["id"] != 1 {
		t.Fatalf("expected the parent row to survive a restricted delete, got %v", rows)
	}
}

func TestForeignKeyExplicitNoActionRestrictsDelete(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE NO ACTION)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	stmt := mustParse(`DELETE FROM parent WHERE id = 1`)
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("expected explicit ON DELETE NO ACTION to restrict the delete, got no error")
	}
}

func TestForeignKeyDeleteWithNoMatchingChildSucceeds(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO parent VALUES (2)`)
	// No child row references parent id=2 at all.
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 2`)

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM parent`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 1 {
		t.Errorf("expected deleting an unreferenced parent row to succeed, got %d parent rows left", got)
	}
}

func TestForeignKeyOnDeleteCascade(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO parent VALUES (2)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)
	execSQL(t, db, `INSERT INTO child VALUES (2, 1)`)
	execSQL(t, db, `INSERT INTO child VALUES (3, 2)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM child`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 1 {
		t.Errorf("expected ON DELETE CASCADE to remove both children of parent 1, leaving 1 row, got %d", got)
	}
	rs = execSQL(t, db, `SELECT parent_id FROM child`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["parent_id"]) != 2 {
		t.Errorf("expected the surviving child row to reference parent 2, got %+v", rs.Rows)
	}
}

func TestForeignKeyOnDeleteCascadeTransitive(t *testing.T) {
	// grandchild -> child -> parent, both FKs ON DELETE CASCADE. Deleting the
	// parent must cascade through child into grandchild too, not just one
	// level deep.
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)`)
	execSQL(t, db, `CREATE TABLE grandchild (id INT, child_id INT REFERENCES child(id) ON DELETE CASCADE)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1)`)
	execSQL(t, db, `INSERT INTO grandchild VALUES (100, 10)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM child`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 0 {
		t.Errorf("expected child row to be cascade-deleted, got %d left", got)
	}
	rs = execSQL(t, db, `SELECT COUNT(*) AS cnt FROM grandchild`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 0 {
		t.Errorf("expected grandchild row to be transitively cascade-deleted, got %d left", got)
	}
}

func TestForeignKeyOnDeleteSetNull(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE SET NULL)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)

	rs := execSQL(t, db, `SELECT parent_id FROM child WHERE id = 1`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected the child row to survive, got %+v", rs.Rows)
	}
	if rs.Rows[0]["parent_id"] != nil {
		t.Errorf("expected ON DELETE SET NULL to null parent_id, got %v", rs.Rows[0]["parent_id"])
	}
}

func TestForeignKeyOnUpdateCascade(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON UPDATE CASCADE)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	execSQL(t, db, `UPDATE parent SET id = 999 WHERE id = 1`)

	rs := execSQL(t, db, `SELECT parent_id FROM child WHERE id = 1`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected the child row to survive, got %+v", rs.Rows)
	}
	if got := expectAsInt(t, rs.Rows[0]["parent_id"]); got != 999 {
		t.Errorf("expected ON UPDATE CASCADE to propagate the new id, got parent_id=%v", got)
	}
}

func TestForeignKeyOnUpdateDefaultRestricts(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (1, 1)`)

	stmt := mustParse(`UPDATE parent SET id = 999 WHERE id = 1`)
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("expected default (no ON UPDATE clause) to restrict changing a referenced value, got no error")
	}
	rs := execSQL(t, db, `SELECT id FROM parent`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 1 {
		t.Errorf("expected the restricted UPDATE to leave parent.id unchanged, got %+v", rs.Rows)
	}
}

func TestUpsertUpdatesReferencedKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action string
		child  any
	}{
		{"restrict", "", 1},
		{"cascade", " ON UPDATE CASCADE", 2},
		{"set_null", " ON UPDATE SET NULL", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			defer db.Close()
			execSQL(t, db, "CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)")
			execSQL(t, db, "CREATE TABLE child (pid INT REFERENCES parent(id)"+tc.action+")")
			execSQL(t, db, "INSERT INTO parent VALUES (1, 'p')")
			execSQL(t, db, "INSERT INTO child VALUES (1)")
			s := mustParse("INSERT INTO parent VALUES (2, 'p') ON CONFLICT (code) DO UPDATE SET id = excluded.id")
			_, err := Execute(t.Context(), db, "default", s)
			wantParent := 2
			if tc.name == "restrict" {
				wantParent = 1
				if err == nil {
					t.Fatal("updating a referenced parent key should fail")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			parent := execSQL(t, db, "SELECT id FROM parent")
			child := execSQL(t, db, "SELECT pid FROM child")
			if len(parent.Rows) != 1 || parent.Rows[0]["id"] != wantParent ||
				len(child.Rows) != 1 || child.Rows[0]["pid"] != tc.child {
				t.Fatalf("parent=%v child=%v, want %v/%v", parent.Rows, child.Rows, wantParent, tc.child)
			}
		})
	}
}

func TestUpsertRollsBackCascadeOnChildCheckFailure(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)")
	execSQL(t, db, "CREATE TABLE child (pid INT REFERENCES parent(id) ON UPDATE CASCADE, CHECK (pid < 10))")
	execSQL(t, db, "CREATE INDEX ix_child ON child(pid)")
	execSQL(t, db, "INSERT INTO parent VALUES (1, 'a'), (2, 'b')")
	execSQL(t, db, "INSERT INTO child VALUES (1), (2)")
	stmt := mustParse("INSERT INTO parent VALUES (3, 'a'), (10, 'b') ON CONFLICT (code) DO UPDATE SET id = excluded.id")
	if _, err := Execute(t.Context(), db, "default", stmt); err == nil {
		t.Fatal("cascade should have failed the second child's CHECK")
	}
	for _, query := range []string{
		"SELECT id FROM parent WHERE id IN (1, 2)",
		"SELECT pid FROM child WHERE pid IN (1, 2)",
	} {
		if rs := execSQL(t, db, query); len(rs.Rows) != 2 {
			t.Errorf("after rollback, %s returned %v", query, rs.Rows)
		}
	}
}

func TestUpsertCascadesIntoItsOwnRow(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE node (id INT PRIMARY KEY, parent INT REFERENCES node(id) ON UPDATE CASCADE)")
	execSQL(t, db, "INSERT INTO node VALUES (1, NULL)")
	execSQL(t, db, "UPDATE node SET parent = 1 WHERE id = 1")
	execSQL(t, db, "INSERT INTO node VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET id = 2")
	rs := execSQL(t, db, "SELECT parent FROM node WHERE id = 2")
	if len(rs.Rows) != 1 || rs.Rows[0]["parent"] != 2 {
		t.Fatalf("self-reference after key change = %v", rs.Rows)
	}
}

func TestUpsertRollsBackUpdateTriggerWrites(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE parent (id INT PRIMARY KEY, n INT CHECK (n >= 0))")
	execSQL(t, db, "CREATE TABLE audit (id INT)")
	execSQL(t, db, "CREATE TRIGGER audit_updates AFTER UPDATE ON parent BEGIN INSERT INTO audit VALUES (NEW.id); END")
	execSQL(t, db, "INSERT INTO parent VALUES (1, 1)")
	stmt := mustParse("INSERT INTO parent VALUES (1, 2), (1, -1) ON CONFLICT (id) DO UPDATE SET n = excluded.n")
	if _, err := Execute(t.Context(), db, "default", stmt); err == nil {
		t.Fatal("expected CHECK failure")
	}
	if rs := execSQL(t, db, "SELECT id FROM audit"); len(rs.Rows) != 0 {
		t.Fatalf("failed upsert left audit records %v", rs.Rows)
	}
}

func TestForeignKeyNoForeignKeysAnywhereUnaffected(t *testing.T) {
	// tenantHasAnyForeignKeys must short-circuit to true no-op when the
	// tenant has no FK columns at all, so ordinary DELETE/UPDATE keep using
	// their existing fast paths with zero behavior change.
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE plain (id INT, val INT)`)
	execSQL(t, db, `INSERT INTO plain VALUES (1, 10)`)
	execSQL(t, db, `INSERT INTO plain VALUES (2, 20)`)

	execSQL(t, db, `UPDATE plain SET val = 99 WHERE id = 1`)
	execSQL(t, db, `DELETE FROM plain WHERE id = 2`)

	rs := execSQL(t, db, `SELECT * FROM plain`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row left, got %+v", rs.Rows)
	}
	if got := expectAsInt(t, rs.Rows[0]["val"]); got != 99 {
		t.Errorf("expected val=99 after UPDATE, got %v", got)
	}
}

func TestForeignKeySetNullMaintainsSecondaryIndexes(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE SET NULL)`)
	execSQL(t, db, `CREATE UNIQUE INDEX idx_child_parent_id ON child(parent_id)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	// The referential SET NULL must release the old index key. Before this
	// regression, the materialized index still held key 1 and rejected this
	// otherwise valid row as a duplicate.
	execSQL(t, db, `INSERT INTO child VALUES (11, 1)`)

	rs := execSQL(t, db, `SELECT id FROM child WHERE parent_id = 1`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 11 {
		t.Fatalf("indexed lookup after ON DELETE SET NULL = %#v", rs.Rows)
	}
}

func TestForeignKeyCascadeMaintainsSecondaryIndexRowIDs(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)`)
	execSQL(t, db, `CREATE INDEX idx_child_id ON child(id)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1), (2)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1), (20, 2)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)
	// Child id=20 is compacted from row position 1 to 0. Its index entry must
	// be remapped too, otherwise an indexed lookup points beyond child.Rows.
	rs := execSQL(t, db, `SELECT parent_id FROM child WHERE id = 20`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["parent_id"]) != 2 {
		t.Fatalf("indexed lookup after ON DELETE CASCADE = %#v", rs.Rows)
	}
}

func TestForeignKeyUpdateCascadeMaintainsSecondaryIndexes(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id) ON UPDATE CASCADE)`)
	execSQL(t, db, `CREATE INDEX idx_child_parent_id ON child(parent_id)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1)`)

	execSQL(t, db, `UPDATE parent SET id = 2 WHERE id = 1`)
	rs := execSQL(t, db, `SELECT id FROM child WHERE parent_id = 2`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 10 {
		t.Fatalf("indexed lookup after ON UPDATE CASCADE = %#v", rs.Rows)
	}
}

func TestForeignKeySetNullRejectsNotNullChildColumn(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT, parent_id INT NOT NULL REFERENCES parent(id) ON DELETE SET NULL)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1)`)

	stmt := mustParse(`DELETE FROM parent WHERE id = 1`)
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("expected ON DELETE SET NULL to reject a NOT NULL child column")
	}
	parent := execSQL(t, db, `SELECT id FROM parent`)
	child := execSQL(t, db, `SELECT parent_id FROM child`)
	if len(parent.Rows) != 1 || len(child.Rows) != 1 || expectAsInt(t, child.Rows[0]["parent_id"]) != 1 {
		t.Fatalf("failed SET NULL action mutated rows: parent=%#v child=%#v", parent.Rows, child.Rows)
	}
}

func TestForeignKeyUpdateCascadePropagatesTransitively(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON UPDATE CASCADE)`)
	execSQL(t, db, `CREATE TABLE grandchild (id INT, child_parent_id INT REFERENCES child(parent_id) ON UPDATE CASCADE)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1)`)
	execSQL(t, db, `INSERT INTO grandchild VALUES (100, 1)`)

	execSQL(t, db, `UPDATE parent SET id = 2 WHERE id = 1`)
	child := execSQL(t, db, `SELECT parent_id FROM child WHERE id = 10`)
	grandchild := execSQL(t, db, `SELECT child_parent_id FROM grandchild WHERE id = 100`)
	if len(child.Rows) != 1 || expectAsInt(t, child.Rows[0]["parent_id"]) != 2 {
		t.Fatalf("child update cascade = %#v", child.Rows)
	}
	if len(grandchild.Rows) != 1 || expectAsInt(t, grandchild.Rows[0]["child_parent_id"]) != 2 {
		t.Fatalf("grandchild update cascade = %#v", grandchild.Rows)
	}
}

func TestForeignKeySetNullPropagatesDownstreamUpdateAction(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE SET NULL)`)
	execSQL(t, db, `CREATE TABLE grandchild (id INT, child_parent_id INT REFERENCES child(parent_id) ON UPDATE CASCADE)`)
	execSQL(t, db, `INSERT INTO parent VALUES (1)`)
	execSQL(t, db, `INSERT INTO child VALUES (10, 1)`)
	execSQL(t, db, `INSERT INTO grandchild VALUES (100, 1)`)

	execSQL(t, db, `DELETE FROM parent WHERE id = 1`)
	child := execSQL(t, db, `SELECT parent_id FROM child WHERE id = 10`)
	grandchild := execSQL(t, db, `SELECT child_parent_id FROM grandchild WHERE id = 100`)
	if len(child.Rows) != 1 || child.Rows[0]["parent_id"] != nil {
		t.Fatalf("child SET NULL action = %#v", child.Rows)
	}
	if len(grandchild.Rows) != 1 || grandchild.Rows[0]["child_parent_id"] != nil {
		t.Fatalf("downstream UPDATE CASCADE after SET NULL = %#v", grandchild.Rows)
	}
}

func TestForeignKeyCascadeCycleFailsWithoutPartialMutation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE node (id INT PRIMARY KEY, parent_id INT REFERENCES node(id) ON DELETE CASCADE)`)
	execSQL(t, db, `INSERT INTO node VALUES (1, NULL)`)
	execSQL(t, db, `INSERT INTO node VALUES (2, 1)`)
	execSQL(t, db, `UPDATE node SET parent_id = 2 WHERE id = 1`)

	stmt := mustParse(`DELETE FROM node WHERE id = 1`)
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("expected cyclic cascade to return an error")
	}
	rs := execSQL(t, db, `SELECT id FROM node`)
	if len(rs.Rows) != 2 {
		t.Fatalf("cyclic cascade partially mutated table: %#v", rs.Rows)
	}
}
