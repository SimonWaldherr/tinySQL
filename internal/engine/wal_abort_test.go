package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFailedStatementsReleaseAdvancedWALTransaction(t *testing.T) {
	for _, failure := range []string{"constraint", "panic"} {
		t.Run(failure, func(t *testing.T) {
			db := storage.NewDB()
			execSQL(t, db, `CREATE TABLE items (id INT PRIMARY KEY)`)
			dir := t.TempDir()
			wal, err := storage.OpenAdvancedWAL(storage.AdvancedWALConfig{
				Path: filepath.Join(dir, "log.wal"), CheckpointPath: filepath.Join(dir, "checkpoint.gob"),
			})
			if err != nil {
				t.Fatal(err)
			}
			db.AttachAdvancedWAL(wal)
			t.Cleanup(func() { db.Close() })
			query := `INSERT INTO items VALUES (1), (1)`
			if failure == "panic" {
				name := "wal_abort_panic_test"
				if err := RegisterStoredProcedureWithOptions(name, StoredProcedureOptions{Atomic: true}, func(pc ProcedureContext, _ []any) (*ResultSet, error) {
					if _, err := pc.ExecuteSQL(`INSERT INTO items VALUES (1)`); err != nil {
						return nil, err
					}
					panic("injected procedure panic")
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { UnregisterStoredProcedure(name) })
				query = "CALL " + name + "()"
			}
			if _, err := Execute(context.Background(), db, "default", mustParse(query)); err == nil {
				t.Fatal("expected statement failure")
			}
			if got := execSQL(t, db, `SELECT COUNT(*) AS n FROM items`).Rows[0]["n"]; got != 0 {
				t.Fatalf("failed statement retained rows: %v", got)
			}
			if err := wal.Checkpoint(db); err != nil {
				t.Fatalf("failed statement blocked checkpoint: %v", err)
			}
			execSQL(t, db, `INSERT INTO items VALUES (2)`)
			if err := wal.Checkpoint(db); err != nil {
				t.Fatalf("subsequent statement checkpoint: %v", err)
			}
		})
	}
}
