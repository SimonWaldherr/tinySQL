package engine

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Includes OpenDB and the first SELECT; fixture creation and Close are excluded.
// OS file caches are warm: this measures process-level restart, not cold media.
func BenchmarkKeyValueReopenFirstQuery(b *testing.B) {
	for _, mode := range []storage.StorageMode{storage.ModeDisk, storage.ModePagedIndex} {
		for _, profile := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/profile=%t", mode.String(), profile), func(b *testing.B) {
				cfg := storage.DefaultStorageConfig(mode)
				cfg.Path = filepath.Join(b.TempDir(), "cold.db")
				db, err := storage.OpenDB(cfg)
				if err != nil {
					b.Fatal(err)
				}
				ddl := "CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB)"
				if profile {
					ddl = "CREATE VIRTUAL TABLE kv USING keyvalue"
				}
				if _, err := Execute(b.Context(), db, "default", mustParse(ddl)); err != nil {
					b.Fatal(err)
				}
				table, _ := db.Get("default", "kv")
				for i := 0; i < 20000; i++ {
					table.Rows = append(table.Rows, []any{fmt.Sprint(i), []byte("value")})
				}
				table.Version++
				if err := table.RebuildSecondaryIndexes(); err != nil {
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				stmt := mustParse("SELECT value FROM kv WHERE key = '10000'")
				b.ReportAllocs()
				for b.Loop() {
					reopened, err := storage.OpenDB(cfg)
					if err != nil {
						b.Fatal(err)
					}
					rs, err := Execute(b.Context(), reopened, "default", stmt)
					if err != nil || len(rs.Rows) != 1 {
						b.Fatalf("lookup: %v %v", rs, err)
					}
					b.StopTimer()
					if err := reopened.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}
