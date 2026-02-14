package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"github.com/SimonWaldherr/tinySQL/internal/storage/pager"

	_ "modernc.org/sqlite"
)

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

func tmpDir(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "tinysql_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// ── tinySQL Storage-Backend wrappers ──────────────────────────────────────

type backendEntry struct {
	name string
	// open returns a save/load/close triplet
	open func(b *testing.B) backendOps
}

type backendOps struct {
	save  func(name string, nRows int) // write a table
	load  func(name string) int        // read table, return row count
	close func()
}

func tinyBackends() []backendEntry {
	return []backendEntry{
		{"tinySQL-Memory", openTinyMemory},
		{"tinySQL-Disk", openTinyDisk},
		{"tinySQL-DiskGzip", openTinyDiskGzip},
		{"tinySQL-Hybrid", openTinyHybrid},
		{"tinySQL-Index", openTinyIndex},
		{"tinySQL-Page", openTinyPage},
		{"SQLite-modernc", openSQLite},
	}
}

// ── tinySQL via storage.OpenDB ────────────────────────────────────────────

func openTinySQLDB(b *testing.B, mode storage.StorageMode) backendOps {
	b.Helper()
	dir := tmpDir(b)
	cfg := storage.DefaultStorageConfig(mode)
	cfg.Path = filepath.Join(dir, "bench.db")
	db, err := tinysql.OpenDB(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	// Pre-create the table schema once.
	createDDL := func(name string) {
		stmt, _ := tinysql.ParseSQL(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (id INT, name STRING, score FLOAT64)", name))
		tinysql.Execute(ctx, db, "default", stmt)
	}

	return backendOps{
		save: func(name string, nRows int) {
			createDDL(name)
			// Truncate existing rows.
			stmt, _ := tinysql.ParseSQL(fmt.Sprintf("DELETE FROM %s WHERE 1=1", name))
			tinysql.Execute(ctx, db, "default", stmt)
			for i := 0; i < nRows; i++ {
				ins, _ := tinysql.ParseSQL(fmt.Sprintf(
					"INSERT INTO %s VALUES (%d, 'user_%d', %f)", name, i, i, float64(i)*1.1))
				tinysql.Execute(ctx, db, "default", ins)
			}
		},
		load: func(name string) int {
			stmt, _ := tinysql.ParseSQL(fmt.Sprintf("SELECT * FROM %s", name))
			rs, err := tinysql.Execute(ctx, db, "default", stmt)
			if err != nil || rs == nil {
				return 0
			}
			return len(rs.Rows)
		},
		close: func() { db.Close() },
	}
}

func openTinyMemory(b *testing.B) backendOps {
	return openTinySQLDB(b, storage.ModeMemory)
}

func openTinyDisk(b *testing.B) backendOps {
	return openTinySQLDB(b, storage.ModeDisk)
}

func openTinyDiskGzip(b *testing.B) backendOps {
	b.Helper()
	dir := tmpDir(b)
	cfg := storage.DefaultStorageConfig(storage.ModeDisk)
	cfg.Path = filepath.Join(dir, "bench.db")
	cfg.CompressFiles = true
	db, err := tinysql.OpenDB(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	createDDL := func(name string) {
		stmt, _ := tinysql.ParseSQL(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (id INT, name STRING, score FLOAT64)", name))
		tinysql.Execute(ctx, db, "default", stmt)
	}
	return backendOps{
		save: func(name string, nRows int) {
			createDDL(name)
			stmt, _ := tinysql.ParseSQL(fmt.Sprintf("DELETE FROM %s WHERE 1=1", name))
			tinysql.Execute(ctx, db, "default", stmt)
			for i := 0; i < nRows; i++ {
				ins, _ := tinysql.ParseSQL(fmt.Sprintf(
					"INSERT INTO %s VALUES (%d, 'user_%d', %f)", name, i, i, float64(i)*1.1))
				tinysql.Execute(ctx, db, "default", ins)
			}
		},
		load: func(name string) int {
			stmt, _ := tinysql.ParseSQL(fmt.Sprintf("SELECT * FROM %s", name))
			rs, err := tinysql.Execute(ctx, db, "default", stmt)
			if err != nil || rs == nil {
				return 0
			}
			return len(rs.Rows)
		},
		close: func() { db.Close() },
	}
}

func openTinyHybrid(b *testing.B) backendOps {
	return openTinySQLDB(b, storage.ModeHybrid)
}

func openTinyIndex(b *testing.B) backendOps {
	return openTinySQLDB(b, storage.ModeIndex)
}

// ── tinySQL PageBackend (direct B+Tree API) ───────────────────────────────

func openTinyPage(b *testing.B) backendOps {
	b.Helper()
	dir := tmpDir(b)
	pb, err := pager.NewPageBackend(pager.PageBackendConfig{
		Path: filepath.Join(dir, "bench.db"),
	})
	if err != nil {
		b.Fatal(err)
	}
	return backendOps{
		save: func(name string, nRows int) {
			cols := []pager.ColumnInfo{
				{Name: "id", Type: 0},
				{Name: "name", Type: 13},
				{Name: "score", Type: 11},
			}
			rows := make([][]any, nRows)
			for i := range rows {
				rows[i] = []any{float64(i), fmt.Sprintf("user_%d", i), float64(i) * 1.1}
			}
			td := &pager.TableData{Name: name, Columns: cols, Rows: rows, Version: 1}
			if err := pb.SaveTable("default", td); err != nil {
				b.Fatal(err)
			}
		},
		load: func(name string) int {
			td, err := pb.LoadTable("default", name)
			if err != nil || td == nil {
				return 0
			}
			return len(td.Rows)
		},
		close: func() { pb.Close() },
	}
}

// ── SQLite via modernc (pure Go) ─────────────────────────────────────────

func openSQLite(b *testing.B) backendOps {
	b.Helper()
	dir := tmpDir(b)
	dbPath := filepath.Join(dir, "bench.sqlite3")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		b.Fatal(err)
	}
	// WAL mode + relaxed sync for fair comparison (tinySQL backends
	// don't fsync on every INSERT either).
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")

	return backendOps{
		save: func(name string, nRows int) {
			db.Exec(fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s (id INTEGER, name TEXT, score REAL)", name))
			db.Exec(fmt.Sprintf("DELETE FROM %s", name))

			tx, _ := db.Begin()
			stmt, _ := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?,?,?)", name))
			for i := 0; i < nRows; i++ {
				stmt.Exec(i, fmt.Sprintf("user_%d", i), float64(i)*1.1)
			}
			stmt.Close()
			tx.Commit()
		},
		load: func(name string) int {
			rows, err := db.Query(fmt.Sprintf("SELECT id, name, score FROM %s", name))
			if err != nil {
				return 0
			}
			defer rows.Close()
			count := 0
			var id int
			var nm string
			var sc float64
			for rows.Next() {
				rows.Scan(&id, &nm, &sc)
				count++
			}
			return count
		},
		close: func() { db.Close() },
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: BulkInsert — write N rows into a table
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkBulkInsert(b *testing.B) {
	rowCounts := []int{10, 100, 1000}

	for _, rc := range rowCounts {
		for _, be := range tinyBackends() {
			b.Run(fmt.Sprintf("%s/rows=%d", be.name, rc), func(b *testing.B) {
				ops := be.open(b)
				defer ops.close()

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					ops.save("bench", rc)
				}
			})
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: FullScan — read all rows from a table
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkFullScan(b *testing.B) {
	rowCounts := []int{10, 100, 1000}

	for _, rc := range rowCounts {
		for _, be := range tinyBackends() {
			b.Run(fmt.Sprintf("%s/rows=%d", be.name, rc), func(b *testing.B) {
				ops := be.open(b)
				defer ops.close()

				// Pre-populate.
				ops.save("scan_target", rc)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					n := ops.load("scan_target")
					if n != rc {
						b.Fatalf("expected %d rows, got %d", rc, n)
					}
				}
			})
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: RoundTrip — write then read back
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkRoundTrip(b *testing.B) {
	for _, be := range tinyBackends() {
		b.Run(be.name, func(b *testing.B) {
			ops := be.open(b)
			defer ops.close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ops.save("rt", 100)
				n := ops.load("rt")
				if n != 100 {
					b.Fatalf("expected 100 rows, got %d", n)
				}
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: SingleInsert — one INSERT per iteration (latency-sensitive)
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkSingleInsert(b *testing.B) {
	for _, be := range tinyBackends() {
		b.Run(be.name, func(b *testing.B) {
			ops := be.open(b)
			defer ops.close()

			// Pre-create with 1 row so the table exists.
			ops.save("single", 1)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ops.save("single", 1)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: PointQuery — lookup single row via SQL WHERE (SQLite) or
//            full scan (others; measures query overhead)
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkPointQuery(b *testing.B) {
	for _, entry := range []struct {
		name string
		open func(b *testing.B) pointQueryOps
	}{
		{"tinySQL-Memory", openTinyPointQuery(storage.ModeMemory)},
		{"tinySQL-Disk", openTinyPointQuery(storage.ModeDisk)},
		{"tinySQL-Page", openPagePointQuery},
		{"SQLite-modernc", openSQLitePointQuery},
	} {
		b.Run(entry.name, func(b *testing.B) {
			ops := entry.open(b)
			defer ops.close()
			ops.populate(1000)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				v := ops.pointGet(500)
				if v == "" {
					b.Fatal("empty result")
				}
			}
		})
	}
}

type pointQueryOps struct {
	populate func(n int)
	pointGet func(id int) string
	close    func()
}

func openTinyPointQuery(mode storage.StorageMode) func(b *testing.B) pointQueryOps {
	return func(b *testing.B) pointQueryOps {
		b.Helper()
		dir := tmpDir(b)
		cfg := storage.DefaultStorageConfig(mode)
		cfg.Path = filepath.Join(dir, "bench.db")
		db, err := tinysql.OpenDB(cfg)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()

		return pointQueryOps{
			populate: func(n int) {
				stmt, _ := tinysql.ParseSQL("CREATE TABLE IF NOT EXISTS t (id INT, name STRING, score FLOAT64)")
				tinysql.Execute(ctx, db, "default", stmt)
				for i := 0; i < n; i++ {
					ins, _ := tinysql.ParseSQL(fmt.Sprintf(
						"INSERT INTO t VALUES (%d, 'user_%d', %f)", i, i, float64(i)*1.1))
					tinysql.Execute(ctx, db, "default", ins)
				}
			},
			pointGet: func(id int) string {
				stmt, _ := tinysql.ParseSQL(fmt.Sprintf(
					"SELECT name FROM t WHERE id = %d", id))
				rs, err := tinysql.Execute(ctx, db, "default", stmt)
				if err != nil || rs == nil || len(rs.Rows) == 0 {
					return ""
				}
				return fmt.Sprint(rs.Rows[0]["name"])
			},
			close: func() { db.Close() },
		}
	}
}

func openPagePointQuery(b *testing.B) pointQueryOps {
	b.Helper()
	dir := tmpDir(b)
	pb, err := pager.NewPageBackend(pager.PageBackendConfig{
		Path: filepath.Join(dir, "bench.db"),
	})
	if err != nil {
		b.Fatal(err)
	}
	var cached *pager.TableData
	return pointQueryOps{
		populate: func(n int) {
			cols := []pager.ColumnInfo{
				{Name: "id", Type: 0},
				{Name: "name", Type: 13},
				{Name: "score", Type: 11},
			}
			rows := make([][]any, n)
			for i := range rows {
				rows[i] = []any{float64(i), fmt.Sprintf("user_%d", i), float64(i) * 1.1}
			}
			td := &pager.TableData{Name: "t", Columns: cols, Rows: rows, Version: 1}
			pb.SaveTable("default", td)
		},
		pointGet: func(id int) string {
			if cached == nil {
				cached, _ = pb.LoadTable("default", "t")
			}
			if cached == nil || id >= len(cached.Rows) {
				return ""
			}
			return fmt.Sprint(cached.Rows[id][1])
		},
		close: func() { pb.Close() },
	}
}

func openSQLitePointQuery(b *testing.B) pointQueryOps {
	b.Helper()
	dir := tmpDir(b)
	dbPath := filepath.Join(dir, "bench.sqlite3")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		b.Fatal(err)
	}
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")
	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")

	return pointQueryOps{
		populate: func(n int) {
			tx, _ := db.Begin()
			stmt, _ := tx.Prepare("INSERT INTO t VALUES (?,?,?)")
			for i := 0; i < n; i++ {
				stmt.Exec(i, fmt.Sprintf("user_%d", i), float64(i)*1.1)
			}
			stmt.Close()
			tx.Commit()
		},
		pointGet: func(id int) string {
			var name string
			db.QueryRow("SELECT name FROM t WHERE id = ?", id).Scan(&name)
			return name
		},
		close: func() { db.Close() },
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: MixedWorkload — interleaved read+write
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkMixedWorkload(b *testing.B) {
	for _, be := range tinyBackends() {
		b.Run(be.name, func(b *testing.B) {
			ops := be.open(b)
			defer ops.close()

			// Seed initial data.
			ops.save("mix", 50)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Write cycle.
				ops.save("mix", 10)
				// Read cycle.
				ops.load("mix")
			}
		})
	}
}
