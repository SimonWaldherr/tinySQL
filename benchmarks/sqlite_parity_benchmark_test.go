package benchmarks

// ═══════════════════════════════════════════════════════════════════════════
// tinySQL vs modernc.org/sqlite, measured the way an application would see it.
//
// The other benchmark files in this package drive tinySQL through its direct
// tinysql.Execute API while driving SQLite through database/sql. That is the
// right shape for measuring the storage backends against each other, but it is
// not the comparison someone asking "can I replace SQLite with tinySQL?" needs:
// it hands tinySQL a head start by skipping the driver, the placeholder
// binding, and the row-scanning path that the SQLite side pays for.
//
// Here both engines go through database/sql with bound parameters, on schemas
// and durability settings chosen to match:
//
//	tinySQL/mem          in-memory, no durability      SQLite/mem       :memory:
//	tinySQL/wal          WAL, fsync per statement      SQLite/wal-full  journal_mode=WAL, synchronous=FULL
//	                                                   SQLite/wal-norm  journal_mode=WAL, synchronous=NORMAL
//
// SQLite/wal-full is the honest counterpart to tinySQL's ModeWAL, which syncs
// the log on every committed statement. SQLite/wal-norm is included because it
// is what most applications actually run, and the gap between the two shows how
// much of any difference is fsync rather than engine work.
//
// Every engine gets MaxOpenConns(1). SQLite needs it for :memory: (each
// connection would otherwise get its own empty database) and it removes pool
// scheduling as a variable for both sides.
//
// Run with:
//
//	go test ./benchmarks/ -run='^$' -bench='Parity' -benchtime=200x
//
// The vector benchmark is separate (BenchmarkParityVectorTopK): SQLite has no
// vector type here, so its baseline is what an application without a vector
// extension has to do — read the vectors out and rank them in Go.
// ═══════════════════════════════════════════════════════════════════════════

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	_ "github.com/SimonWaldherr/tinySQL/driver"
	_ "modernc.org/sqlite"
)

// parityEngine is one database under test, plus the dialect details that differ
// between the two.
type parityEngine struct {
	name string
	open func(b *testing.B) *sql.DB
	// createRows is the DDL for the row-workload table. The column types differ
	// by dialect; the column names and semantics do not.
	createRows string
	// indexRows creates the secondary index the range-scan benchmark needs, or
	// is empty when the engine cannot use one.
	indexRows string
	// createBuckets is the DDL for the join benchmark's dimension table.
	createBuckets string
}

func parityEngines() []parityEngine {
	const tinyRows = `CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, score FLOAT, bucket INT)`
	const liteRows = `CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score REAL, bucket INTEGER)`
	const tinyBuckets = `CREATE TABLE buckets (bucket INT PRIMARY KEY, label TEXT)`
	const liteBuckets = `CREATE TABLE buckets (bucket INTEGER PRIMARY KEY, label TEXT)`
	const bucketIndex = `CREATE INDEX bench_bucket ON bench (bucket)`
	return []parityEngine{
		{
			name:          "tinySQL/mem",
			open:          func(b *testing.B) *sql.DB { return openParity(b, "tinysql", "mem://?tenant=default") },
			createRows:    tinyRows,
			indexRows:     bucketIndex,
			createBuckets: tinyBuckets,
		},
		{
			name:          "SQLite/mem",
			open:          func(b *testing.B) *sql.DB { return openParity(b, "sqlite", ":memory:") },
			createRows:    liteRows,
			indexRows:     bucketIndex,
			createBuckets: liteBuckets,
		},
		{
			name: "tinySQL/wal",
			open: func(b *testing.B) *sql.DB {
				path := filepath.Join(tmpDir(b), "parity")
				return openParity(b, "tinysql", "file:"+path+"?tenant=default&mode=wal")
			},
			createRows:    tinyRows,
			indexRows:     bucketIndex,
			createBuckets: tinyBuckets,
		},
		{
			name: "SQLite/wal-full",
			open: func(b *testing.B) *sql.DB {
				path := filepath.Join(tmpDir(b), "parity.sqlite")
				return openParity(b, "sqlite",
					"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)")
			},
			createRows:    liteRows,
			indexRows:     bucketIndex,
			createBuckets: liteBuckets,
		},
		{
			name: "SQLite/wal-norm",
			open: func(b *testing.B) *sql.DB {
				path := filepath.Join(tmpDir(b), "parity.sqlite")
				return openParity(b, "sqlite",
					"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
			},
			createRows:    liteRows,
			indexRows:     bucketIndex,
			createBuckets: liteBuckets,
		},
	}
}

func openParity(b *testing.B, driverName, dsn string) *sql.DB {
	b.Helper()
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		b.Fatalf("open %s %q: %v", driverName, dsn, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		b.Fatalf("ping %s %q: %v", driverName, dsn, err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(b *testing.B, db *sql.DB, query string, args ...any) {
	b.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		b.Fatalf("exec %q: %v", query, err)
	}
}

// seedRows loads n rows in one transaction. Bulk loading through autocommit
// would make setup dominate the benchmark's wall time on the fsync-per-statement
// configurations.
func seedRows(b *testing.B, db *sql.DB, e parityEngine, n int) {
	b.Helper()
	mustExec(b, db, e.createRows)
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("begin seed: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := tx.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, ?, ?, ?)`,
			i, fmt.Sprintf("row_%d", i), float64(i)*1.5, i%64); err != nil {
			_ = tx.Rollback()
			b.Fatalf("seed insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("commit seed: %v", err)
	}
	if e.indexRows != "" {
		mustExec(b, db, e.indexRows)
	}
}

const parityRows = 10_000

// ── Writes ────────────────────────────────────────────────────────────────

// BenchmarkParityInsertAutocommit measures one durable INSERT per operation —
// the shape that shows fsync cost most directly.
func BenchmarkParityInsertAutocommit(b *testing.B) {
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			mustExec(b, db, e.createRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, ?, ?, ?)`,
					i, "row", float64(i), i%64); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}

// BenchmarkParityInsertTxBatch reports the cost of one transaction containing
// parityBatch inserts, so ns/op is per batch, not per row.
func BenchmarkParityInsertTxBatch(b *testing.B) {
	const parityBatch = 100
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			mustExec(b, db, e.createRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin()
				if err != nil {
					b.Fatalf("begin: %v", err)
				}
				for j := 0; j < parityBatch; j++ {
					id := i*parityBatch + j
					if _, err := tx.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, ?, ?, ?)`,
						id, "row", float64(id), id%64); err != nil {
						_ = tx.Rollback()
						b.Fatalf("insert: %v", err)
					}
				}
				if err := tx.Commit(); err != nil {
					b.Fatalf("commit: %v", err)
				}
			}
		})
	}
}

func BenchmarkParityUpdateByPK(b *testing.B) {
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			seedRows(b, db, e, parityRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Exec(`UPDATE bench SET score = ? WHERE id = ?`,
					float64(i), i%parityRows); err != nil {
					b.Fatalf("update: %v", err)
				}
			}
		})
	}
}

// ── Reads ─────────────────────────────────────────────────────────────────

func BenchmarkParityPointLookupPK(b *testing.B) {
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			seedRows(b, db, e, parityRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var name string
				var score float64
				if err := db.QueryRow(`SELECT name, score FROM bench WHERE id = ?`,
					i%parityRows).Scan(&name, &score); err != nil {
					b.Fatalf("point lookup: %v", err)
				}
			}
		})
	}
}

func BenchmarkParityRangeScan(b *testing.B) {
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			seedRows(b, db, e, parityRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(`SELECT id, name FROM bench WHERE bucket = ?`, i%64)
				if err != nil {
					b.Fatalf("range scan: %v", err)
				}
				n := 0
				for rows.Next() {
					var id int64
					var name string
					if err := rows.Scan(&id, &name); err != nil {
						_ = rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n++
				}
				if err := rows.Err(); err != nil {
					b.Fatalf("rows: %v", err)
				}
				_ = rows.Close()
				if n == 0 {
					b.Fatal("range scan returned no rows")
				}
			}
		})
	}
}

func BenchmarkParityAggregate(b *testing.B) {
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			seedRows(b, db, e, parityRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(`SELECT bucket, COUNT(*) AS n, SUM(score) AS total FROM bench GROUP BY bucket`)
				if err != nil {
					b.Fatalf("aggregate: %v", err)
				}
				groups := 0
				for rows.Next() {
					var bucket, n int64
					var total float64
					if err := rows.Scan(&bucket, &n, &total); err != nil {
						_ = rows.Close()
						b.Fatalf("scan: %v", err)
					}
					groups++
				}
				_ = rows.Close()
				if groups != 64 {
					b.Fatalf("aggregate returned %d groups, want 64", groups)
				}
			}
		})
	}
}

func BenchmarkParityJoin(b *testing.B) {
	for _, e := range parityEngines() {
		b.Run(e.name, func(b *testing.B) {
			db := e.open(b)
			seedRows(b, db, e, parityRows)
			// A small dimension table to join against, one row per bucket.
			mustExec(b, db, e.createBuckets)
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < 64; i++ {
				if _, err := tx.Exec(`INSERT INTO buckets (bucket, label) VALUES (?, ?)`, i, fmt.Sprintf("b%d", i)); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(`SELECT k.label AS label, COUNT(*) AS n
					FROM bench AS v JOIN buckets AS k ON v.bucket = k.bucket
					GROUP BY k.label`)
				if err != nil {
					b.Fatalf("join: %v", err)
				}
				groups := 0
				for rows.Next() {
					var label string
					var n int64
					if err := rows.Scan(&label, &n); err != nil {
						_ = rows.Close()
						b.Fatalf("scan: %v", err)
					}
					groups++
				}
				_ = rows.Close()
				if groups != 64 {
					b.Fatalf("join returned %d groups, want 64", groups)
				}
			}
		})
	}
}

// ── Vector search ─────────────────────────────────────────────────────────

const (
	parityVecRows = 10_000
	parityVecDim  = 128
	parityVecTopK = 10
)

func parityVectors(n, dim int) [][]float64 {
	rng := rand.New(rand.NewSource(42))
	out := make([][]float64, n)
	for i := range out {
		v := make([]float64, dim)
		var norm float64
		for j := range v {
			v[j] = rng.NormFloat64()
			norm += v[j] * v[j]
		}
		norm = math.Sqrt(norm)
		for j := range v {
			v[j] /= norm
		}
		out[i] = v
	}
	return out
}

func vectorJSON(b *testing.B, v []float64) string {
	b.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		b.Fatal(err)
	}
	return string(raw)
}

// vectorBlob encodes a vector the way an application storing embeddings in
// SQLite without a vector extension would: packed little-endian float32.
func vectorBlob(v []float64) []byte {
	out := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(out[4*i:], math.Float32bits(float32(f)))
	}
	return out
}

func vectorFromBlob(raw []byte, dst []float32) []float32 {
	dst = dst[:0]
	for i := 0; i+4 <= len(raw); i += 4 {
		dst = append(dst, math.Float32frombits(binary.LittleEndian.Uint32(raw[i:])))
	}
	return dst
}

// BenchmarkParityVectorTopK compares tinySQL's in-engine k-NN against the only
// thing plain SQLite can do: return every stored vector and rank them in the
// application. Both sides are in-memory, so this measures search work rather
// than storage.
//
// The two are not the same operation and the numbers should not be read as
// "engine A's k-NN is faster than engine B's" — SQLite has no k-NN here. What
// the comparison shows is the cost an application pays for that missing
// capability.
func BenchmarkParityVectorTopK(b *testing.B) {
	vectors := parityVectors(parityVecRows, parityVecDim)
	query := parityVectors(1, parityVecDim)[0]

	b.Run("tinySQL/mem-VEC_SEARCH-flat", func(b *testing.B) {
		db := openParity(b, "tinysql", "mem://?tenant=default")
		mustExec(b, db, `CREATE TABLE docs (id INT, embedding VECTOR)`)
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		for i, v := range vectors {
			if _, err := tx.Exec(`INSERT INTO docs (id, embedding) VALUES (?, VEC_FROM_JSON(?))`,
				i, vectorJSON(b, v)); err != nil {
				_ = tx.Rollback()
				b.Fatalf("insert vector %d: %v", i, err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		queryJSON := vectorJSON(b, query)
		// Build the column cache outside the timed loop so the first iteration
		// does not carry a one-time cost the rest do not.
		if _, err := db.Exec(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'flat')`); err != nil {
			b.Logf("VEC_WARM unavailable, first iteration includes cache build: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				`SELECT id, _vec_similarity FROM VEC_SEARCH('docs', 'embedding', VEC_FROM_JSON(?), ?, 'cosine')`,
				queryJSON, parityVecTopK)
			if err != nil {
				b.Fatalf("vec_search: %v", err)
			}
			n := 0
			for rows.Next() {
				var id int64
				var sim float64
				if err := rows.Scan(&id, &sim); err != nil {
					_ = rows.Close()
					b.Fatalf("scan: %v", err)
				}
				n++
			}
			_ = rows.Close()
			if n != parityVecTopK {
				b.Fatalf("vec_search returned %d rows, want %d", n, parityVecTopK)
			}
		}
	})

	b.Run("SQLite/mem-app-scan", func(b *testing.B) {
		db := openParity(b, "sqlite", ":memory:")
		mustExec(b, db, `CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding BLOB)`)
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		for i, v := range vectors {
			if _, err := tx.Exec(`INSERT INTO docs (id, embedding) VALUES (?, ?)`, i, vectorBlob(v)); err != nil {
				_ = tx.Rollback()
				b.Fatalf("insert vector %d: %v", i, err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		q32 := make([]float32, parityVecDim)
		for i, f := range query {
			q32[i] = float32(f)
		}
		type scored struct {
			id  int64
			sim float32
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`SELECT id, embedding FROM docs`)
			if err != nil {
				b.Fatalf("scan query: %v", err)
			}
			best := make([]scored, 0, parityVecTopK+1)
			buf := make([]float32, 0, parityVecDim)
			for rows.Next() {
				var id int64
				var raw []byte
				if err := rows.Scan(&id, &raw); err != nil {
					_ = rows.Close()
					b.Fatalf("scan: %v", err)
				}
				buf = vectorFromBlob(raw, buf)
				var dot float32
				for j, f := range buf {
					dot += f * q32[j]
				}
				// Insertion sort into a bounded top-k, the same shape a real
				// application would use.
				if len(best) < parityVecTopK || dot > best[len(best)-1].sim {
					best = append(best, scored{id: id, sim: dot})
					sort.Slice(best, func(a, b int) bool { return best[a].sim > best[b].sim })
					if len(best) > parityVecTopK {
						best = best[:parityVecTopK]
					}
				}
			}
			_ = rows.Close()
			if len(best) != parityVecTopK {
				b.Fatalf("app scan produced %d results, want %d", len(best), parityVecTopK)
			}
		}
	})
}
