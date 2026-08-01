package benchmarks

// Bounding-box and radius queries: the access pattern a map or navigation UI
// repeats constantly.
//
//	WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
//
// Every viewport redraw, every "what is near me", every POI layer refresh is this
// query. Without a range-capable index it is a full table scan, which is what
// tinySQL did before: the index existed and only equality could use it.
//
// SQLite is compared with the same B-tree index, which is the honest analogue of
// what tinySQL now has. SQLite's R*Tree module would be a different and stronger
// structure for true 2-D search; it is not what this measures.
//
// "SQLite" here is modernc.org/sqlite, the pure-Go port -- not the C
// implementation. See the note at the top of BENCHMARKS.md.

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	_ "github.com/SimonWaldherr/tinySQL/driver"
	_ "modernc.org/sqlite"
)

const (
	// poiCount is a plausible POI layer for a region: fuel stations, parking,
	// charging points across a country-sized extract.
	poiCount = 50000
	// poiSpanDeg is the coordinate span the POIs cover.
	poiSpanDeg = 10.0
	// poiViewportDeg is the size of the queried box, roughly a city view at the
	// zoom where POIs are usually shown.
	poiViewportDeg = 0.05
)

type spatialEngine struct {
	name      string
	open      func(b *testing.B) *sql.DB
	createDDL string
	createIdx string
}

func spatialEngines() []spatialEngine {
	return []spatialEngine{
		{
			name:      "tinySQL/mem",
			open:      func(b *testing.B) *sql.DB { return openParity(b, "tinysql", "mem://?tenant=default") },
			createDDL: `CREATE TABLE poi (id INT, lat FLOAT, lon FLOAT, cat INT, name TEXT)`,
			createIdx: `CREATE INDEX poi_latlon ON poi (lat, lon)`,
		},
		{
			name:      "SQLite/mem",
			open:      func(b *testing.B) *sql.DB { return openParity(b, "sqlite", ":memory:") },
			createDDL: `CREATE TABLE poi (id INTEGER, lat REAL, lon REAL, cat INTEGER, name TEXT)`,
			createIdx: `CREATE INDEX poi_latlon ON poi (lat, lon)`,
		},
	}
}

// loadPOIs fills the table with a deterministic pseudo-random POI distribution.
func loadPOIs(b *testing.B, db *sql.DB, eng spatialEngine, withIndex bool) {
	b.Helper()
	if _, err := db.Exec(eng.createDDL); err != nil {
		b.Fatalf("%s: create poi: %v", eng.name, err)
	}
	rng := rand.New(rand.NewSource(0x504f49))
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("%s: begin: %v", eng.name, err)
	}
	stmt, err := tx.Prepare(`INSERT INTO poi (id, lat, lon, cat, name) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		b.Fatalf("%s: prepare: %v", eng.name, err)
	}
	for i := 0; i < poiCount; i++ {
		lat := 45.0 + rng.Float64()*poiSpanDeg
		lon := 5.0 + rng.Float64()*poiSpanDeg
		if _, err := stmt.Exec(i, lat, lon, i%8, fmt.Sprintf("poi-%d", i)); err != nil {
			b.Fatalf("%s: insert: %v", eng.name, err)
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("%s: commit: %v", eng.name, err)
	}
	if withIndex && eng.createIdx != "" {
		if _, err := db.Exec(eng.createIdx); err != nil {
			b.Fatalf("%s: create index: %v", eng.name, err)
		}
	}
}

// benchmarkViewport measures one bounding-box query per iteration, at a random
// position, the way a panning map issues them.
func benchmarkViewport(b *testing.B, eng spatialEngine, withIndex bool) {
	db := eng.open(b)
	defer db.Close()
	loadPOIs(b, db, eng, withIndex)

	stmt, err := db.Prepare(
		`SELECT id FROM poi WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?`)
	if err != nil {
		b.Fatalf("%s: prepare select: %v", eng.name, err)
	}
	defer stmt.Close()

	rng := rand.New(rand.NewSource(0x56494557))
	b.ReportAllocs()
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		lat := 45.0 + rng.Float64()*(poiSpanDeg-poiViewportDeg)
		lon := 5.0 + rng.Float64()*(poiSpanDeg-poiViewportDeg)
		rows, err := stmt.Query(lat, lat+poiViewportDeg, lon, lon+poiViewportDeg)
		if err != nil {
			b.Fatalf("%s: query: %v", eng.name, err)
		}
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				b.Fatalf("%s: scan: %v", eng.name, err)
			}
			total++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			b.Fatal(err)
		}
		rows.Close()
	}
	b.StopTimer()
	b.ReportMetric(float64(total)/float64(b.N), "hits/op")
}

// BenchmarkViewportIndexed is the shipped configuration: an index on (lat, lon).
func BenchmarkViewportIndexed(b *testing.B) {
	for _, eng := range spatialEngines() {
		b.Run(eng.name, func(b *testing.B) { benchmarkViewport(b, eng, true) })
	}
}

// BenchmarkViewportNoIndex is the baseline the range seek replaces, and what
// tinySQL did for every viewport query before it existed.
func BenchmarkViewportNoIndex(b *testing.B) {
	for _, eng := range spatialEngines() {
		b.Run(eng.name, func(b *testing.B) { benchmarkViewport(b, eng, false) })
	}
}

// BenchmarkCategoryInViewport adds an equality predicate, exercising the
// composite shape: an equality prefix followed by the bounded column.
func BenchmarkCategoryInViewport(b *testing.B) {
	for _, eng := range spatialEngines() {
		b.Run(eng.name, func(b *testing.B) {
			db := eng.open(b)
			defer db.Close()
			loadPOIs(b, db, eng, false)
			idx := `CREATE INDEX poi_cat_lat ON poi (cat, lat)`
			if _, err := db.Exec(idx); err != nil {
				b.Fatalf("%s: %s: %v", eng.name, idx, err)
			}
			stmt, err := db.Prepare(
				`SELECT id FROM poi WHERE cat = ? AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?`)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()

			rng := rand.New(rand.NewSource(0x43415400))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lat := 45.0 + rng.Float64()*(poiSpanDeg-poiViewportDeg)
				lon := 5.0 + rng.Float64()*(poiSpanDeg-poiViewportDeg)
				rows, err := stmt.Query(i%8, lat, lat+poiViewportDeg, lon, lon+poiViewportDeg)
				if err != nil {
					b.Fatalf("%s: query: %v", eng.name, err)
				}
				for rows.Next() {
					var id int
					if err := rows.Scan(&id); err != nil {
						rows.Close()
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}
