package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func fillSearchCorpus(t *testing.T, db *storage.DB, rows int) {
	t.Helper()
	execSQL(t, db, `CREATE TABLE docs (id INT, body TEXT, emb VECTOR)`)
	for i := 0; i < rows; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO docs (id, body, emb) VALUES (%d, 'document number %d about topic%d with filler words', '[%d.0, %d.5, %d.25, %d.75]')`,
			i, i, i%7, i, i%11, i%13, i%5))
	}
}

// searchAnswers runs one FTS and one vector query and returns their rows.
func searchAnswers(t *testing.T, db *storage.DB) [][]Row {
	t.Helper()
	fts := execSQL(t, db, `SELECT id, _fts_rank FROM FTS_SEARCH('docs', 'topic3 filler', 5, 'body')`)
	vec := execSQL(t, db, `SELECT id, _vec_rank FROM VEC_SEARCH('docs', 'emb', '[7.0, 7.5, 7.25, 2.75]', 5, 'cosine', 'hnsw')`)
	if len(fts.Rows) == 0 || len(vec.Rows) == 0 {
		t.Fatalf("empty search answers: fts=%d vec=%d", len(fts.Rows), len(vec.Rows))
	}
	return [][]Row{fts.Rows, vec.Rows}
}

func warmSearchIndexes(t *testing.T, db *storage.DB) {
	t.Helper()
	execSQL(t, db, `SELECT * FROM FTS_WARM('docs', 'body')`)
	execSQL(t, db, `SELECT * FROM VEC_WARM('docs', 'emb', 'cosine', 'hnsw')`)
}

func TestSkipSearchIndexPersistenceEndToEnd(t *testing.T) {
	const rows = 60
	cold := storage.NewDB()
	fillSearchCorpus(t, cold, rows)
	want := searchAnswers(t, cold)

	var sizes [2]int64
	for i, skip := range []bool{true, false} {
		dir := t.TempDir()
		cfg := storage.StorageConfig{Mode: storage.ModeDisk, Path: dir, SkipSearchIndexPersistence: skip}
		db, err := storage.OpenDB(cfg)
		if err != nil {
			t.Fatal(err)
		}
		fillSearchCorpus(t, db, rows)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		afterRows := db.BackendStats().TableWrites

		// Building the search structures changes no row: with the flag the
		// table must stay clean, without it the images are written.
		warmSearchIndexes(t, db)
		if got := searchAnswers(t, db); !reflect.DeepEqual(got, want) {
			t.Fatalf("skip=%v: warm answers %v, cold answers %v", skip, got, want)
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		wrote := db.BackendStats().TableWrites - afterRows
		if skip && wrote != 0 {
			t.Fatalf("skip: FTS_WARM/VEC_WARM made Sync write %d table files", wrote)
		}
		if !skip && wrote != 1 {
			t.Fatalf("default: FTS_WARM/VEC_WARM made Sync write %d table files, want 1", wrote)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(filepath.Join(dir, "default", "docs.tbl"))
		if err != nil {
			t.Fatal(err)
		}
		sizes[i] = info.Size()

		reopened, err := storage.OpenDB(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := searchAnswers(t, reopened); !reflect.DeepEqual(got, want) {
			t.Fatalf("skip=%v: answers after reopen %v, want %v", skip, got, want)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if sizes[0] >= sizes[1] {
		t.Fatalf("table file with the flag (%d bytes) is not smaller than without (%d bytes)", sizes[0], sizes[1])
	}
}
