package engine

import (
	"maps"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGContextAppendPreservesPriorIndex(t *testing.T) {
	const doc int64 = 1 << 53
	table := storage.NewTable("append_context", []storage.Column{
		{Name: "doc", Type: storage.IntType}, {Name: "chunk", Type: storage.IntType},
	}, false)
	table.Rows = [][]any{{doc, 2}, {doc, 1}, {doc + 1, 0}, {int64(7), 2}, {int64(7), 1}, {nil, 0}, {doc}}
	source := ragSourceFromTable("default", table)
	index := getRAGContextIndex(source, "doc", "chunk")
	t.Cleanup(func() { purgeRAGContextCachesFor("default", table.Name) })
	for _, batch := range [][][]any{
		{{doc, 1}, {doc, 0}, {doc + 1, -1}, {int64(8), 4}, {doc, "invalid"}, {}, {nil, 1}},
		{{doc, 3}, {int64(8), 2}, {doc + 1, 0}},
	} {
		before := ragContextIndex{byDocument: maps.Clone(index.byDocument)}
		for key, rows := range before.byDocument {
			before.byDocument[key] = append([]ragContextRow(nil), rows...)
		}
		old := index
		start := len(table.Rows)
		table.Rows = append(table.Rows, batch...)
		table.MarkDirtyFrom(start)
		table.Version++
		source = ragSourceFromTable("default", table)
		index = getRAGContextIndex(source, "DOC", "CHUNK")
		if want := ragBuildContextIndex(source, "doc", "chunk"); !reflect.DeepEqual(index, want) {
			t.Fatalf("incremental index differs from full rebuild: got %v, want %v", index, want)
		}
		if !reflect.DeepEqual(old, before) {
			t.Fatal("append mutated the previously published index")
		}
		key := ragContextDocumentKey(int64(7))
		if &index.byDocument[key][0] != &old.byDocument[key][0] {
			t.Fatal("untouched document's chunk slice was rebuilt")
		}
		// Existing readers can search the old snapshot while new readers use
		// the extended index. Neither lookup may change a shared chunk slice.
		var readers sync.WaitGroup
		for range 8 {
			readers.Go(func() {
				for range 20 {
					old.find(doc, 1, 1, 1)
					index.find(doc, 1, 1, 1)
				}
			})
		}
		readers.Wait()
	}
}

func TestRAGContextAppendRebuildsAfterStructuralChanges(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE append_mutations (id INT PRIMARY KEY, doc TEXT, chunk INT)`)
	execSQL(t, db, `INSERT INTO append_mutations VALUES (1,'a',2),(2,'a',0),(3,'b',0)`)
	check := func() {
		t.Helper()
		table, _ := db.Get("default", "append_mutations")
		source := ragSourceFromTable("default", table)
		got := getRAGContextIndex(source, "doc", "chunk")
		if want := ragBuildContextIndex(source, "doc", "chunk"); !reflect.DeepEqual(got, want) {
			t.Fatalf("cached context differs from source: got %v, want %v", got, want)
		}
	}
	t.Cleanup(func() { purgeRAGContextCachesFor("default", "append_mutations") })
	check()
	for _, mutation := range []string{
		`INSERT INTO append_mutations VALUES (4,'a',1)`,
		`UPDATE append_mutations SET doc='b',chunk=9 WHERE id=1`,
		`INSERT INTO append_mutations VALUES (5,'b',1)`,
		`DELETE FROM append_mutations WHERE id=2`,
		`INSERT INTO append_mutations VALUES (6,'a',0)`,
		`ALTER TABLE append_mutations ADD COLUMN body TEXT`,
	} {
		execSQL(t, db, mutation)
		check()
	}
}

func TestRAGContextAppendKeepsAuthorizationBoundary(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE append_acl (id INT PRIMARY KEY, tenant TEXT, doc TEXT, chunk INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO append_acl VALUES (1,'a','doc',0,'[1,0]'),(2,'b','doc',1,'[0,1]')`)
	query := `SELECT id FROM RAG_SEARCH('append_acl','embedding','[1,0]',1,
		'{"pre_filter":{"equals":{"tenant":"a"}},"expand_after":3,"doc_id_column":"doc","chunk_index_column":"chunk"}') ORDER BY id`
	if got := execSQL(t, db, query); len(got.Rows) != 1 || got.Rows[0]["id"] != 1 {
		t.Fatal(got.Rows)
	}
	execSQL(t, db, `INSERT INTO append_acl VALUES (3,'a','doc',2,'[0,1]'),(4,'b','doc',3,'[0,1]')`)
	for range 2 {
		if got := execSQL(t, db, query); len(got.Rows) != 2 || got.Rows[0]["id"] != 1 || got.Rows[1]["id"] != 3 {
			t.Fatalf("appended context crossed authorization boundary or lost a neighbor: %v", got.Rows)
		}
	}
}

func TestRAGContextIndexRetainsIndependentDatabases(t *testing.T) {
	// Make room for both entries so a legitimate capacity eviction cannot
	// masquerade as a database-identity collision.
	ragContextIndexCacheMu.Lock()
	evictOverCap(ragContextIndexCache, ragContextIndexCacheMaxEntries-2)
	ragContextIndexCacheMu.Unlock()
	var indexes []ragContextIndex
	var tables []*storage.Table
	for range 2 {
		table := storage.NewTable("independent_context", []storage.Column{
			{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType},
		}, false)
		table.Rows = [][]any{{"a", 0}}
		tables = append(tables, table)
		indexes = append(indexes, getRAGContextIndex(ragSourceFromTable("default", table), "doc", "chunk"))
	}
	t.Cleanup(func() { purgeRAGContextCachesFor("default", "independent_context") })
	for i, table := range tables {
		source := ragSourceFromTable("default", table)
		cached, ok := cachedRAGContextIndex(source, ragContextCacheKey(source, "doc", "chunk"))
		key := ragContextDocumentKey("a")
		if !ok || &cached.byDocument[key][0] != &indexes[i].byDocument[key][0] {
			t.Fatal("independent databases displaced each other's context index")
		}
	}
}
