package engine

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRetrievalPreFilterRunsBeforeVectorFTSAndRRF(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `
		CREATE TABLE acl_chunks (
			chunk_id TEXT PRIMARY KEY,
			tenant_id TEXT,
			doc_id TEXT,
			chunk_index INT,
			body TEXT,
			embedding VECTOR
		)
	`)
	// This makes the equality pre-filter take its secondary-index path. The
	// blocked row is deliberately closest and inserted first, so an accidental
	// post-filter would consume the only candidate slot and return no allowed
	// result.
	execSQL(t, db, `CREATE INDEX idx_acl_chunks_tenant ON acl_chunks(tenant_id)`)
	execSQL(t, db, `
		INSERT INTO acl_chunks VALUES
			('blocked-best', 'blocked', 'doc-a', 1, 'secret blocked neighbor', '[1.0, 0.0]'),
			('allowed-hit',  'acme',    'doc-a', 0, 'secret allowed answer',  '[0.99, 0.10]'),
			('allowed-next', 'acme',    'doc-a', 2, 'allowed context',        '[0.00, 1.00]')
	`)

	prefilter := `'{"pre_filter":{"equals":{"tenant_id":"acme"}}}'`
	vec := execSQL(t, db, `
		SELECT chunk_id, _vec_rank
		FROM VEC_SEARCH_FILTERED(
			'acl_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			`+prefilter+`
		)
	`)
	if len(vec.Rows) != 1 || vec.Rows[0]["chunk_id"] != "allowed-hit" || vec.Rows[0]["_vec_rank"] != 1 {
		t.Fatalf("filtered vector result = %#v, want allowed-hit at rank 1", vec.Rows)
	}

	fts := execSQL(t, db, `
		SELECT chunk_id, _fts_rank
		FROM FTS_SEARCH_FILTERED('acl_chunks', 'secret', 1, `+prefilter+`, 'body')
	`)
	if len(fts.Rows) != 1 || fts.Rows[0]["chunk_id"] != "allowed-hit" || fts.Rows[0]["_fts_rank"] != 1 {
		t.Fatalf("filtered FTS result = %#v, want allowed-hit at rank 1", fts.Rows)
	}

	// Both RRF branches use the same pre-filter. candidate_k=1 turns this
	// into a sensitive slot-consumption regression: any post-filtering would
	// let blocked-best take both branch candidates before the allowed row is
	// considered.
	hybrid := execSQL(t, db, `
		SELECT chunk_id, _vec_rank, _fts_rank, _rrf_rank
		FROM HYBRID_SEARCH(
			'acl_chunks', 'embedding', 'body', 'secret',
			VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"candidate_k":1,"pre_filter":{"equals":{"tenant_id":"acme"}}}'
		)
	`)
	if len(hybrid.Rows) != 1 || hybrid.Rows[0]["chunk_id"] != "allowed-hit" {
		t.Fatalf("filtered hybrid result = %#v, want allowed-hit", hybrid.Rows)
	}
	for _, column := range []string{"_vec_rank", "_fts_rank", "_rrf_rank"} {
		if hybrid.Rows[0][column] != 1 {
			t.Errorf("filtered hybrid %s = %v, want 1", column, hybrid.Rows[0][column])
		}
	}

	// Context expansion must obey exactly the same boundary. blocked-best is a
	// neighboring chunk in doc-a and would previously be added after a correct
	// retrieval hit unless the expansion source were restricted too.
	contextRows := execSQL(t, db, `
		SELECT chunk_id, _context_offset
		FROM HYBRID_SEARCH(
			'acl_chunks', 'embedding', 'body', 'secret',
			VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{
				"candidate_k":1,
				"pre_filter":{"equals":{"tenant_id":"acme"}},
				"expand_after":2,
				"doc_id_column":"doc_id",
				"chunk_index_column":"chunk_index"
			}'
		)
	`)
	if len(contextRows.Rows) != 2 {
		t.Fatalf("filtered context rows = %#v, want allowed hit + allowed neighbor", contextRows.Rows)
	}
	for _, result := range contextRows.Rows {
		if strings.HasPrefix(result["chunk_id"].(string), "blocked") {
			t.Fatalf("pre-filter leaked blocked context row: %#v", result)
		}
	}

	// The retrieval table is a physical table, so a same-named CTE must not
	// replace it only for context expansion. Such a substitution would give the
	// context phase row IDs unrelated to the resolved ACL filter.
	execSQL(t, db, `
		CREATE TABLE acl_decoy (
			chunk_id TEXT PRIMARY KEY,
			tenant_id TEXT,
			doc_id TEXT,
			chunk_index INT,
			body TEXT,
			embedding VECTOR
		)
	`)
	execSQL(t, db, `
		INSERT INTO acl_decoy VALUES
			('cte-blocked', 'blocked', 'doc-a', 1, 'CTE-only secret', '[1.0, 0.0]')
	`)
	cteShadow := execSQL(t, db, `
		WITH acl_chunks AS (SELECT * FROM acl_decoy)
		SELECT chunk_id
		FROM HYBRID_SEARCH(
			'acl_chunks', 'embedding', 'body', 'secret',
			VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{
				"candidate_k":1,
				"pre_filter":{"equals":{"tenant_id":"acme"}},
				"expand_after":2,
				"doc_id_column":"doc_id",
				"chunk_index_column":"chunk_index"
			}'
		)
	`)
	if len(cteShadow.Rows) != 2 {
		t.Fatalf("CTE-shadowed filtered context rows = %#v, want physical allowed hit + neighbor", cteShadow.Rows)
	}
	for _, result := range cteShadow.Rows {
		if result["chunk_id"] == "cte-blocked" {
			t.Fatalf("CTE shadow bypassed pre-filter in context expansion: %#v", result)
		}
	}
}

func TestRetrievalPreFilterAllowedStableIDsAndValidation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE id_chunks (chunk_id TEXT PRIMARY KEY, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `
		INSERT INTO id_chunks VALUES
			('closest-but-denied', 'needle', '[1.0, 0.0]'),
			('explicitly-allowed', 'needle', '[0.0, 1.0]')
	`)

	// Stable primary-key values, rather than mutable physical row positions,
	// provide the public ACL handoff between an authorization layer and RAG.
	rs := execSQL(t, db, `
		SELECT chunk_id FROM VEC_SEARCH_FILTERED(
			'id_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{"id_column":"chunk_id","allowed_row_ids":["explicitly-allowed"]}}'
		)
	`)
	if len(rs.Rows) != 1 || rs.Rows[0]["chunk_id"] != "explicitly-allowed" {
		t.Fatalf("allowed_row_ids result = %#v, want explicitly-allowed", rs.Rows)
	}
	denyAll := execSQL(t, db, `
		SELECT chunk_id FROM VEC_SEARCH_FILTERED(
			'id_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{"allowed_row_ids":[]}}'
		)
	`)
	if len(denyAll.Rows) != 0 {
		t.Fatalf("empty allowed_row_ids = %#v, want deny-all", denyAll.Rows)
	}

	_, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT * FROM VEC_SEARCH_FILTERED(
			'id_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1, '{}'
		)
	`))
	if err == nil || !strings.Contains(err.Error(), "requires pre_filter") {
		t.Fatalf("missing pre_filter error = %v, want explicit pre_filter requirement", err)
	}

	_, err = Execute(context.Background(), db, "default", mustParse(`
		SELECT * FROM VEC_SEARCH_FILTERED(
			'id_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{}}'
		)
	`))
	if err == nil || !strings.Contains(err.Error(), "pre_filter requires") {
		t.Fatalf("empty pre_filter error = %v, want a non-empty boundary requirement", err)
	}

	_, err = Execute(context.Background(), db, "default", mustParse(`
		SELECT * FROM RAG_SEARCH(
			'id_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":null}'
		)
	`))
	if err == nil || !strings.Contains(err.Error(), "pre_filter must not be null") {
		t.Fatalf("null pre_filter error = %v, want fail-closed null validation", err)
	}

	// JSON's default float64 decoding would round this identifier. The filter
	// parser intentionally preserves json.Number until it can coerce against
	// the INT column, so it must select the exact 64-bit ID rather than its
	// adjacent representable value.
	execSQL(t, db, `CREATE TABLE numeric_ids (id INT PRIMARY KEY, embedding VECTOR)`)
	execSQL(t, db, `
		INSERT INTO numeric_ids VALUES
			(9007199254740992, '[1.0, 0.0]'),
			(9007199254740993, '[0.0, 1.0]')
	`)
	precise := execSQL(t, db, `
		SELECT id FROM VEC_SEARCH_FILTERED(
			'numeric_ids', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{"allowed_row_ids":[9007199254740993]}}'
		)
	`)
	if len(precise.Rows) != 1 || precise.Rows[0]["id"] != int(9007199254740993) {
		t.Fatalf("large numeric allowed_row_ids result = %#v, want exact 9007199254740993", precise.Rows)
	}

	// JSON numbers retain their original spelling. Integral decimal and exponent
	// spellings must match SQL INTEGER values without a float64 round-trip;
	// non-integral values remain fail-closed instead of being truncated.
	execSQL(t, db, `CREATE TABLE decimal_ids (id INT PRIMARY KEY, tenant_id INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO decimal_ids VALUES (1, 7, '[1.0, 0.0]'), (2, 8, '[0.0, 1.0]')`)
	decimal := execSQL(t, db, `
		SELECT id FROM VEC_SEARCH_FILTERED(
			'decimal_ids', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{"allowed_row_ids":[1.0]}}'
		)
	`)
	if len(decimal.Rows) != 1 || decimal.Rows[0]["id"] != 1 {
		t.Fatalf("decimal integer ID filter = %#v, want id 1", decimal.Rows)
	}
	scientific := execSQL(t, db, `
		SELECT id FROM VEC_SEARCH_FILTERED(
			'decimal_ids', 'embedding', VEC_FROM_JSON('[0.0, 1.0]'), 1,
			'{"pre_filter":{"equals":{"tenant_id":8e0}}}'
		)
	`)
	if len(scientific.Rows) != 1 || scientific.Rows[0]["id"] != 2 {
		t.Fatalf("scientific integer equality filter = %#v, want id 2", scientific.Rows)
	}
	_, err = Execute(context.Background(), db, "default", mustParse(`
		SELECT * FROM VEC_SEARCH_FILTERED(
			'decimal_ids', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{"allowed_row_ids":[1.5]}}'
		)
	`))
	if err == nil || !strings.Contains(err.Error(), "without loss") {
		t.Fatalf("fractional integer filter error = %v, want lossless rejection", err)
	}
}

func TestRetrievalPreFilterRespectsTenantNamespace(t *testing.T) {
	db := storage.NewDB()
	execRAGPrefilterTenant(t, db, "alpha", `CREATE TABLE chunks (id TEXT PRIMARY KEY, body TEXT, embedding VECTOR)`)
	execRAGPrefilterTenant(t, db, "beta", `CREATE TABLE chunks (id TEXT PRIMARY KEY, body TEXT, embedding VECTOR)`)
	execRAGPrefilterTenant(t, db, "alpha", `INSERT INTO chunks VALUES ('alpha-only', 'needle', '[1.0, 0.0]')`)
	execRAGPrefilterTenant(t, db, "beta", `INSERT INTO chunks VALUES ('beta-only', 'needle', '[1.0, 0.0]')`)

	rs := execRAGPrefilterTenant(t, db, "alpha", `
		SELECT id FROM FTS_SEARCH_FILTERED(
			'chunks', 'needle', 2,
			'{"pre_filter":{"allowed_row_ids":["alpha-only"]}}', 'body'
		)
	`)
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != "alpha-only" {
		t.Fatalf("alpha filtered retrieval = %#v, want only alpha-only", rs.Rows)
	}
}

func TestRetrievalPreFilterUsesAuthorizedBM25Statistics(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE fts_acl_stats (id TEXT PRIMARY KEY, tenant_id TEXT, body TEXT)`)
	execSQL(t, db, `INSERT INTO fts_acl_stats VALUES ('allowed', 'acme', 'needle')`)
	// The word occurs in many forbidden rows. Corpus-wide BM25 would give the
	// allowed document a near-zero IDF and make its visible score reveal that
	// forbidden corpus statistic. A strict pre-filter instead computes N/DF and
	// average document length inside the allowed subset.
	for i := 0; i < 32; i++ {
		execSQL(t, db, `INSERT INTO fts_acl_stats VALUES ('blocked-`+strconv.Itoa(i)+`', 'blocked', 'needle')`)
	}

	rs := execSQL(t, db, `
		SELECT id, _fts_score
		FROM FTS_SEARCH_FILTERED(
			'fts_acl_stats', 'needle', 1,
			'{"pre_filter":{"equals":{"tenant_id":"acme"}}}', 'body'
		)
	`)
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != "allowed" {
		t.Fatalf("strict-BM25 filtered FTS result = %#v, want allowed only", rs.Rows)
	}
	score, ok := rs.Rows[0]["_fts_score"].(float64)
	if !ok || score <= 0.1 {
		t.Fatalf("authorized-only BM25 score = %#v, want local IDF above 0.1", rs.Rows[0]["_fts_score"])
	}
}

func TestRetrievalPreFilterCachesPurgeOnDrop(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE prefilter_drop_purge (id TEXT PRIMARY KEY, tenant_id TEXT, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO prefilter_drop_purge VALUES ('allowed', 'acme', 'needle', '[1.0, 0.0]')`)
	execSQL(t, db, `
		SELECT * FROM VEC_SEARCH_FILTERED(
			'prefilter_drop_purge', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"pre_filter":{"equals":{"tenant_id":"acme"}}}'
		)
	`)
	execSQL(t, db, `
		SELECT * FROM FTS_SEARCH_FILTERED(
			'prefilter_drop_purge', 'needle', 1,
			'{"pre_filter":{"equals":{"tenant_id":"acme"}}}', 'body'
		)
	`)
	table, err := db.Get("default", "prefilter_drop_purge")
	if err != nil {
		t.Fatal(err)
	}

	contains := func() (rowFilter, candidates, stats bool) {
		ragRowFilterCacheMu.RLock()
		for key := range ragRowFilterCache {
			rowFilter = rowFilter || key.table == table
		}
		ragRowFilterCacheMu.RUnlock()
		ragFilteredFTSCandidateCacheMu.RLock()
		for key := range ragFilteredFTSCandidateCache {
			candidates = candidates || key.table == table
		}
		ragFilteredFTSCandidateCacheMu.RUnlock()
		ragFilteredFTSStatsCacheMu.RLock()
		for key := range ragFilteredFTSStatsCache {
			stats = stats || key.table == table
		}
		ragFilteredFTSStatsCacheMu.RUnlock()
		return rowFilter, candidates, stats
	}
	if rowFilter, candidates, stats := contains(); !rowFilter || !candidates || !stats {
		t.Fatalf("expected populated pre-filter caches: rows=%t candidates=%t stats=%t", rowFilter, candidates, stats)
	}
	execSQL(t, db, `DROP TABLE prefilter_drop_purge`)
	if rowFilter, candidates, stats := contains(); rowFilter || candidates || stats {
		t.Fatalf("DROP left pre-filter cache entries: rows=%t candidates=%t stats=%t", rowFilter, candidates, stats)
	}
}

func execRAGPrefilterTenant(t *testing.T, db *storage.DB, tenant, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(context.Background(), db, tenant, mustParse(sql))
	if err != nil {
		t.Fatalf("tenant %q SQL failed: %s\n  error: %v", tenant, sql, err)
	}
	return rs
}
