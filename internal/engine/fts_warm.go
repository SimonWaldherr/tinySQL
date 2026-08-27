// FTS_WARM — eager warm-up for full-text retrieval structures.
//
// Usage:
//
//	SELECT * FROM FTS_WARM('table_name' [, 'column_name'...])
//
// FTS_SEARCH builds a tokenized document directory, term dictionary and
// inverted postings lazily on its first query for a table/column set. That is
// ideal for one-off searches, but a serving process should not make its first
// user wait for an O(corpus) build. FTS_WARM moves that work to an explicit,
// observable lifecycle step. It uses the exact same column-set semantics and
// cache key as FTS_SEARCH, so the first real query can reuse the warmed entry
// without changing retrieval behavior.
package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// FTSWarmTableFunc implements FTS_WARM(table [, columns...]).
type FTSWarmTableFunc struct{}

func (f *FTSWarmTableFunc) Name() string { return "FTS_WARM" }

func (f *FTSWarmTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 1 {
		return fmt.Errorf("FTS_WARM requires at least 1 argument: (table [, columns...])")
	}
	return nil
}

func (f *FTSWarmTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("FTS_WARM: %w", err)
		}
	}

	tableVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("FTS_WARM table: %w", err)
	}
	tableName, ok := tableVal.(string)
	if !ok {
		return nil, fmt.Errorf("FTS_WARM: table name must be a string")
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("FTS_WARM: table %q not found: %w", tableName, err)
	}

	// Mirror FTS_SEARCH exactly: invalid/non-string optional column arguments
	// are ignored, and no usable explicit column means all columns. Matching
	// this behavior is important because the ordered column positions form the
	// FTS cache key.
	searchCols := ftsWarmSearchCols(env, args[1:], row, table)
	cache := getFTSDocCache(tenant, table, searchCols)

	columnNames := make([]string, 0, len(searchCols))
	for _, colIdx := range searchCols {
		if colIdx >= 0 && colIdx < len(table.Cols) {
			columnNames = append(columnNames, table.Cols[colIdx].Name)
		}
	}
	postingCount := 0
	for _, rows := range cache.postings {
		postingCount += len(rows)
	}

	return &ResultSet{
		Cols: []string{
			"table_name", "columns", "row_count", "valid_docs", "term_count",
			"posting_count", "token_count", "avg_doc_len",
		},
		Rows: []Row{{
			"table_name":    tableName,
			"columns":       strings.Join(columnNames, ","),
			"row_count":     len(table.Rows),
			"valid_docs":    cache.numDocs,
			"term_count":    len(cache.termIDs),
			"posting_count": postingCount,
			"token_count":   len(cache.docTokenIDs),
			"avg_doc_len":   cache.avgDocLen,
		}},
	}, nil
}

// ftsWarmSearchCols is intentionally kept beside FTS_WARM rather than
// changing FTS_SEARCH's mature public path. Its logic is byte-for-byte
// equivalent in effect to FTSSearchTableFunc.Execute's optional-column
// handling, and the returned ordered indices therefore address the same
// persistent and in-memory cache entry.
func ftsWarmSearchCols(env ExecEnv, args []Expr, row Row, table *storage.Table) []int {
	searchCols := make([]int, 0, len(args))
	for _, colArg := range args {
		cv, err := evalExpr(env, colArg, row)
		if err != nil {
			continue
		}
		cn, ok := cv.(string)
		if !ok {
			continue
		}
		idx, err := table.ColIndex(cn)
		if err == nil {
			searchCols = append(searchCols, idx)
		}
	}
	if len(searchCols) != 0 {
		return searchCols
	}
	searchCols = make([]int, len(table.Cols))
	for i := range table.Cols {
		searchCols[i] = i
	}
	return searchCols
}

func init() {
	RegisterTableFunc(&FTSWarmTableFunc{})
}
