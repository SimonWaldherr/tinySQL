package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// HYBRID_SEARCH is the search-box-oriented form of RAG_SEARCH:
//
//	HYBRID_SEARCH(table, vector_column, text_column, search_term,
//	              query_vector, k [, options_json])
//
// search_term and query_vector represent the same user input: callers create
// query_vector with the embedding model that was used for vector_column.
// HYBRID_SEARCH uses the string for BM25/wildcard retrieval and the vector for
// semantic retrieval, then reciprocal-rank-fuses both candidate lists.
type HybridSearchTableFunc struct {
	name string
}

func (f *HybridSearchTableFunc) Name() string {
	if f.name == "" {
		return "HYBRID_SEARCH"
	}
	return f.name
}

func (f *HybridSearchTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 6 || len(args) > 7 {
		return fmt.Errorf("%s requires 6-7 arguments: (table, vector_column, text_column, search_term, query_vector, k [, options_json])", f.Name())
	}
	return nil
}

func (f *HybridSearchTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	tableName, err := hybridStringArg(env, args[0], row, f.Name(), "table")
	if err != nil {
		return nil, err
	}
	vectorColumn, err := hybridStringArg(env, args[1], row, f.Name(), "vector_column")
	if err != nil {
		return nil, err
	}
	textColumn, err := hybridStringArg(env, args[2], row, f.Name(), "text_column")
	if err != nil {
		return nil, err
	}
	searchTerm, err := hybridStringArg(env, args[3], row, f.Name(), "search_term")
	if err != nil {
		return nil, err
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("%s: table %q not found: %w", f.Name(), tableName, err)
	}
	vectorIdx, err := table.ColIndex(vectorColumn)
	if err != nil {
		return nil, fmt.Errorf("%s: vector column %q not found", f.Name(), vectorColumn)
	}
	if table.Cols[vectorIdx].Type != storage.VectorType {
		return nil, fmt.Errorf("%s: column %q is not VECTOR", f.Name(), vectorColumn)
	}
	if _, err := table.ColIndex(textColumn); err != nil {
		return nil, fmt.Errorf("%s: text column %q not found", f.Name(), textColumn)
	}

	var opts ragSearchOptions
	if len(args) == 7 {
		value, err := evalExpr(env, args[6], row)
		if err != nil {
			return nil, fmt.Errorf("%s options: %w", f.Name(), err)
		}
		if value != nil {
			raw, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("%s: options must be a JSON string, got %T", f.Name(), value)
			}
			if strings.TrimSpace(raw) != "" {
				if err := json.Unmarshal([]byte(raw), &opts); err != nil {
					return nil, fmt.Errorf("%s: invalid options JSON: %w", f.Name(), err)
				}
			}
		}
	}

	// Positional arguments deliberately win over duplicate JSON properties:
	// one search term must drive both halves of this API.
	opts.TextColumn = textColumn
	opts.TextQuery = searchTerm
	if len(opts.KeyColumns) == 0 {
		for _, column := range table.Cols {
			if column.Constraint == storage.PrimaryKey {
				opts.KeyColumns = append(opts.KeyColumns, column.Name)
			}
		}
	}
	if len(opts.KeyColumns) == 0 {
		return nil, fmt.Errorf("%s: source table needs a PRIMARY KEY or options.key_columns for hybrid result fusion", f.Name())
	}
	for _, key := range opts.KeyColumns {
		if _, err := table.ColIndex(key); err != nil {
			return nil, fmt.Errorf("%s: key column %q not found", f.Name(), key)
		}
	}

	optionsJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("%s options: %w", f.Name(), err)
	}
	ragArgs := []Expr{
		&Literal{Val: tableName},
		&Literal{Val: vectorColumn},
		args[4],
		args[5],
		&Literal{Val: string(optionsJSON)},
	}
	result, err := (&RAGSearchTableFunc{}).Execute(ctx, ragArgs, env, row)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	// RAG_SEARCH deliberately omits score keys for a row that was absent from
	// one candidate list. HYBRID_SEARCH is a public search-box API, so expose
	// a rectangular SQL result instead: projected score columns resolve to
	// NULL instead of raising "unknown column" on vector-only/FTS-only rows.
	resultColumns := make(map[string]bool, len(result.Cols))
	for _, column := range result.Cols {
		resultColumns[strings.ToLower(column)] = true
	}
	for _, resultRow := range result.Rows {
		for _, column := range []string{
			"_vec_rank", "_vec_distance", "_vec_similarity",
			"_fts_rank", "_fts_score", "_rrf_rank", "_rrf_score",
		} {
			if _, ok := resultRow[column]; !ok && resultColumns[column] {
				resultRow[column] = nil
			}
		}
	}
	return result, nil
}

func hybridStringArg(env ExecEnv, expr Expr, row Row, function, argument string) (string, error) {
	value, err := evalExpr(env, expr, row)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", function, argument, err)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s: %s must be a string, got %T", function, argument, value)
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%s: %s must not be empty", function, argument)
	}
	return text, nil
}

func init() {
	RegisterTableFunc(&HybridSearchTableFunc{name: "HYBRID_SEARCH"})
	RegisterTableFunc(&HybridSearchTableFunc{name: "VEC_HYBRID_SEARCH"})
}
