package engine

import "strings"

// Built-in retrievers materialize new hit maps for each execution. Qualify
// those maps directly; extension table functions may return shared results
// and must retain the copying conversion.
func aliasRetrievalRows(fn TableFunction, result *ResultSet, alias string) []Row {
	switch fn.(type) {
	case *VecSearchTableFunc, *VecSearchFilteredTableFunc,
		*FTSSearchTableFunc, *FTSSearchFilteredTableFunc,
		*RAGSearchTableFunc, *HybridSearchTableFunc,
		*RAGContextTableFunc, *RAGContextFromTableFunc:
	default:
		return aliasRows(result.Rows, alias)
	}
	if result.Rows == nil {
		return []Row{}
	}
	for _, col := range result.Cols {
		// Preserve legacy casing and dotted-key collision behavior through
		// the copying path, before modifying any result row.
		if strings.ToLower(col) != col || strings.Contains(col, ".") {
			return aliasRows(result.Rows, alias)
		}
	}
	if alias == "" {
		return result.Rows
	}
	var scratch [16]string
	qualified := scratch[:]
	if len(result.Cols) > len(qualified) {
		qualified = make([]string, len(result.Cols))
	} else {
		qualified = qualified[:len(result.Cols)]
	}
	lowerAlias := strings.ToLower(alias)
	for i, col := range result.Cols {
		qualified[i] = lowerAlias + "." + col
	}
	for _, row := range result.Rows {
		for i, col := range result.Cols {
			if value, found := row[col]; found {
				row[qualified[i]] = value
			}
		}
	}
	return result.Rows
}
