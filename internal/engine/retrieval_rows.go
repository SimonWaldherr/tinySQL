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
	keys := make([]derivedKeys, len(result.Cols))
	lowerAlias := strings.ToLower(alias)
	for i, col := range result.Cols {
		// Preserve legacy casing and dotted-key collision behavior through
		// the copying path, before modifying any result row.
		if strings.ToLower(col) != col || strings.Contains(col, ".") {
			return aliasRows(result.Rows, alias)
		}
		keys[i].plain = col
		if alias != "" {
			keys[i].qualified = lowerAlias + "." + col
		}
	}
	if result.Rows == nil {
		return []Row{}
	}
	if alias != "" {
		for _, row := range result.Rows {
			for _, key := range keys {
				if value, found := row[key.plain]; found {
					row[key.qualified] = value
				}
			}
		}
	}
	return result.Rows
}
