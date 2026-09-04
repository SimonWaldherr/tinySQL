// Common table expressions, including recursive ones. Recursion is bounded by
// a row cap so a CTE without a terminating condition fails instead of running
// until memory is gone.
package engine

import (
	"fmt"
	"strings"
)

// processCTEs extracts and evaluates CTEs (including simple recursive CTEs)
// and returns an ExecEnv with any CTE results bound.
func processCTEs(env ExecEnv, s *Select) (ExecEnv, error) {
	if len(s.CTEs) == 0 {
		return env, nil
	}

	// Preserve all execution state (including statement WAL and trigger row)
	// and copy outer bindings so nested WITH queries retain their scope.
	cteEnv := env
	cteEnv.ctes = make(map[string]*ResultSet, len(env.ctes)+len(s.CTEs))
	if cteEnv.cteRowCache == nil {
		cteEnv.cteRowCache = newCTERowCache()
	}
	for name, rs := range env.ctes {
		cteEnv.ctes[strings.ToLower(name)] = rs
	}

	for _, cte := range s.CTEs {
		if !cte.Recursive {
			rs, err := evalNonRecursiveCTE(cteEnv, &cte)
			if err != nil {
				return env, err
			}
			rs, err = applyCTEColumnAliases(&cte, rs)
			if err != nil {
				return env, err
			}
			// A non-recursive CTE is evaluated once before its consumers run,
			// making its ResultSet immutable for this statement. Its FROM/JOIN
			// row maps can therefore be memoized safely.
			rs.cteCacheable = true
			cteEnv.ctes[strings.ToLower(cte.Name)] = rs
			continue
		}

		rs, err := evalRecursiveCTE(cteEnv, &cte)
		if err != nil {
			return env, err
		}
		cteEnv.ctes[strings.ToLower(cte.Name)] = rs
	}

	return cteEnv, nil
}

// evalNonRecursiveCTE evaluates a simple (non-recursive) CTE and returns its ResultSet.
func evalNonRecursiveCTE(env ExecEnv, cte *CTE) (*ResultSet, error) {
	if cte.Select == nil {
		return nil, fmt.Errorf("CTE %s: missing select", cte.Name)
	}
	rs, err := executeSelect(env, cte.Select)
	if err != nil {
		return nil, fmt.Errorf("CTE %s: %v", cte.Name, err)
	}
	return rs, nil
}

// applyCTEColumnAliases implements WITH c(alias1, ...) AS (...) by remapping
// the result columns by position. The returned rows contain both unqualified
// and CTE-qualified keys so subsequent CTEs, aliases, and recursive steps use
// the same lookup rules as regular tables.
func applyCTEColumnAliases(cte *CTE, rs *ResultSet) (*ResultSet, error) {
	if rs == nil || len(cte.Columns) == 0 {
		return rs, nil
	}
	if len(cte.Columns) != len(rs.Cols) {
		return nil, fmt.Errorf("CTE %s declares %d column aliases for %d result columns", cte.Name, len(cte.Columns), len(rs.Cols))
	}
	cols := append([]string(nil), cte.Columns...)
	// Bind source/output keys once, rather than allocating qualified names
	// and normalizing the same column names for every row.
	sources := make([]string, len(cols))
	targets := make([]string, len(cols))
	qualified := make([]string, len(cols))
	for i := range cols {
		sources[i] = strings.ToLower(rs.Cols[i])
		targets[i] = strings.ToLower(cols[i])
		qualified[i] = strings.ToLower(cte.Name + "." + cols[i])
	}
	rows := make([]Row, len(rs.Rows))
	for rowIdx, row := range rs.Rows {
		out := make(Row, len(cols)*2)
		for i, source := range sources {
			value := row[source]
			out[targets[i]] = value
			out[qualified[i]] = value
		}
		rows[rowIdx] = out
	}
	return &ResultSet{Cols: cols, Rows: rows}, nil
}

// evalRecursiveCTE evaluates a recursive CTE (WITH RECURSIVE) by executing the
// anchor and iteratively applying the recursive part until stabilization or limit.
// alignRecursiveCTERows aligns the rows from recursive part to match anchor columns
func alignRecursiveCTERows(accRs *ResultSet, nextRs *ResultSet, cteName string) []Row {
	// If columns don't match or aren't available, return as-is
	if accRs == nil || accRs.Cols == nil || nextRs == nil || nextRs.Cols == nil || len(nextRs.Cols) != len(accRs.Cols) {
		return nextRs.Rows
	}

	sources := make([]string, len(accRs.Cols))
	targets := make([]string, len(accRs.Cols))
	qualified := make([]string, len(accRs.Cols))
	for i, col := range accRs.Cols {
		sources[i] = strings.ToLower(nextRs.Cols[i])
		targets[i] = strings.ToLower(col)
		qualified[i] = strings.ToLower(cteName + "." + targets[i])
	}
	alignedRows := make([]Row, len(nextRs.Rows))
	for rowIdx, r := range nextRs.Rows {
		nr := make(Row, len(targets)*2)
		for i, source := range sources {
			val, ok := r[source]
			if !ok {
				val = r[nextRs.Cols[i]]
			}
			nr[targets[i]] = val
			nr[qualified[i]] = val
		}
		alignedRows[rowIdx] = nr
	}
	return alignedRows
}

// recursiveCTEMaxRows caps the total accumulated rows a recursive CTE may
// produce, guarding against fan-out recursive terms (e.g. a self-join) that
// grow exponentially and would otherwise exhaust memory well before iterLimit
// is reached. A var (not const) so tests can lower it temporarily instead of
// allocating millions of rows to exercise the cap.
var recursiveCTEMaxRows = 4_000_000

func evalRecursiveCTE(env ExecEnv, cte *CTE) (*ResultSet, error) {
	if cte.Select == nil || cte.Select.Union == nil {
		return nil, fmt.Errorf("recursive CTE %s must be a UNION of anchor and recursive part", cte.Name)
	}
	union := cte.Select.Union
	if union.Type != UnionDistinct && union.Type != UnionAll {
		return nil, fmt.Errorf("recursive CTE %s must use UNION or UNION ALL", cte.Name)
	}
	if union.Next != nil {
		return nil, fmt.Errorf("recursive CTE %s supports one recursive UNION term", cte.Name)
	}

	anchor := *cte.Select
	anchor.Union = nil
	// A recursive CTE's ORDER BY/LIMIT/OFFSET belongs to the complete compound
	// result. Do not let the anchor consume it while the recursive member sees
	// no tail at all.
	anchor.OrderBy = nil
	anchor.Limit = nil
	anchor.Offset = nil

	recursiveSel := union.Right
	if recursiveSel == nil {
		return nil, fmt.Errorf("recursive CTE %s missing recursive part", cte.Name)
	}

	accRs, err := executeSelect(env, &anchor)
	if err != nil {
		return nil, fmt.Errorf("CTE %s anchor: %v", cte.Name, err)
	}
	accRs, err = applyCTEColumnAliases(cte, accRs)
	if err != nil {
		return nil, err
	}
	if accRs == nil {
		return &ResultSet{}, nil
	}

	seen := make(map[string]bool)
	var signatureColumns []string
	var signatureBuf []byte
	if union.Type == UnionDistinct {
		signatureColumns = setOperationColumnKeys(accRs.Cols)
		signatureBuf = make([]byte, 0, 64)
	}
	accRows := make([]Row, 0, len(accRs.Rows))
	frontier := make([]Row, 0, len(accRs.Rows))
	for _, row := range accRs.Rows {
		if union.Type == UnionDistinct {
			signatureBuf = appendSetOperationSignature(signatureBuf[:0], row, signatureColumns)
			if seen[string(signatureBuf)] {
				continue
			}
			seen[string(signatureBuf)] = true
		}
		accRows = append(accRows, row)
		frontier = append(frontier, row)
	}
	maxRows := recursiveCTEWorkLimit(cte.Select)
	if maxRows >= 0 && len(accRows) >= maxRows {
		accRows = accRows[:maxRows]
		frontier = nil
	}

	iterLimit := 1024
	for iter := 0; iter < iterLimit && len(frontier) > 0; iter++ {
		// SQL recursive evaluation feeds each iteration only the rows produced
		// by the previous iteration (the working table), not all accumulated
		// rows. This is essential for UNION ALL semantics and termination.
		env.ctes[strings.ToLower(cte.Name)] = &ResultSet{Cols: accRs.Cols, Rows: frontier}

		nextRs, err := executeSelect(env, recursiveSel)
		if err != nil {
			return nil, fmt.Errorf("CTE %s recursive eval: %v", cte.Name, err)
		}
		if nextRs == nil || len(nextRs.Rows) == 0 {
			frontier = nil
			break
		}
		if len(nextRs.Cols) != len(accRs.Cols) {
			return nil, fmt.Errorf("recursive CTE %s has %d columns in anchor and %d in recursive term", cte.Name, len(accRs.Cols), len(nextRs.Cols))
		}
		alignedRows := alignRecursiveCTERows(accRs, nextRs, cte.Name)
		if union.Type == UnionAll {
			if maxRows >= 0 {
				remaining := maxRows - len(accRows)
				if remaining <= 0 {
					frontier = nil
					break
				}
				if len(alignedRows) > remaining {
					alignedRows = alignedRows[:remaining]
				}
			}
			accRows = append(accRows, alignedRows...)
			frontier = alignedRows
			if maxRows >= 0 && len(accRows) >= maxRows {
				frontier = nil
				break
			}
			if len(accRows) > recursiveCTEMaxRows {
				return nil, fmt.Errorf("recursive CTE %s exceeded row limit %d", cte.Name, recursiveCTEMaxRows)
			}
			continue
		}

		frontier = frontier[:0]
		for _, row := range alignedRows {
			signatureBuf = appendSetOperationSignature(signatureBuf[:0], row, signatureColumns)
			if seen[string(signatureBuf)] {
				continue
			}
			seen[string(signatureBuf)] = true
			accRows = append(accRows, row)
			frontier = append(frontier, row)
			if maxRows >= 0 && len(accRows) >= maxRows {
				frontier = nil
				break
			}
		}
		if len(accRows) > recursiveCTEMaxRows {
			return nil, fmt.Errorf("recursive CTE %s exceeded row limit %d", cte.Name, recursiveCTEMaxRows)
		}
	}
	if len(frontier) > 0 {
		return nil, fmt.Errorf("recursive CTE %s exceeded iteration limit %d", cte.Name, iterLimit)
	}
	accRows = applyRecursiveCTECompoundTail(cte.Select, accRows)
	return &ResultSet{Cols: accRs.Cols, Rows: accRows}, nil
}

// recursiveCTEWorkLimit returns the maximum number of rows the recursive
// working table needs before its compound LIMIT/OFFSET tail is finalised. A
// bounded CTE must stop producing rows at that point; merely trimming after
// recursion would still overflow the iteration cap for an otherwise infinite
// `... UNION ALL ... LIMIT n` query.
func recursiveCTEWorkLimit(s *Select) int {
	if s.Limit == nil || *s.Limit < 0 {
		return -1
	}
	if *s.Limit == 0 {
		return 0
	}
	offset := 0
	if s.Offset != nil && *s.Offset > 0 {
		offset = *s.Offset
	}
	if offset > int(^uint(0)>>1)-*s.Limit {
		return -1
	}
	return offset + *s.Limit
}

func applyRecursiveCTECompoundTail(s *Select, rows []Row) []Row {
	if len(s.OrderBy) > 0 {
		rows = applySortOrderWithLimit(s.OrderBy, rows, s.Limit, s.Offset)
	}
	return applyOffsetLimit(s, rows)
}
