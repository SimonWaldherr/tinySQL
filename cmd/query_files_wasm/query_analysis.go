package main

import (
	"fmt"

	"github.com/SimonWaldherr/tinySQL/sqlutil"
)

// The analysis action executes the input to measure it. Restrict the action
// to one parsed SELECT, including SELECT statements that begin with WITH.
func queryAnalysisSQL(sql string) (string, error) {
	statements := sqlutil.SplitStatements(sql)
	if len(statements) != 1 {
		return "", fmt.Errorf("Query analysis requires a single SELECT statement")
	}
	statement := statements[0]
	analysis, err := sqlutil.Analyze(statement)
	if err != nil {
		return "", fmt.Errorf("Parse error: %w", err)
	}
	if analysis.Kind != sqlutil.KindSelect || !analysis.ReadOnly {
		return "", fmt.Errorf("Query analysis requires a single SELECT statement")
	}
	return "EXPLAIN ANALYZE " + statement, nil
}
