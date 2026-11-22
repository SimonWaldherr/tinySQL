package main

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/sqlutil"
)

func TestQueryAnalysisSQLSelectOnly(t *testing.T) {
	for _, query := range []string{
		`SELECT ';' AS value;`,
		"-- SQL comment;\nWITH src AS (SELECT 1 AS value) SELECT * FROM src;",
		`SELECT 1 AS value UNION ALL SELECT 2`,
	} {
		wrapped, err := queryAnalysisSQL(query)
		if err != nil || !strings.HasPrefix(wrapped, "EXPLAIN ANALYZE ") {
			t.Fatalf("analysis of %q = %q, %v", query, wrapped, err)
		}
		analysis, err := sqlutil.Analyze(wrapped)
		if err != nil || analysis.Kind != sqlutil.KindExplain || !analysis.ReadOnly {
			t.Fatalf("wrapped statement %q: %+v, %v", wrapped, analysis, err)
		}
	}
	for _, query := range []string{
		"", `SELECT 1; SELECT 2`, `SELECT 1; DROP TABLE t`, `INSERT INTO t VALUES (1)`,
		`WITH src AS (SELECT 1 AS value) INSERT INTO t SELECT * FROM src`,
		`EXPLAIN DELETE FROM t`, `PRAGMA table_info(t)`, `SELECT 1 DROP TABLE t`,
	} {
		if got, err := queryAnalysisSQL(query); got != "" || err == nil {
			t.Errorf("analysis accepted %q: %q, %v", query, got, err)
		}
	}
}
