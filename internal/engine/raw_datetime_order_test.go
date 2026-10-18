package engine

import (
	"reflect"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawDateTimeEvaluationOrder(t *testing.T) {
	plan := &simpleSelectPlan{now: time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)}
	env := ExecEnv{now: plan.now}
	for _, text := range []string{
		"DATE_ADD(NULL, 1/0, 'DAY')",
		"DATE_SUB(NULL, 1/0, 'DAY')",
		"DATE_ADD(NULL, 1/0)",
		"DATE_SUB(NULL, 1/0)",
		"DATEADD(NULL, 1/0, 'DAY')",
		"DATESUB(NULL, 1/0, 'DAY')",
		"DATEADD(NULL, 1/0)",
		"DATESUB(NULL, 1/0)",
		"DATE_ADD('invalid', 1/0, 'DAY')",
		"DATE_SUB('invalid', 1/0, 'DAY')",
		"DATE_ADD('invalid', NULL, 'DAY')",
		"DATE_ADD('2024-01-01', NULL, 1/0)",
		"DATE_SUB('2024-01-01', NULL, 1/0)",
		"DATE_ADD(1/0)",
		"DATEDIFF(NULL, 1/0, 1/0)",
		"DATEDIFF(1, 1/0, 1/0)",
		"DATEDIFF('DAYS', NULL, 1/0)",
		"DATEDIFF('DAYS', 'invalid', NULL)",
		"DATEDIFF(1/0)",
		"EXTRACT(NULL, 1/0)",
		"EXTRACT(123, 1/0)",
		"EXTRACT(1/0)",
		"DATE_TRUNC(NULL, 1/0)",
		"DATE_TRUNC(123, NULL)",
		"DATE_TRUNC(123, 1/0)",
		"DATE_TRUNC(1/0)",
		"EOMONTH(NULL, 1/0)",
		"EOMONTH('invalid', 1/0)",
		"EOMONTH(1/0, 1/0, 1/0)",
		"ADD_MONTHS(NULL, 1/0)",
		"ADD_MONTHS('invalid', 1/0)",
		"ADD_MONTHS(1/0)",
		"STRFTIME(NULL, 1/0)",
		"STRFTIME(NULL, 1/0, 'modifier')",
		"STRFTIME('%Y', NULL, 'modifier')",
		"DATE(1/0, 'modifier')",
		"TIME(1/0, 'modifier')",
		"DATETIME(1/0, 'modifier')",
		"JULIANDAY(1/0, 'modifier')",
		"UNIXEPOCH(1/0, 'modifier')",
		"NOW(1/0)",
		"GETDATE(1/0)",
		"TODAY(1/0)",
		"CURRENT_TIMESTAMP(1/0)",
		"CURRENT_TIME(1/0)",
		"CURRENT_DATE(1/0)",
		"OVERLAPS(NULL, 1/0, 1/0, 1/0)",
		"OVERLAPS('invalid', NULL, 1/0, 1/0)",
		"OVERLAPS('2024-01-01', NULL, 1/0, 1/0)",
		"OVERLAPS(1/0, 1/0, 1/0)",
	} {
		t.Run(text, func(t *testing.T) {
			expr := mustParse("SELECT " + text).(*Select).Projs[0].Expr
			want, wantErr := evalExpr(env, expr, nil)
			got, gotErr := evalRawExpr(plan, nil, expr)
			if (wantErr == nil) != (gotErr == nil) || wantErr != nil && wantErr.Error() != gotErr.Error() {
				t.Fatalf("generic result=%#v, error=%v; raw result=%#v, error=%v", want, wantErr, got, gotErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("generic result=%#v, raw result=%#v", want, got)
			}
		})
	}
}

func TestRawDateTimeNullColumns(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, "CREATE TABLE nullable_dates (value TEXT)")
	execSQL(t, db, "INSERT INTO nullable_dates VALUES (NULL), (NULL)")
	for _, query := range []string{
		"SELECT DATE_ADD(value, 1/0, 'DAY') AS a, STRFTIME(value, 1/0) AS f FROM nullable_dates",
		"SELECT value FROM nullable_dates WHERE DATE_ADD(value, 1/0, 'DAY') IS NULL AND STRFTIME(value, 1/0) IS NULL",
	} {
		t.Run(query, func(t *testing.T) {
			stmt := mustParse(query).(*Select)
			if _, handled, err := buildSimpleSelectPlan(ExecEnv{ctx: t.Context(), db: db, tenant: "default"}, stmt); err != nil || !handled {
				t.Fatalf("raw plan: handled=%v, error=%v", handled, err)
			}
			result := execSQL(t, db, query)
			if len(result.Rows) != 2 {
				t.Fatalf("got %d rows, want 2", len(result.Rows))
			}
			for _, row := range result.Rows {
				for _, column := range result.Cols {
					if row[column] != nil {
						t.Fatalf("expected NULL output: %#v", row)
					}
				}
			}
		})
	}
	result := execSQL(t, db, "SELECT COUNT(*) AS n FROM nullable_dates WHERE DATE_ADD(value, 1/0, 'DAY') IS NULL AND STRFTIME(value, 1/0) IS NULL")
	if len(result.Rows) != 1 || result.Rows[0]["n"] != 2 {
		t.Fatalf("nullable raw aggregate: %#v", result)
	}
}
