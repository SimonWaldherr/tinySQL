package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func newRawClockDB(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, "CREATE TABLE clocks (id INT, value TEXT)")
	execSQL(t, db, "INSERT INTO clocks VALUES (1, 'now'), (2, 'now'), (3, 'now')")
	return db
}

func TestRawClockFixedEnvironment(t *testing.T) {
	db := newRawClockDB(t)
	now := time.Date(2024, 3, 5, 6, 7, 8, 123_000_000, time.UTC)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", now: now}
	for _, predicate := range []string{
		"NOW() = CURRENT_TIMESTAMP()",
		"GETDATE() = NOW()",
		"id > 0 AND NOW() = CURRENT_TIMESTAMP()",
		"id = 0 OR NOW() = CURRENT_TIMESTAMP()",
		"CONTAINS_ALL(STRFTIME('%F', 'now'), '2024', '03')",
		"ROW_TO_TEXT() LIKE '%now%' AND ROW_TO_TEXT() LIKE '%1%' AND NOW() = CURRENT_TIMESTAMP()",
		"STRFTIME('%s', value) = STRFTIME('%s', NOW())",
		"IN_PERIOD('TODAY', NOW())",
	} {
		t.Run(predicate, func(t *testing.T) {
			stmt := mustParse("SELECT id, NOW() AS n, DATETIME(value) AS d, RECENCY_SCORE(NOW(), 1) AS r FROM clocks WHERE " + predicate).(*Select)
			result, handled, err := executeSimpleSelectFastPath(env, stmt)
			if err != nil || !handled {
				t.Fatalf("raw execution handled=%v, err=%v", handled, err)
			}
			wantRows := 3
			if strings.HasPrefix(predicate, "ROW_TO_TEXT") {
				wantRows = 1
			}
			if len(result.Rows) != wantRows {
				t.Fatalf("got %d rows, want %d", len(result.Rows), wantRows)
			}
			for _, row := range result.Rows {
				if got := row["n"].(time.Time); !got.Equal(now) {
					t.Errorf("NOW() = %v, want %v", got, now)
				}
				if got := row["d"]; got != "2024-03-05 06:07:08" {
					t.Errorf("DATETIME(value) = %#v", got)
				}
				if got := row["r"]; got != float64(1) {
					t.Errorf("RECENCY_SCORE(NOW(), 1) = %#v, want 1", got)
				}
			}
		})
	}
}

func TestRawClockCachedPlanRefreshes(t *testing.T) {
	db := newRawClockDB(t)
	stmt := mustParse("SELECT NOW() AS n FROM clocks WHERE NOW() = CURRENT_TIMESTAMP()").(*Select)
	first := time.Date(2024, 1, 1, 1, 2, 3, 0, time.UTC)
	for _, now := range []time.Time{first, first.Add(24 * time.Hour)} {
		env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", now: now}
		result, handled, err := executeSimpleSelectFastPath(env, stmt)
		if err != nil || !handled || len(result.Rows) != 3 {
			t.Fatalf("execution: handled=%v, result=%#v, err=%v", handled, result, err)
		}
		for _, row := range result.Rows {
			if got := row["n"].(time.Time); !got.Equal(now) {
				t.Fatalf("cached plan returned %v, want %v", got, now)
			}
		}
	}
	if entry := stmt.simplePlanCache.entry.Load(); entry == nil || !entry.plan.now.IsZero() {
		t.Fatalf("template retained an execution clock: %#v", entry)
	}
}

func TestRawClockCachedPlanConcurrent(t *testing.T) {
	db := newRawClockDB(t)
	stmt := mustParse("SELECT NOW() AS n FROM clocks WHERE NOW() = CURRENT_TIMESTAMP()").(*Select)
	for worker := range 8 {
		t.Run(fmt.Sprintf("worker%d", worker), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2024, 1, worker+1, 1, 2, 3, 0, time.UTC)
			for range 8 {
				env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", now: now}
				result, handled, err := executeSimpleSelectFastPath(env, stmt)
				if err != nil || !handled || len(result.Rows) != 3 {
					t.Fatalf("execution: handled=%v, result=%#v, err=%v", handled, result, err)
				}
				for _, row := range result.Rows {
					if got := row["n"].(time.Time); !got.Equal(now) {
						t.Fatalf("another execution's clock: got %v, want %v", got, now)
					}
				}
			}
		})
	}
}

func TestRawClockExecutionAPIs(t *testing.T) {
	db := newRawClockDB(t)
	stmt := mustParse("SELECT NOW() AS n, CURRENT_TIMESTAMP() AS c, RECENCY_SCORE(NOW(), 1) AS r FROM clocks WHERE NOW() = CURRENT_TIMESTAMP()")
	for _, method := range []string{"materialized", "columnar", "stream1", "stream128"} {
		t.Run(method, func(t *testing.T) {
			var previous time.Time
			for range 2 {
				var rows []Row
				switch method {
				case "materialized":
					result, err := Execute(t.Context(), db, "default", stmt)
					if err != nil {
						t.Fatal(err)
					}
					rows = result.Rows
				case "columnar":
					result, err := ExecuteColumnar(t.Context(), db, "default", stmt)
					if err != nil {
						t.Fatal(err)
					}
					for i := range result.RowCount {
						rows = append(rows, Row{"n": result.Values[0][i], "c": result.Values[1][i], "r": result.Values[2][i]})
					}
				default:
					buffer := 1
					if method == "stream128" {
						buffer = 128
					}
					stream, err := ExecuteStreamWithOptions(t.Context(), db, "default", stmt, StreamOptions{Buffer: buffer})
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					for stream.Next() {
						row := stream.Row()
						rows = append(rows, Row{"n": row["n"], "c": row["c"], "r": row["r"]})
					}
					if err := stream.Err(); err != nil {
						t.Fatal(err)
					}
				}
				if len(rows) != 3 {
					t.Fatalf("got %d rows, want 3", len(rows))
				}
				now := rows[0]["n"].(time.Time)
				for _, row := range rows {
					if !row["n"].(time.Time).Equal(now) || !row["c"].(time.Time).Equal(now) || row["r"] != float64(1) {
						t.Fatalf("inconsistent statement clocks: %#v", rows)
					}
				}
				if !previous.IsZero() && !now.After(previous) {
					t.Fatalf("clock did not refresh: %v then %v", previous, now)
				}
				previous = now
			}
		})
	}
}

func TestRawClockAggregateAndDML(t *testing.T) {
	db := newRawClockDB(t)
	now := time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)
	env := ExecEnv{ctx: context.Background(), db: db, tenant: "default", now: now}
	stmt := mustParse("SELECT COUNT(NOW()) AS n FROM clocks WHERE NOW() = CURRENT_TIMESTAMP()").(*Select)
	result, handled, err := executeSimpleAggregateFastPath(env, stmt)
	if err != nil || !handled || len(result.Rows) != 1 || result.Rows[0]["n"] != 3 {
		t.Fatalf("aggregate clock: result=%#v, handled=%v, err=%v", result, handled, err)
	}
	result, err = execStmt(env, mustParse("UPDATE clocks SET value = STRFTIME('%F', 'now') WHERE NOW() = CURRENT_TIMESTAMP()"))
	if err != nil || result.Rows[0]["updated"] != 3 {
		t.Fatalf("update clock: result=%#v, err=%v", result, err)
	}
	for _, row := range execSQL(t, db, "SELECT value FROM clocks").Rows {
		if row["value"] != "2024-03-05" {
			t.Fatalf("UPDATE used another clock: %#v", row)
		}
	}
	result, err = execStmt(env, mustParse("DELETE FROM clocks WHERE NOW() = CURRENT_TIMESTAMP()"))
	if err != nil || result.Rows[0]["deleted"] != 3 {
		t.Fatalf("delete clock: result=%#v, err=%v", result, err)
	}
}
