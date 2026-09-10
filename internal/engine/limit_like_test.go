package engine

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestLimitLikePaginationMatchesFullResult(t *testing.T) {
	db := setupLimitTable(t)
	defer db.Close()
	execSQL(t, db, `CREATE INDEX t_id ON t(id)`)
	for _, sql := range []string{
		`SELECT * FROM t`,
		`SELECT * FROM t WHERE id >= 2`,
		`SELECT * FROM t WHERE id IN (2,4,9)`,
		`SELECT id FROM t WHERE id % 2 = 0`,
		`SELECT * FROM t WHERE id LIKE '%1%'`,
		`SELECT DISTINCT id % 3 AS g FROM t`,
		`SELECT DISTINCT id AS x, id AS x FROM t`,
		`SELECT DISTINCT id % 3 AS g FROM t ORDER BY g DESC`,
		`SELECT * FROM t WHERE id > 0 ORDER BY id DESC`,
		`SELECT id, COUNT(*) AS n FROM t GROUP BY id ORDER BY n DESC, id DESC`,
		`SELECT id, COUNT(*) AS n FROM t GROUP BY id ORDER BY id DESC`,
	} {
		full := execSQL(t, db, sql)
		for _, limit := range []int{0, 1, 5, math.MaxInt} {
			for _, offset := range []int{0, 1, 9, 10, 100, math.MaxInt} {
				stmt := mustParse(sql).(*Select)
				stmt.Limit, stmt.Offset = &limit, &offset
				rs, err := Execute(context.Background(), db, "default", stmt)
				if err != nil {
					t.Fatalf("%s limit=%d offset=%d: %v", sql, limit, offset, err)
				}
				want := applyOffsetLimit(&Select{Limit: &limit, Offset: &offset}, full.Rows)
				if len(rs.Rows) != len(want) || !reflect.DeepEqual(rs.Cols, full.Cols) {
					t.Fatalf("%s limit=%d offset=%d: wrong page/schema", sql, limit, offset)
				}
				for i := range want {
					if !reflect.DeepEqual(rs.Rows[i], want[i]) {
						t.Fatalf("%s limit=%d offset=%d: row %d: %v want %v", sql, limit, offset, i, rs.Rows[i], want[i])
					}
				}
			}
		}
	}
}

func TestLimitZeroPagedSchemaWithoutLoadingRows(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `CREATE TABLE events (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO events VALUES (1, 'INFO_request')`)
	execSQL(t, db, `CREATE INDEX events_id ON events(id)`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tail := range []string{
		`WHERE body LIKE '%missing%'`, `WHERE id = 1`, `WHERE id >= 0`,
		`WHERE body ILIKE 'info%' ORDER BY id`,
	} {
		stmt := mustParse(`SELECT * FROM events ` + tail + ` LIMIT 0`)
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 0 || !reflect.DeepEqual(rs.Cols, []string{"id", "body"}) {
			t.Fatalf("%s: result=%+v err=%v", tail, rs, err)
		}
	}
	if stats := db.BackendStats(); stats.LoadCount != 0 {
		t.Fatalf("LIMIT 0 loaded source data: %+v", stats)
	}
}

func TestLimitZeroValidationAndCancellation(t *testing.T) {
	db := setupLimitTable(t)
	defer db.Close()
	for _, sql := range []string{
		`SELECT * FROM missing LIMIT 0`,
		`SELECT * FROM t WHERE absent LIKE '%' LIMIT 0`,
		`SELECT * FROM t WHERE absent IN (1,2) LIMIT 0`,
		`SELECT * FROM t WHERE ABS(absent) > 0 LIMIT 0`,
		`SELECT * FROM t WHERE id BETWEEN 0 AND absent LIMIT 0`,
		`SELECT ABS(absent) FROM t LIMIT 0`,
		`SELECT * FROM t ORDER BY absent LIMIT 0`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err == nil {
			t.Fatalf("missing schema error: %s", sql)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, sql := range []string{
		`SELECT * FROM t WHERE id > 0 LIMIT 0`,
		`SELECT * FROM t ORDER BY id LIMIT 0`,
		`SELECT DISTINCT id FROM t LIMIT 0`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: cancellation lost: %v", sql, err)
		}
	}
}

func TestLimitStopsBeforeUnusedExpressions(t *testing.T) {
	db := setupLimitTable(t)
	defer db.Close()
	// LIMIT 0 requests schema only; the filter and projection are never needed.
	zero := execSQL(t, db, `SELECT 1 / (id-id) AS quotient FROM t WHERE 1 / (id-id) > 0 LIMIT 0`)
	if len(zero.Rows) != 0 || !reflect.DeepEqual(zero.Cols, []string{"quotient"}) {
		t.Fatal(zero)
	}
	// A finite page stops before evaluating the next matching row, while
	// expressions on matches skipped by OFFSET still report their errors.
	execSQL(t, db, `SELECT 1 / (id-2) AS quotient FROM t WHERE id > 0 LIMIT 1`)
	if _, err := Execute(t.Context(), db, "default", mustParse(`SELECT 1 / (id-2) AS quotient FROM t WHERE id > 0 LIMIT 1 OFFSET 2`)); err == nil || !strings.Contains(err.Error(), "division by zero") {
		t.Fatalf("lost skipped projection error: %v", err)
	}
	stmt := mustParse(`SELECT * FROM t WHERE id > 0 LIMIT 0`).(*Select)
	for _, n := range []int{0, 2, 0, 1} {
		*stmt.Limit = n
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || len(rs.Rows) != n {
			t.Fatalf("stale cached LIMIT plan for %d: %+v %v", n, rs, err)
		}
	}
}

func TestCompiledLikeEscapesAndUnicodeLowering(t *testing.T) {
	patterns := []string{
		`log\_%`, `%log\_%`, `%\%%`, `a\%b%c\_d`, `\é%`, `abc\`, `abc\\%`,
		`%\_%\_`, `\%`, `\_`, `\`, `\_%_`, `%%%`, `k%`, `i%`, `σ%`, `ς%`, `s%`,
		`K`, `İ`, `Σ`, `K`, `é`, `�`, "\xff%", "", `%`,
	}
	texts := []string{
		"", "log_request", "xlog_request", "%", "_", `abc\`, "a%bc_d", "a_b_",
		"Kelvin", "İstanbul", "Σigma", "ςigma", "ſ", "K", "İ", "Σ", "K", "É", "éclair",
		"�", "\xff", "\xfflog_request", "K\xff", "a\nb", "a\x00b", "%_",
	}
	for _, pattern := range patterns {
		for _, insensitive := range []bool{false, true} {
			match := compileLikeStringMatcher(pattern, insensitive)
			for _, text := range texts {
				s, p := text, pattern
				if insensitive {
					s, p = strings.ToLower(s), strings.ToLower(p)
				}
				if got, want := match(text), matchLikePattern(s, p, '\\'); got != want {
					t.Fatalf("pattern=%q text=%q insensitive=%v: %v want %v", pattern, text, insensitive, got, want)
				}
			}
		}
	}
}

func TestLikeLimitBoundPatternsAndStreaming(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("events", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "body", Type: storage.TextType}}, false)
	for i, value := range []any{nil, "log_request", "LOG_REQUEST", "xlog_request", "Kelvin", "kelvin", "k", "a%b", "a_b", 123, []byte("log_blob")} {
		table.Rows = append(table.Rows, []any{i, value})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	for _, insensitive := range []bool{false, true} {
		for _, negate := range []bool{false, true} {
			stmt := mustParse(`SELECT * FROM events WHERE body LIKE 'placeholder' LIMIT 2 OFFSET 1`).(*Select)
			like := stmt.Where.(*LikeExpr)
			like.CaseInsensitive, like.Negate = insensitive, negate
			pattern := like.Pattern.(*Literal)
			pattern.Parameter = true
			for _, p := range []string{`log\_%`, `%log\_%`, `k%`, `%\%%`, `%`, `1%`, `no_match`} {
				pattern.Val = p
				var want []any
				for _, row := range table.Rows {
					if row[1] == nil {
						continue
					}
					text, pat := valueText(row[1]), p
					if insensitive {
						text, pat = strings.ToLower(text), strings.ToLower(pat)
					}
					if matchLikePattern(text, pat, '\\') != negate {
						want = append(want, row[0])
					}
				}
				if len(want) > 0 {
					want = want[1:]
				}
				if len(want) > 2 {
					want = want[:2]
				}
				rs, err := Execute(context.Background(), db, "default", stmt)
				if err != nil {
					t.Fatal(err)
				}
				var got []any
				for _, row := range rs.Rows {
					got = append(got, row["id"])
				}
				if len(got) != len(want) || len(want) > 0 && !reflect.DeepEqual(got, want) {
					t.Fatalf("pattern=%q insensitive=%v negate=%v: %v want %v", p, insensitive, negate, got, want)
				}
				stream, err := ExecuteStream(context.Background(), db, "default", stmt)
				if err != nil {
					t.Fatal(err)
				}
				got = nil
				for stream.Next() {
					got = append(got, stream.Row()["id"])
				}
				streamErr := stream.Err()
				stream.Close()
				if streamErr != nil || len(got) != len(want) || len(want) > 0 && !reflect.DeepEqual(got, want) {
					t.Fatalf("stream pattern=%q: %v want %v err=%v", p, got, want, streamErr)
				}
			}
		}
	}
}
