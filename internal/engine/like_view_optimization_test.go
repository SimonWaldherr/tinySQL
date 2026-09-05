package engine

import (
	"context"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompiledLikeMatchesRuneReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	alphabet := []string{"a", "b", "A", "%", "_", "\\", "é", "Ω", "�", "\xff"}
	randomText := func(maxLen int) string {
		var b strings.Builder
		for n := rng.Intn(maxLen + 1); n > 0; n-- {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	for i := 0; i < 10000; i++ {
		pattern, text := randomText(10), randomText(15)
		for _, insensitive := range []bool{false, true} {
			p, s := pattern, text
			if insensitive {
				p = strings.ToLower(p)
				s = strings.ToLower(s)
			}
			want := matchLikePattern(s, p, '\\')
			if got := compileLikeStringMatcher(pattern, insensitive)(text); got != want {
				t.Fatalf("pattern=%q text=%q insensitive=%v got=%v want=%v", pattern, text, insensitive, got, want)
			}
		}
	}
	for _, tc := range []struct {
		pattern, text string
		want          bool
	}{
		{"%a%a", "a", false}, {"ab%bc", "abc", false}, {"ab%bc", "abbc", true},
		{"%ab%cd%ef", "xxabyycdzzef", true}, {"%ab%cd%ef", "xxabyycdzzef!", false},
		{`a\%`, `a%`, true}, {`a\\`, `a\`, true}, {"%%a%%%b%%", "ab", true},
	} {
		if got := compileLikeStringMatcher(tc.pattern, false)(tc.text); got != tc.want {
			t.Fatalf("%#v got %v", tc, got)
		}
	}
}

func TestLikeFilterMatchesSQLValueConversion(t *testing.T) {
	for _, pattern := range []string{"%", "1%", "%2%3", `a\%`, "_", "%é%"} {
		for _, negate := range []bool{false, true} {
			for _, value := range []any{nil, 123, "a%", []byte("123"), "é"} {
				filter := buildCompiledLikeFilter(0, pattern, negate)
				got, err := filter([]any{value})
				want := false
				if value != nil {
					want = matchLikePattern(valueText(value), pattern, '\\')
					if negate {
						want = !want
					}
				}
				if err != nil || got != want {
					t.Fatalf("pattern=%q value=%#v negate=%v got=%v err=%v", pattern, value, negate, got, err)
				}
			}
		}
	}
	if buildRawFilterLike(map[string]int{"v": 0}, &LikeExpr{Expr: &VarRef{Name: "v"}, Pattern: &Literal{Val: "a%", Parameter: true}}) != nil {
		t.Fatal("bound pattern captured as constant")
	}
}

func TestMaterializationBlocksKeepRowsIndependent(t *testing.T) {
	rs := &ResultSet{Cols: []string{"ID", "Text"}, Rows: make([]Row, 5000)}
	for i := range rs.Rows {
		rs.Rows[i] = Row{"id": i, "text": "value"}
	}
	table := materializeViewResult("snapshot", rs)
	if len(table.Rows) != 5000 || table.Cols[0].Type != storage.IntType {
		t.Fatal("invalid shape or inferred type")
	}
	for i, row := range table.Rows {
		if row[0] != i || row[1] != "value" || cap(row) != len(row) {
			t.Fatalf("row %d: %#v cap=%d", i, row, cap(row))
		}
	}
	extended := append(table.Rows[0], "new")
	extended[0] = "changed"
	table.Rows[2048][1] = "edited"
	if table.Rows[1][0] != 1 || table.Rows[2049][1] != "value" || rs.Rows[2048]["text"] != "value" {
		t.Fatal("row storage aliases another row/source")
	}
	empty := materializeViewResult("empty", &ResultSet{Cols: []string{"ID"}})
	if len(empty.Rows) != 0 || len(empty.Cols) != 1 || empty.Cols[0].Type != storage.TextType {
		t.Fatal("empty schema changed")
	}
	// Preserve missing-vs-NULL and qualified alias behavior for ordinary views.
	rows := rowsFromResultSet(&ResultSet{Cols: []string{"ID", "Absent"}, Rows: []Row{{"id": nil}}}, "Mixed")
	if !reflect.DeepEqual(rows, []Row{{"id": nil, "mixed.id": nil}}) {
		t.Fatal(rows)
	}
}

func TestMaterializedRefreshPublishesIndependentSnapshot(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE source (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO source VALUES (1, 'old'), (2, NULL)`)
	execSQL(t, db, `CREATE MATERIALIZED VIEW snapshot AS SELECT id AS ID, body AS Body FROM source WITH DATA`)
	mv, ok := db.Catalog().GetMaterializedView("main", "snapshot")
	if !ok {
		t.Fatal("materialized view not registered")
	}
	old, err := db.Get("default", mv.CacheTableName)
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE source SET body = 'new' WHERE id = 1`)
	execSQL(t, db, `REFRESH MATERIALIZED VIEW snapshot`)
	fresh, err := db.Get("default", mv.CacheTableName)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == old || old.Rows[0][1] != "old" || fresh.Rows[0][1] != "new" || fresh.Rows[1][1] != nil {
		t.Fatal("refresh mutated existing snapshot or lost NULL")
	}
	execSQL(t, db, `DROP TABLE source`)
	if _, err := Execute(context.Background(), db, "default", mustParse(`REFRESH MATERIALIZED VIEW snapshot`)); err == nil {
		t.Fatal("expected failed refresh")
	}
	retained, err := db.Get("default", mv.CacheTableName)
	if err != nil || retained != fresh {
		t.Fatal("failed refresh discarded last good snapshot", err)
	}
}
