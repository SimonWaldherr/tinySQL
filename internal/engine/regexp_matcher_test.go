package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRegexpMatcherMatchesGo(t *testing.T) {
	patterns := []string{
		``, `^`, `$`, `^$`, `()`, `abc`, `^abc`, `abc$`, `^abc$`, `\Aabc\z`,
		`a.*b`, `^a.*b`, `a.*b$`, `^a.*b$`, `^a.*?b$`, `(?s)^a.*b$`,
		`.*`, `^.*`, `.*$`, `^.*$`, `(?s)^.*$`, `^a.*b.*c$`, `(?s)a.*b.*c`,
		`^a\nb.*c$`, `a\nb`, `(?s)a\nb.*c`, `a(?s:.*)b.*c`, `(?s:a.*)b(?-s:.*)c`,
		`^a\.\*b\$$`, `^\^a$`, `^a(.*)b$`, `^(?P<word>abc)$`, `a()b`,
		`(?m)^a`, `(?m)b$`, `(?m)^a.*b$`, `(?im)^a.*b$`, `(?i)^k`, `(?i)σ$`,
		`a|bc`, `(?:a|b).*c`, `[[:alpha:]]+`, `^\p{L}+$`, `\babc\b`,
		`a{0,3}`, `^a.+b$`, `a.*\x{FFFD}`, `^�$`, `\x{FFFD}`, `[^\n]*`, `(?s:[^x])*`,
		`^é.*🙂$`, `a.*ab`, `^ab.*bc$`, `.*a.*a`, `a$^`, `^a^`, `a$$`,
	}
	texts := []string{
		"", "a", "b", "abc", "abbc", "ab", "aa", "xaabcy", "a\nb", "a\nbc", "a\rbc", "ab\n",
		"\n", "\nabc", "abc\n", "abc\nabc", "a\nb\nc", "a.*b$", "^a", "a\x00b",
		"é🙂", "é\n🙂", "K", "Σ", "ς", "�", "\xff", "a\xffb", "a\xc0\xafbc", "\xc3a",
	}
	check := func(pattern string, inputs []string) {
		t.Helper()
		re, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("bad test pattern %q: %v", pattern, err)
		}
		match, err := compileCachedRegexpMatcher(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, input := range inputs {
			if got, want := match(input), re.MatchString(input); got != want {
				t.Fatalf("pattern=%q input=%q: got %v want %v", pattern, input, got, want)
			}
		}
	}
	for _, pattern := range patterns {
		check(pattern, texts)
	}
	rng := rand.New(rand.NewSource(812))
	literals := []string{"", "a", "ab", "b", "c", "%_", "^$", "\\", "é", "🙂", "�", "\n", "\x00"}
	gaps := []string{`.*`, `.*?`, `(?s:.*)`, `.+`, `[ab]*`, `\b`, `(?:x|y)?`}
	for i := 0; i < 1500; i++ {
		var pattern, candidate strings.Builder
		if rng.Intn(2) == 0 {
			pattern.WriteByte('^')
		}
		for j, n := 0, 1+rng.Intn(4); j < n; j++ {
			lit := literals[rng.Intn(len(literals))]
			pattern.WriteString("(" + regexp.QuoteMeta(lit) + ")")
			candidate.WriteString(lit)
			if j+1 < n {
				pattern.WriteString(gaps[rng.Intn(len(gaps))])
				candidate.WriteString("ab")
			}
		}
		if rng.Intn(2) == 0 {
			pattern.WriteByte('$')
		}
		input := candidate.String()
		inputs := append([]string{input, "x" + input, input + "y", "x" + input + "y", input + "\n", "\n" + input}, texts...)
		check(pattern.String(), inputs)
	}
}

func TestRegexpMatcherSQLPathsAndParameters(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE texts (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO texts VALUES (1,'abc'), (2,'abbc'), (3,'xabc'), (4,NULL)`)
	execSQL(t, db, `CREATE TABLE ids (id INT)`)
	execSQL(t, db, `INSERT INTO ids VALUES (1),(2),(3),(4)`)
	for _, pattern := range []string{`^ab`, `bc$`, `^a.*c$`, `(?i)^AB`, ``, `^$`} {
		re := regexp.MustCompile(pattern)
		var want []any
		for i, text := range []string{"abc", "abbc", "xabc"} {
			if re.MatchString(text) {
				want = append(want, i+1)
			}
		}
		for _, sql := range []string{
			`SELECT id FROM texts WHERE body REGEXP 'pattern'`,
			`SELECT id FROM texts WHERE body RLIKE 'pattern'`,
			`SELECT id FROM texts WHERE REGEXP_MATCH(body, 'pattern')`,
			`SELECT texts.id AS id FROM texts JOIN ids ON texts.id=ids.id AND texts.body REGEXP 'pattern'`,
		} {
			query := strings.Replace(sql, "'pattern'", "'"+pattern+"'", 1)
			rs := execSQL(t, db, query)
			var got []any
			for _, row := range rs.Rows {
				got = append(got, row["id"])
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %v want %v", query, got, want)
			}
		}
	}
	stmt := mustParse(`SELECT id FROM texts WHERE body REGEXP 'pattern' LIMIT 1`).(*Select)
	param := stmt.Where.(*RegexpExpr).Pattern.(*Literal)
	param.Parameter = true
	for _, tc := range []struct {
		pattern string
		id      int
	}{{`^abc$`, 1}, {`^abbc$`, 2}, {`^x`, 3}, {`^abc$`, 1}} {
		param.Val = tc.pattern
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["id"] != tc.id {
			t.Fatalf("stale parameter %q: %+v %v", tc.pattern, rs, err)
		}
	}
	// Projection evaluates through the generic function path. Captures and
	// replacement locations must continue to use the original Go regexp.
	rs := execSQL(t, db, `SELECT REGEXP_MATCH(body, '^a.*c$') AS matched, REGEXP_EXTRACT(body, 'a.*?c') AS extracted, REGEXP_REPLACE(body, '(a)(b)', '$2$1') AS replaced FROM texts WHERE id=1`)
	if rs.Rows[0]["matched"] != true || rs.Rows[0]["extracted"] != "abc" || rs.Rows[0]["replaced"] != "bac" {
		t.Fatal(rs.Rows)
	}
	for _, query := range []string{
		`SELECT id FROM texts WHERE body REGEXP '['`,
		`SELECT REGEXP_MATCH(body, '[') FROM texts`,
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatalf("missing invalid-pattern error: %s", query)
		}
	}
	for _, query := range []string{
		`SELECT id FROM texts WHERE id=4 AND body REGEXP '['`,
		`SELECT id FROM texts WHERE body NOT REGEXP '^a' AND id=4`,
		`SELECT id FROM texts WHERE NOT (body REGEXP '^a') AND id=4`,
	} {
		if rs := execSQL(t, db, query); len(rs.Rows) != 0 {
			t.Fatalf("NULL semantics changed: %s", query)
		}
	}
}

func TestRegexpMatcherConcurrentCacheEviction(t *testing.T) {
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 400; i++ {
				pattern := fmt.Sprintf(`^event_%d.*end$`, i)
				match, err := compileCachedRegexpMatcher(pattern)
				if err != nil || !match(fmt.Sprintf("event_%d_payload_end", i)) || match("unrelated") {
					t.Errorf("bad cached matcher for %q: %v", pattern, err)
					return
				}
			}
		})
	}
	wg.Wait()
	regexCacheMu.RLock()
	n := len(regexCache)
	regexCacheMu.RUnlock()
	if n > regexCacheMaxEntries {
		t.Fatalf("cache exceeded bound: %d", n)
	}
}
