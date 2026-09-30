//go:build tinysql_minimal

package tinysql_test

import (
	"context"
	"strings"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

// TestMinimalBuildKeepsCoreSQL covers the in-memory CRUD path that browser
// WASM embedders rely on when they build with -tags tinysql_minimal.
func TestMinimalBuildKeepsCoreSQL(t *testing.T) {
	db, err := tsqldriver.Open("mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		"CREATE TABLE IF NOT EXISTS pois (id TEXT PRIMARY KEY, name TEXT, lat FLOAT, ts INT)",
		"INSERT INTO pois (id, name, lat, ts) VALUES ('a', 'Alpha', 48.1, 1)",
		"INSERT INTO pois (id, name, lat, ts) VALUES ('b', 'Beta', 48.2, 2)",
		"DELETE FROM pois WHERE id = 'a'",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var name string
	var lat float64
	if err := db.QueryRow("SELECT name, lat FROM pois ORDER BY ts DESC").Scan(&name, &lat); err != nil {
		t.Fatal(err)
	}
	if name != "Beta" || lat != 48.2 {
		t.Fatalf("got %q %v", name, lat)
	}
}

func TestMinimalBuildRejectsExcludedFeatures(t *testing.T) {
	db, err := tsqldriver.Open("mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"SELECT HTTP('https://example.com')",
		`SELECT HTML_TEMPLATE('<b>{{.name}}</b>', '{"name":"x"}')`,
	} {
		var value any
		if err := db.QueryRow(query).Scan(&value); err == nil {
			t.Fatalf("%s succeeded in tinysql_minimal build", query)
		}
	}
	var escaped string
	if err := db.QueryRow(`SELECT HTML_ESCAPE('<b>"x" & y</b>')`).Scan(&escaped); err != nil {
		t.Fatal(err)
	}
	if escaped != `&lt;b&gt;&#34;x&#34; &amp; y&lt;/b&gt;` {
		t.Fatalf("HTML_ESCAPE = %q", escaped)
	}
	_, err = tsql.ImportYAML(context.Background(), tsql.NewDB(), "default", "t", strings.NewReader("- id: 1\n"), &tsql.ImportOptions{CreateTable: true})
	if err == nil || !strings.Contains(err.Error(), "tinysql_minimal") {
		t.Fatalf("ImportYAML error = %v, want tinysql_minimal hint", err)
	}
}
