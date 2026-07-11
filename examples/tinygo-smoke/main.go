// tinygo-smoke verifies the minimal tinySQL API on TinyGo targets.
//
// Run with:
//
//	tinygo run -target=wasm ./examples/tinygo-smoke
package main

import (
	"context"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func main() {
	db := tinysql.NewDB()
	stmt, err := tinysql.ParseSQL("SELECT 1 AS ready")
	if err != nil {
		panic(err)
	}
	result, err := tinysql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		panic(err)
	}
	if len(result.Rows) != 1 {
		panic("expected one result row")
	}

	// HTTP is deliberately retained as a SQL function on TinyGo, but returns
	// an actionable unsupported-target error instead of pulling in net/http.
	httpStmt, err := tinysql.ParseSQL("SELECT HTTP('https://example.invalid')")
	if err != nil {
		panic(err)
	}
	if _, err := tinysql.Execute(context.Background(), db, "default", httpStmt); err == nil {
		panic("expected HTTP() to be unavailable on TinyGo")
	}
}
