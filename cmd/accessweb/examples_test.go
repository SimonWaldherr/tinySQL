package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
	idrv "github.com/SimonWaldherr/tinySQL/internal/driver"

	_ "github.com/SimonWaldherr/tinySQL/driver"
)

func ExampleApp_apiExportHandler() {
	nativeDB := tinysql.NewDB()
	idrv.SetDefaultDB(nativeDB)
	sqlDB, err := sql.Open("tinysql", "mem://?tenant=example_export")
	if err != nil {
		panic(err)
	}
	defer sqlDB.Close()

	tpl, err := parseTemplates()
	if err != nil {
		panic(err)
	}
	app := newApp(nativeDB, sqlDB, "example_export", tpl)

	if _, err := app.sqlDB.Exec("CREATE TABLE people (id INT, name TEXT)"); err != nil {
		panic(err)
	}
	if _, err := app.sqlDB.Exec("INSERT INTO people (id, name) VALUES (1, 'Alice')"); err != nil {
		panic(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/export",
		strings.NewReader(`{"sql":"SELECT * FROM people","format":"csv"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	app.apiExportHandler(w, req)

	fmt.Println(w.Code)
	fmt.Println(strings.Contains(w.Header().Get("Content-Type"), "text/csv"))
	fmt.Println(strings.Contains(w.Body.String(), "Alice"))
	// Output:
	// 200
	// true
	// true
}
