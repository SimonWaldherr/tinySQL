package tinystore_test

import (
	"context"
	"fmt"
	"os"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/tinystore"
)

func Example() {
	dir, err := os.MkdirTemp("", "tinystore-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	facts := tsql.NewTable("sales", []tsql.Column{{Name: "region", Type: tsql.TextType}, {Name: "amount", Type: tsql.FloatType}}, false)
	facts.Rows = [][]any{{"north", 12.5}, {"north", 7.5}, {"south", 8.0}}
	if _, err := tinystore.Write(dir, facts); err != nil {
		panic(err)
	}
	tables, err := tinystore.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	db := tsql.NewDB()
	defer db.Close()
	for _, table := range tables {
		if err := db.Put("default", table); err != nil {
			panic(err)
		}
	}
	stmt, err := tsql.ParseSQL("SELECT SUM(amount) AS total FROM sales")
	if err != nil {
		panic(err)
	}
	result, err := tsql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Rows[0]["total"])
	// Output: 28
}
