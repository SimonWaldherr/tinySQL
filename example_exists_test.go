package tinysql_test

import (
	"context"
	"fmt"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// existsExampleDB contains the minimal setup and execution helpers for the
// fluent EXISTS example.
type existsExampleDB struct {
	db  *tsql.DB
	ctx context.Context
}

func (e existsExampleDB) execute(sql string) *tsql.ResultSet {
	stmt, err := tsql.ParseSQL(sql)
	if err != nil {
		panic(err)
	}
	rs, err := tsql.Execute(e.ctx, e.db, "default", stmt)
	if err != nil {
		panic(err)
	}
	return rs
}

func ExampleExists() {
	example := existsExampleDB{db: tsql.NewDB(), ctx: context.Background()}
	example.execute("CREATE TABLE items (id INT, name TEXT)")
	example.execute("CREATE TABLE tags (item_id INT)")
	example.execute("INSERT INTO items VALUES (1, 'Ada'), (2, 'Linus')")
	example.execute("INSERT INTO tags VALUES (1)")

	query := tsql.Select(tsql.Col("name")).
		From("items").
		Where(tsql.Exists(tsql.SelectStar().From("tags"))).
		Build()
	results, err := tsql.Execute(example.ctx, example.db, "default", query)
	if err != nil {
		panic(err)
	}
	for _, row := range results.Rows {
		fmt.Println(row["name"])
	}

	// Output:
	// Ada
	// Linus
}
