package tinysql_test

import (
	"context"
	"fmt"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// dependencyExampleDB keeps the example focused on view dependencies instead
// of repeating the parse-and-execute plumbing for every SQL statement.
type dependencyExampleDB struct {
	db  *tsql.DB
	ctx context.Context
}

func (e dependencyExampleDB) execute(sql string) *tsql.ResultSet {
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

func Example_viewDependencies() {
	example := dependencyExampleDB{db: tsql.NewDB(), ctx: context.Background()}
	example.execute("CREATE TABLE customers (id INT, active BOOL)")
	example.execute("CREATE TABLE orders (customer_id INT, amount INT)")
	example.execute(`
		CREATE VIEW active_customer_orders AS
		SELECT orders.amount
		FROM orders JOIN customers ON orders.customer_id = customers.id
		WHERE customers.active = true
	`)

	rs := example.execute(`
		SELECT depends_on_name
		FROM sys.dependencies
		WHERE object_name = 'active_customer_orders'
		ORDER BY depends_on_name
	`)
	for _, row := range rs.Rows {
		fmt.Println(row["depends_on_name"])
	}

	// Output:
	// customers
	// orders
}
