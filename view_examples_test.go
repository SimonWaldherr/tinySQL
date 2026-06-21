package tinysql_test

import (
	"context"
	"fmt"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func Example_viewsAndMaterializedViews() {
	db := tsql.NewDB()
	ctx := context.Background()

	execSQL := func(sql string) {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			panic(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			panic(err)
		}
	}
	querySQL := func(sql string) float64 {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			panic(err)
		}
		rs, err := tsql.Execute(ctx, db, "default", stmt)
		if err != nil {
			panic(err)
		}
		return rs.Rows[0]["total"].(float64)
	}

	execSQL("CREATE TABLE orders (customer_id INT, amount INT, status TEXT)")
	execSQL("INSERT INTO orders VALUES (1, 10, 'paid'), (1, 4, 'open'), (2, 7, 'paid')")

	execSQL(`
		CREATE VIEW paid_customer_totals AS
		WITH paid_orders AS (
			SELECT customer_id, amount FROM orders WHERE status = 'paid'
		)
		SELECT customer_id, SUM(amount) AS total
		FROM paid_orders
		GROUP BY customer_id
	`)
	fmt.Printf("view: %.0f\n", querySQL("SELECT total FROM paid_customer_totals WHERE customer_id = 1"))

	execSQL(`
		CREATE MATERIALIZED VIEW paid_customer_totals_mv AS
		WITH paid_orders AS (
			SELECT customer_id, amount FROM orders WHERE status = 'paid'
		)
		SELECT customer_id, SUM(amount) AS total
		FROM paid_orders
		GROUP BY customer_id
		WITH DATA
	`)

	execSQL("INSERT INTO orders VALUES (1, 5, 'paid')")
	fmt.Printf("materialized before refresh: %.0f\n", querySQL("SELECT total FROM paid_customer_totals_mv WHERE customer_id = 1"))

	execSQL("REFRESH MATERIALIZED VIEW paid_customer_totals_mv")
	fmt.Printf("materialized after refresh: %.0f\n", querySQL("SELECT total FROM paid_customer_totals_mv WHERE customer_id = 1"))

	execSQL("ALTER MATERIALIZED VIEW paid_customer_totals_mv TO VIEW")
	execSQL("INSERT INTO orders VALUES (1, 2, 'paid')")
	fmt.Printf("converted view: %.0f\n", querySQL("SELECT total FROM paid_customer_totals_mv WHERE customer_id = 1"))

	// Output:
	// view: 10
	// materialized before refresh: 10
	// materialized after refresh: 15
	// converted view: 17
}
