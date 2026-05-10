package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

// ExampleOpen demonstrates opening a tinySQL in-memory database via database/sql.
func ExampleOpen() {
	db, err := tsqldriver.Open("mem://?tenant=default")
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE products (id INT, name TEXT, price FLOAT)`); err != nil {
		fmt.Println("create error:", err)
		return
	}
	if _, err := db.Exec(`INSERT INTO products VALUES (?, ?, ?)`, 1, "Widget", 9.99); err != nil {
		fmt.Println("insert error:", err)
		return
	}

	var name string
	var price float64
	row := db.QueryRow(`SELECT name, price FROM products WHERE id = ?`, 1)
	if err := row.Scan(&name, &price); err != nil {
		fmt.Println("scan error:", err)
		return
	}
	fmt.Printf("%s: $%.2f\n", name, price)

	// Output:
	// Widget: $9.99
}

// ExampleOpenInMemory demonstrates the OpenInMemory convenience helper.
func ExampleOpenInMemory() {
	db, err := tsqldriver.OpenInMemory("demo")
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE kv (k TEXT, v TEXT)`); err != nil {
		fmt.Println("create error:", err)
		return
	}
	if _, err := db.Exec(`INSERT INTO kv VALUES (?, ?)`, "hello", "world"); err != nil {
		fmt.Println("insert error:", err)
		return
	}

	var v string
	if err := db.QueryRow(`SELECT v FROM kv WHERE k = ?`, "hello").Scan(&v); err != nil {
		fmt.Println("scan error:", err)
		return
	}
	fmt.Println(v)

	// Output:
	// world
}

// ExampleOpenWithConfig demonstrates config-driven setup with explicit pool and
// timeout settings, then executes a query via database/sql.
func ExampleOpenWithConfig() {
	cfg := tsqldriver.DefaultOpenConfig()
	cfg.Tenant = "example"
	cfg.PoolReaders = 2
	cfg.BusyTimeout = 100 * time.Millisecond
	cfg.MaxOpenConns = 4
	cfg.MaxIdleConns = 2
	cfg.PingTimeout = 3 * time.Second

	ctx := context.Background()
	db, err := tsqldriver.OpenWithConfig(ctx, cfg)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE TABLE notes (id INT, body TEXT)`); err != nil {
		fmt.Println("create error:", err)
		return
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO notes VALUES (?, ?)`, 1, "hello tinySQL"); err != nil {
		fmt.Println("insert error:", err)
		return
	}

	rows, err := db.QueryContext(ctx, `SELECT id, body FROM notes ORDER BY id`)
	if err != nil {
		fmt.Println("query error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var body string
		if err := rows.Scan(&id, &body); err != nil {
			fmt.Println("scan error:", err)
			return
		}
		fmt.Printf("%d: %s\n", id, body)
	}
	if err := rows.Err(); err != nil {
		fmt.Println("rows error:", err)
	}

	// Output:
	// 1: hello tinySQL
}

// ExampleOpen_transaction demonstrates wrapping multiple operations in a
// database/sql transaction.
func ExampleOpen_transaction() {
	db, err := tsqldriver.Open("mem://?tenant=txdemo")
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE accounts (id INT, balance FLOAT)`); err != nil {
		fmt.Println("create error:", err)
		return
	}
	if _, err := db.Exec(`INSERT INTO accounts VALUES (1, 1000.0)`); err != nil {
		fmt.Println("insert error:", err)
		return
	}
	if _, err := db.Exec(`INSERT INTO accounts VALUES (2, 500.0)`); err != nil {
		fmt.Println("insert error:", err)
		return
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Println("begin error:", err)
		return
	}

	if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = balance - ? WHERE id = ?`, 200.0, 1); err != nil {
		_ = tx.Rollback()
		fmt.Println("update error:", err)
		return
	}
	if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = balance + ? WHERE id = ?`, 200.0, 2); err != nil {
		_ = tx.Rollback()
		fmt.Println("update error:", err)
		return
	}
	if err := tx.Commit(); err != nil {
		fmt.Println("commit error:", err)
		return
	}

	rows, err := db.Query(`SELECT id, balance FROM accounts ORDER BY id`)
	if err != nil {
		fmt.Println("query error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var bal float64
		if err := rows.Scan(&id, &bal); err != nil {
			fmt.Println("scan error:", err)
			return
		}
		fmt.Printf("account %d: %.0f\n", id, bal)
	}

	// Output:
	// account 1: 800
	// account 2: 700
}

// Example_namedDriverAccess demonstrates that all three Open helpers use the
// same registered driver name and produce a working *sql.DB.
func Example_namedDriverAccess() {
	db, err := sql.Open(tsqldriver.DriverName, "mem://?tenant=named_test")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		fmt.Println("ping error:", err)
		return
	}
	fmt.Println("driver name:", tsqldriver.DriverName)
	fmt.Println("ping: ok")

	// Output:
	// driver name: tinysql
	// ping: ok
}
