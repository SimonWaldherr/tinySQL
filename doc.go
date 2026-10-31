// Package tinysql provides a lightweight, embeddable SQL database for Go applications.
//
// TinySQL is an educational SQL database that demonstrates core database concepts including:
//   - SQL parsing and execution (DDL, DML, SELECT with joins, aggregates, CTEs)
//   - Multi-Version Concurrency Control (MVCC) with snapshot isolation
//   - Write-Ahead Logging (WAL) for durability and crash recovery
//   - Multi-tenancy support for isolated data namespaces
//   - In-memory and persistent storage with GOB serialization
//
// # Basic Usage
//
// Create a database, execute SQL, and query results:
//
//	db := tinysql.NewDB()
//	ctx := context.Background()
//
//	// Parse and execute DDL
//	stmt, _ := tinysql.ParseSQL("CREATE TABLE users (id INT, name TEXT)")
//	tinysql.Execute(ctx, db, "default", stmt)
//
//	// Insert data
//	stmt, _ = tinysql.ParseSQL("INSERT INTO users VALUES (1, 'Alice')")
//	tinysql.Execute(ctx, db, "default", stmt)
//
//	// Query data
//	stmt, _ = tinysql.ParseSQL("SELECT * FROM users WHERE id = 1")
//	rs, _ := tinysql.Execute(ctx, db, "default", stmt)
//	for _, row := range rs.Rows {
//	    fmt.Println(row)
//	}
//
// # Persistence
//
// Save and load database snapshots:
//
//	// Save to file
//	tinysql.SaveToFile(db, "mydb.gob")
//
//	// Load from file
//	db, err := tinysql.LoadFromFile("mydb.gob")
//
// # Advanced Features
//
// Enable MVCC for concurrent transactions:
//
//	mvcc := db.MVCC()
//	tx, _ := mvcc.BeginTx(tinysql.SnapshotIsolation)
//	// ... perform transaction operations ...
//	mvcc.CommitTx(tx.ID)
//
// Enable WAL for durability:
//
//	wal, _ := tinysql.NewAdvancedWAL("data/wal.log")
//	db.AttachAdvancedWAL(wal)
//
// # Parameters and scripts
//
// Bind untrusted values instead of formatting them into SQL text, and run
// multi-statement scripts in one call:
//
//	rs, err := tinysql.ExecSQLArgs(ctx, db, "default",
//	    "SELECT name FROM users WHERE id = ? AND name <> ?", 1, "O'Hara")
//
//	_, err = tinysql.ExecScript(ctx, db, "default", `
//	    CREATE TABLE tags (id INT, label TEXT);
//	    INSERT INTO tags VALUES (1, 'a;b');`)
//
// # Query Compilation
//
// Pre-compile repeated statements so parsing is reused:
//
//	cache := tinysql.NewQueryCache(100)
//	query, _ := cache.Compile("SELECT * FROM users WHERE active = TRUE")
//	rs, _ := query.Execute(ctx, db, "default")
//
// Compiled queries have no placeholders; use ExecSQLArgs or prepared
// statements of the database/sql driver for parameterized SQL.
//
// For more examples, see the example_test.go file in the repository.
package tinysql
