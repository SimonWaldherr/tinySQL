//! cargo run --example quickstart
use tinysql::{params, Database, OpenOptions, StorageMode, Value};

fn main() -> tinysql::Result<()> {
    println!("tinySQL {}", tinysql::version()?);
    let directory = std::env::temp_dir().join("tinysql-rust-quickstart");
    let _ = std::fs::remove_dir_all(&directory);

    {
        // A durable WAL database: every acknowledged write survives a restart.
        let db = OpenOptions::new().mode(StorageMode::Wal).open(&directory)?;
        db.execute_script(
            "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT, image BLOB);
             CREATE INDEX products_name ON products (name);",
        )?;
        let rows = vec![
            vec![Value::from(1), "Coffee".into(), 4.5.into(), Value::Null],
            vec![
                Value::from(2),
                "Tea".into(),
                3.0.into(),
                Value::from(&b"\x89PNG"[..]),
            ],
            vec![Value::from(3), "Cocoa".into(), 3.8.into(), Value::Null],
        ];
        db.execute_batch("INSERT INTO products VALUES (?, ?, ?, ?)", &rows)?;
        db.transaction(|tx| {
            tx.execute(
                "UPDATE products SET price = price * ? WHERE name <> ?",
                params![1.1, "Tea"],
            )
        })?;
    }

    let db = Database::open_durable(&directory, StorageMode::Wal)?;
    for row in &db.query(
        "SELECT id, name, price FROM products WHERE price > ? ORDER BY id",
        params![3.5],
    )? {
        let (id, name, price): (i64, String, f64) =
            (row.get("id")?, row.get("name")?, row.get("price")?);
        println!("{id} {name} {price:.2}");
    }
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}
