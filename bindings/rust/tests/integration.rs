use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use tinysql::{params, Database, Error, OpenOptions, StorageMode, Value};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tinysql-rust-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn version_and_abi() {
    assert!(tinysql::abi_version() >= tinysql::REQUIRED_ABI);
    assert!(!tinysql::version().unwrap().is_empty());
}

#[test]
fn values_round_trip() {
    let db = Database::open_in_memory().unwrap();
    db.execute_script("CREATE TABLE v (i INT, f FLOAT, whole FLOAT, s TEXT, b BOOL, raw BLOB, empty BLOB, n TEXT)")
        .unwrap();
    let values = vec![
        Value::Integer(i64::MAX),
        Value::Real(0.1),
        Value::Real(3.0),
        Value::Text("Grüße ' 🦊 ;".into()),
        Value::Boolean(true),
        Value::Blob(vec![0, 255, 16]),
        Value::Blob(vec![]),
        Value::Null,
    ];
    assert_eq!(
        db.execute("INSERT INTO v VALUES (?, ?, ?, ?, ?, ?, ?, ?)", &values)
            .unwrap(),
        1
    );
    let rows = db.query("SELECT * FROM v", params![]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &values[..]);
    assert_eq!(rows[0].get::<i64>("I").unwrap(), i64::MAX);
    assert_eq!(rows[0].get::<f64>(2).unwrap(), 3.0);
    assert_eq!(rows[0].get::<Option<String>>("n").unwrap(), None);
    assert_eq!(rows[0].get::<Vec<u8>>("raw").unwrap(), vec![0, 255, 16]);
    assert!(matches!(
        rows[0].get::<String>("i"),
        Err(Error::Type { .. })
    ));
    assert!(matches!(
        rows[0].get::<i64>("missing"),
        Err(Error::Column(_))
    ));
    assert!(matches!(rows[0].get::<i64>(99), Err(Error::Column(_))));
    assert!(matches!(rows[0].get::<i32>("i"), Err(Error::Type { .. })));
}

#[test]
fn parameters_are_bound_safely() {
    let db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE users (id INT, name TEXT)", &[])
        .unwrap();
    let evil = "x'); DROP TABLE users; --";
    db.execute("INSERT INTO users VALUES ($1, $2)", params![1, evil])
        .unwrap();
    db.execute(
        "INSERT INTO users VALUES (:1, :2)",
        params![2_i32, Some("b")],
    )
    .unwrap();
    let names: Vec<String> = db
        .query(
            "SELECT name FROM users WHERE id <= ? ORDER BY id -- why?",
            params![2],
        )
        .unwrap()
        .iter()
        .map(|row| row.get(0).unwrap())
        .collect();
    assert_eq!(names, [evil, "b"]);
    assert!(matches!(
        db.execute("SELECT ?", params![f64::NAN]),
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        db.execute("SELECT 1\0", &[]),
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        db.execute("SELECT ?, ?", params![1]),
        Err(Error::Database(_))
    ));
    assert_eq!(
        db.query_value::<String>("SELECT name FROM users WHERE id = ?", params![1])
            .unwrap()
            .unwrap(),
        evil
    );
    assert_eq!(
        db.query_value::<String>("SELECT name FROM users WHERE id = ?", params![9])
            .unwrap(),
        None
    );
}

#[test]
fn batch_script_and_returning() {
    let db = Database::open_in_memory().unwrap();
    let summary = db
        .execute_script(
            "CREATE TABLE src (id INT); CREATE TABLE audit (id INT, note TEXT);
             CREATE TRIGGER copy AFTER INSERT ON src BEGIN INSERT INTO audit VALUES (NEW.id, 'a;b'); END;",
        )
        .unwrap();
    assert_eq!(summary.statements, 3);
    let rows: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::from(i)]).collect();
    let summary = db
        .execute_batch("INSERT INTO src VALUES (?)", &rows)
        .unwrap();
    assert_eq!((summary.statements, summary.rows_affected), (100, 100));
    assert_eq!(
        db.query_value::<i64>("SELECT COUNT(*) FROM audit", &[])
            .unwrap(),
        Some(100)
    );
    assert_eq!(
        db.execute_batch("INSERT INTO src VALUES (?)", Vec::<Vec<Value>>::new())
            .unwrap()
            .statements,
        0
    );
    let deleted = db
        .query("DELETE FROM src WHERE id >= ? RETURNING id", params![98])
        .unwrap();
    assert_eq!(deleted.columns(), ["id"]);
    assert_eq!(deleted.len(), 2);
    match db.execute_script("INSERT INTO src VALUES (1); INSERT INTO nope VALUES (1)") {
        Err(Error::Database(message)) => assert!(message.contains("statement 2"), "{message}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn transactions() {
    let db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)", &[]).unwrap();
    db.transaction(|tx| tx.execute("INSERT INTO t VALUES (1)", &[]))
        .unwrap();
    let failed: tinysql::Result<()> = db.transaction(|tx| {
        tx.execute("INSERT INTO t VALUES (2)", &[])?;
        assert!(tx.in_transaction());
        Err(Error::InvalidInput("abort".into()))
    });
    assert!(failed.is_err());
    assert!(!db.in_transaction());
    db.execute_script("BEGIN; INSERT INTO t VALUES (3)")
        .unwrap();
    assert!(db.in_transaction());
    db.execute("ROLLBACK", &[]).unwrap();
    assert!(!db.in_transaction());
    assert_eq!(
        db.query_value::<i64>("SELECT COUNT(*) FROM t", &[])
            .unwrap(),
        Some(1)
    );
}

#[test]
fn snapshots_and_durable_modes() {
    let dir = temp_dir("storage");
    let snapshot = dir.join("db.tinysql.gz");
    {
        let db = Database::open_in_memory().unwrap();
        db.execute_script("CREATE TABLE t (id INT, s TEXT); INSERT INTO t VALUES (1, 'saved')")
            .unwrap();
        db.save(&snapshot).unwrap();
        db.close().unwrap();
    }
    let restored = Database::open_snapshot(&snapshot).unwrap();
    assert_eq!(
        restored
            .query_value::<String>("SELECT s FROM t", &[])
            .unwrap()
            .as_deref(),
        Some("saved")
    );
    let read_only = OpenOptions::new().read_only(true).open(&snapshot).unwrap();
    assert!(read_only
        .execute("INSERT INTO t VALUES (2, 'x')", &[])
        .is_err());
    assert!(Database::open_snapshot(dir.join("missing")).is_err());

    for mode in [
        StorageMode::Wal,
        StorageMode::AdvancedWal,
        StorageMode::Disk,
        StorageMode::Json,
    ] {
        let path = dir.join(format!("{mode:?}"));
        {
            let db = OpenOptions::new()
                .mode(mode)
                .wal_sync_normal()
                .open(&path)
                .unwrap();
            db.execute_script("CREATE TABLE t (id INT)").unwrap();
            db.execute(
                "INSERT INTO t VALUES (?)",
                params![9_007_199_254_740_993_i64],
            )
            .unwrap();
            db.sync().unwrap();
        }
        let db = Database::open_durable(&path, mode).unwrap();
        assert_eq!(
            db.query_value::<i64>("SELECT id FROM t", &[]).unwrap(),
            Some(9_007_199_254_740_993),
            "{mode:?}"
        );
    }

    let key = [7u8; 32];
    let secret = dir.join("secret");
    {
        let db = OpenOptions::new()
            .mode(StorageMode::Json)
            .encryption_key(&key)
            .open(&secret)
            .unwrap();
        db.execute_script("CREATE TABLE s (v TEXT); INSERT INTO s VALUES ('hidden')")
            .unwrap();
    }
    let db = OpenOptions::new()
        .mode(StorageMode::Json)
        .encryption_key(&key)
        .open(&secret)
        .unwrap();
    assert_eq!(
        db.query_value::<String>("SELECT v FROM s", &[])
            .unwrap()
            .as_deref(),
        Some("hidden")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn shared_across_threads() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    db.execute("CREATE TABLE t (id INT)", &[]).unwrap();
    let workers: Vec<_> = (0..8)
        .map(|n| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..50 {
                    db.execute("INSERT INTO t VALUES (?)", params![n * 1000 + i])
                        .unwrap();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        db.query_value::<i64>("SELECT COUNT(*) FROM t", &[])
            .unwrap(),
        Some(400)
    );
    let other = Database::open_in_memory().unwrap();
    assert!(other.query("SELECT * FROM t", &[]).is_err());
}

#[test]
fn closed_database_rejects_calls() {
    let db = Database::open_in_memory().unwrap();
    db.close().unwrap();
    let db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)", &[]).unwrap();
    drop(db);
}
