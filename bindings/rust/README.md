# tinySQL for Rust

Safe Rust bindings for the embedded tinySQL engine. The crate statically links
a library built from the repository's [C ABI](../c/include/tinysql.h); its
build script compiles that library with Go, so no service, runtime download or
dynamic library is involved.

## Add the crate

Requirements: Rust 1.73+, Go (version from the repository `go.mod`) and a C
toolchain for cgo.

```toml
[dependencies]
tinysql = { path = "../tinySQL/bindings/rust" }
# or: tinysql = { git = "https://github.com/SimonWaldherr/tinySQL" }
```

`cargo build` runs `go build -buildmode=c-archive ./bindings/c`. To use a
prebuilt archive instead (cross-compilation, builds without Go), set
`TINYSQL_LIB_DIR` to a directory containing `libtinysql.a`:

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc \
  go build -buildmode=c-archive -o /opt/tinysql/libtinysql.a ./bindings/c
TINYSQL_LIB_DIR=/opt/tinysql cargo build --target aarch64-unknown-linux-gnu
```

A process can contain only one Go runtime archive; do not link a second
Go-based static library next to this crate.

## Use

```rust
use tinysql::{params, Database, Value};

fn main() -> tinysql::Result<()> {
    let db = Database::open_in_memory()?;
    db.execute_script("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, avatar BLOB)")?;

    let rows = vec![
        vec![Value::from(1), "Ada".into(), Value::Null],
        vec![Value::from(2), "Grace".into(), Value::from(&b"\x89PNG"[..])],
    ];
    db.execute_batch("INSERT INTO users VALUES (?, ?, ?)", &rows)?;

    for row in &db.query("SELECT id, name FROM users WHERE id >= ?", params![1])? {
        let id: i64 = row.get("id")?;
        let name: String = row.get(1)?;
        println!("{id} {name}");
    }
    let count: Option<i64> = db.query_value("SELECT COUNT(*) FROM users", params![])?;
    assert_eq!(count, Some(2));
    Ok(())
}
```

Run the complete example with `cargo run --example quickstart`.

### Opening databases

| Call | Result |
| --- | --- |
| `Database::open_in_memory()` | New in-memory database |
| `Database::open_snapshot(path)` | Load a snapshot written by `db.save(path)` (gzip for `.gz`) |
| `Database::open_durable(dir, StorageMode::Wal)` | Durable database; every acknowledged write persists |
| `OpenOptions::new().mode(StorageMode::Hybrid).max_memory_bytes(256 << 20).open(dir)` | Tuned storage |

`OpenOptions` also offers `read_only`, `sync_on_mutate`, `compress_files`,
checkpoint settings, `wal_sync_normal()` and `encryption_key(&[u8; 32])`
(disk, json, index and hybrid modes). See the
[storage guide](../../docs/storage-guide.md).

### Statements, values and transactions

- `execute` returns the affected row count; `query` returns `Rows` (use it for
  `... RETURNING`); `query_value` reads the first column of the first row.
- `execute_batch` prepares once and runs all parameter rows in one native call.
- `execute_script` runs several statements, including trigger bodies.
- Placeholders are `?`, `$1` or `:1`. Build parameter slices with `params!` or
  `Value::from`.
- `Value` covers `Null`, `Integer(i64)`, `Real(f64)`, `Text`, `Boolean` and
  `Blob`. `Row::get::<T>` converts to `i64` (and smaller integer types with a
  range check), `f64`, `bool`, `String`, `Vec<u8>`, `Value` and `Option<T>`
  for nullable columns; columns are addressed by index or case-insensitive name.
- `db.transaction(|tx| { ...; Ok(value) })` commits on `Ok` and rolls back on
  `Err`. `BEGIN`/`COMMIT` as SQL work too; `in_transaction()` reflects the
  engine state.

`Database` is `Send + Sync`. Calls on one database are serialized in the
engine; separate databases run in parallel. Results are materialized, so page
large results with `LIMIT` or a keyset condition. Dropping a database closes
it; `close()` reports close errors.

## Test

```sh
make test-rust            # from the repository root
# or
cargo test --manifest-path bindings/rust/Cargo.toml
```
