//! Safe Rust bindings for [tinySQL](https://github.com/SimonWaldherr/tinySQL),
//! an embeddable SQL engine written in Go.
//!
//! The crate links a static library built from the repository's C ABI
//! (`bindings/c`); the build script compiles it with Go automatically.
//!
//! ```
//! use tinysql::{params, Database};
//!
//! # fn main() -> tinysql::Result<()> {
//! let db = Database::open_in_memory()?;
//! db.execute_script("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")?;
//! db.execute("INSERT INTO users VALUES (?, ?)", params![1, "Ada"])?;
//!
//! let rows = db.query("SELECT id, name FROM users WHERE id = ?", params![1])?;
//! let name: String = rows[0].get("name")?;
//! assert_eq!(name, "Ada");
//! # Ok(())
//! # }
//! ```
//!
//! A [`Database`] is `Send + Sync`: calls on one database are serialized by
//! the engine, while separate databases run in parallel.

mod base64;
mod error;
mod value;

use std::ffi::{c_char, CStr, CString};
use std::ops::Index;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub use error::{Error, Result};
pub use value::{FromValue, Value};

/// ABI version this crate was written against.
pub const REQUIRED_ABI: i32 = 2;

mod ffi {
    use std::ffi::c_char;

    extern "C" {
        pub fn TinySQLABIVersion() -> i32;
        pub fn TinySQLInfo() -> *mut c_char;
        pub fn TinySQLDatabaseOpenWithOptions(options: *const c_char) -> *mut c_char;
        pub fn TinySQLDatabaseExecute(
            handle: u64,
            sql: *const c_char,
            parameters: *const c_char,
        ) -> *mut c_char;
        pub fn TinySQLDatabaseQuery(
            handle: u64,
            sql: *const c_char,
            parameters: *const c_char,
        ) -> *mut c_char;
        pub fn TinySQLDatabaseExecuteBatch(
            handle: u64,
            sql: *const c_char,
            sets: *const c_char,
        ) -> *mut c_char;
        pub fn TinySQLDatabaseExecuteScript(handle: u64, script: *const c_char) -> *mut c_char;
        pub fn TinySQLDatabaseSave(handle: u64, path: *const c_char) -> *mut c_char;
        pub fn TinySQLDatabaseSync(handle: u64) -> *mut c_char;
        pub fn TinySQLDatabaseClose(handle: u64) -> *mut c_char;
        pub fn TinySQLDatabaseFree(buffer: *mut c_char);
    }
}

/// Builds a `&[Value]` parameter list from expressions convertible into
/// [`Value`]: `params![1, "text", 2.5, None::<i64>, &bytes[..]]`.
#[macro_export]
macro_rules! params {
    () => { &[] as &[$crate::Value] };
    ($($value:expr),+ $(,)?) => { &[$($crate::Value::from($value)),+] as &[$crate::Value] };
}

/// Returns the ABI version implemented by the linked library.
pub fn abi_version() -> i32 {
    // SAFETY: no arguments, no allocation.
    unsafe { ffi::TinySQLABIVersion() }
}

/// Returns the version string of the linked tinySQL engine.
pub fn version() -> Result<String> {
    // SAFETY: TinySQLInfo returns an owned buffer consumed by `consume`.
    let response = consume(unsafe { ffi::TinySQLInfo() })?;
    response
        .get("version")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::Protocol("missing version".into()))
}

fn cstring(text: &str, what: &str) -> Result<CString> {
    CString::new(text).map_err(|_| Error::InvalidInput(format!("{what} must not contain NUL")))
}

/// Takes ownership of a response buffer, frees it and decodes the JSON object.
fn consume(pointer: *mut c_char) -> Result<serde_json::Map<String, serde_json::Value>> {
    if pointer.is_null() {
        return Err(Error::Protocol("native call returned NULL".into()));
    }
    // SAFETY: the pointer is a NUL-terminated buffer owned by us until it is
    // released below; it is not used afterwards.
    let parsed = unsafe {
        let parsed =
            serde_json::from_slice::<serde_json::Value>(CStr::from_ptr(pointer).to_bytes());
        ffi::TinySQLDatabaseFree(pointer);
        parsed
    };
    match parsed.map_err(|e| Error::Protocol(e.to_string()))? {
        serde_json::Value::Object(object) => Ok(object),
        _ => Err(Error::Protocol("response is not a JSON object".into())),
    }
}

fn encode_params(params: &[Value]) -> Result<CString> {
    if params.is_empty() {
        return Ok(CString::default());
    }
    let values = params
        .iter()
        .map(Value::to_json)
        .collect::<Result<Vec<_>>>()?;
    cstring(&serde_json::Value::Array(values).to_string(), "parameters")
}

/// Storage backends accepted by [`OpenOptions::mode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageMode {
    /// A new in-memory database (the default without a path).
    Memory,
    /// Load a snapshot written by [`Database::save`]; changes stay in memory
    /// until saved again (the default with a path).
    Snapshot,
    /// In-memory tables with a write-ahead log.
    Wal,
    /// Row-level write-ahead log with checkpoints.
    AdvancedWal,
    /// One GOB file per table.
    Disk,
    /// One human-readable JSON file per table.
    Json,
    /// Disk tables loaded on demand with a bounded cache.
    Index,
    /// Disk tables with a bounded LRU cache.
    Hybrid,
    /// Paged on-disk equality indexes for large read-mostly artifacts.
    PagedIndex,
}

impl StorageMode {
    fn as_str(self) -> &'static str {
        match self {
            StorageMode::Memory => "memory",
            StorageMode::Snapshot => "snapshot",
            StorageMode::Wal => "wal",
            StorageMode::AdvancedWal => "advanced_wal",
            StorageMode::Disk => "disk",
            StorageMode::Json => "json",
            StorageMode::Index => "index",
            StorageMode::Hybrid => "hybrid",
            StorageMode::PagedIndex => "paged_index",
        }
    }
}

/// Builder for opening a database with a storage mode and tuning options.
///
/// ```no_run
/// use tinysql::{OpenOptions, StorageMode};
/// let db = OpenOptions::new().mode(StorageMode::Wal).wal_sync_normal().open("./data")?;
/// # Ok::<(), tinysql::Error>(())
/// ```
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    options: serde_json::Map<String, serde_json::Value>,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    fn set(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.options.insert(key.to_owned(), value.into());
        self
    }

    pub fn mode(self, mode: StorageMode) -> Self {
        self.set("mode", mode.as_str())
    }

    /// Reject every write.
    pub fn read_only(self, read_only: bool) -> Self {
        self.set("readOnly", read_only)
    }

    /// Cache bound for the index and hybrid modes.
    pub fn max_memory_bytes(self, bytes: i64) -> Self {
        self.set("maxMemoryBytes", bytes)
    }

    pub fn sync_on_mutate(self, enabled: bool) -> Self {
        self.set("syncOnMutate", enabled)
    }

    pub fn compress_files(self, enabled: bool) -> Self {
        self.set("compressFiles", enabled)
    }

    pub fn checkpoint_every(self, statements: u64) -> Self {
        self.set("checkpointEvery", statements)
    }

    pub fn checkpoint_interval(self, interval: std::time::Duration) -> Self {
        self.set(
            "checkpointIntervalMs",
            u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
        )
    }

    pub fn checkpoint_max_bytes(self, bytes: i64) -> Self {
        self.set("checkpointMaxBytes", bytes)
    }

    /// Use a regular fsync for WAL commits instead of the strongest flush.
    pub fn wal_sync_normal(self) -> Self {
        self.set("walSync", "normal")
    }

    /// Encrypt table files of the disk, json, index and hybrid modes with
    /// AES-256-GCM.
    pub fn encryption_key(self, key: &[u8; 32]) -> Self {
        self.set("encryptionKey", base64::encode(key))
    }

    /// Open the database at `path`: a snapshot file, or the storage
    /// directory of a durable mode.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| Error::InvalidInput("path must be valid UTF-8".into()))?;
        let mut options = self.options.clone();
        options.insert("path".into(), path.into());
        Database::open_raw(&serde_json::Value::Object(options).to_string())
    }

    /// Open a database without a path (in-memory).
    pub fn open_in_memory(&self) -> Result<Database> {
        Database::open_raw(&serde_json::Value::Object(self.options.clone()).to_string())
    }
}

/// Outcome of [`Database::execute_script`] and [`Database::execute_batch`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    /// Statements (script) or parameter rows (batch) executed.
    pub statements: u64,
    /// Rows changed by INSERT, UPDATE and DELETE.
    pub rows_affected: u64,
}

/// An independent tinySQL database.
///
/// Every database pins one engine connection, so `BEGIN`, `COMMIT` and
/// `ROLLBACK` span calls. Dropping the database closes it; call
/// [`Database::close`] to observe close errors.
#[derive(Debug)]
pub struct Database {
    handle: AtomicU64,
    in_transaction: AtomicBool,
}

impl Database {
    /// Creates a new in-memory database.
    pub fn open_in_memory() -> Result<Self> {
        Self::open_raw("")
    }

    /// Loads a snapshot written by [`Database::save`] (gzip if the path ends
    /// in `.gz`). The file is never modified implicitly.
    pub fn open_snapshot(path: impl AsRef<Path>) -> Result<Self> {
        OpenOptions::new().mode(StorageMode::Snapshot).open(path)
    }

    /// Opens a durable database in `directory` with the given storage mode.
    pub fn open_durable(directory: impl AsRef<Path>, mode: StorageMode) -> Result<Self> {
        OpenOptions::new().mode(mode).open(directory)
    }

    fn open_raw(options: &str) -> Result<Self> {
        let abi = abi_version();
        if abi < REQUIRED_ABI {
            return Err(Error::Protocol(format!(
                "linked library implements ABI {abi}, need {REQUIRED_ABI}"
            )));
        }
        let options = cstring(options, "options")?;
        // SAFETY: options outlives the call; the response is consumed.
        let response = consume(unsafe { ffi::TinySQLDatabaseOpenWithOptions(options.as_ptr()) })?;
        check(&response)?;
        let handle = response
            .get("handle")
            .and_then(serde_json::Value::as_u64)
            .filter(|&h| h != 0)
            .ok_or_else(|| Error::Protocol("missing handle".into()))?;
        Ok(Database {
            handle: AtomicU64::new(handle),
            in_transaction: AtomicBool::new(false),
        })
    }

    fn handle(&self) -> Result<u64> {
        match self.handle.load(Ordering::Acquire) {
            0 => Err(Error::Database("database is closed".into())),
            handle => Ok(handle),
        }
    }

    /// Runs a statement-returning native call and records the transaction state.
    fn statement(
        &self,
        call: impl FnOnce(u64) -> *mut c_char,
    ) -> Result<serde_json::Map<String, serde_json::Value>> {
        let response = consume(call(self.handle()?))?;
        let in_transaction = response
            .get("inTransaction")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        self.in_transaction.store(in_transaction, Ordering::Release);
        check(&response)?;
        Ok(response)
    }

    /// Executes one statement and returns the number of affected rows.
    /// Use [`Database::query`] for statements that return rows, including
    /// `INSERT ... RETURNING`.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let sql = cstring(sql, "SQL")?;
        let params = encode_params(params)?;
        // SAFETY: both strings outlive the call.
        let response = self.statement(|h| unsafe {
            ffi::TinySQLDatabaseExecute(h, sql.as_ptr(), params.as_ptr())
        })?;
        Ok(count(&response, "rowsAffected"))
    }

    /// Runs a statement and returns its complete result set.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Rows> {
        let sql = cstring(sql, "SQL")?;
        let params = encode_params(params)?;
        // SAFETY: both strings outlive the call.
        let mut response = self.statement(|h| unsafe {
            ffi::TinySQLDatabaseQuery(h, sql.as_ptr(), params.as_ptr())
        })?;
        Rows::from_response(&mut response)
    }

    /// Runs a query expected to return one row and converts its first column.
    /// Returns `Ok(None)` when the query returns no rows.
    pub fn query_value<T: FromValue>(&self, sql: &str, params: &[Value]) -> Result<Option<T>> {
        let rows = self.query(sql, params)?;
        rows.rows.first().map(|row| row.get(0)).transpose()
    }

    /// Prepares `sql` once and executes it for every parameter row in a
    /// single native call. Wrap it in a transaction to make it atomic.
    pub fn execute_batch<I, R>(&self, sql: &str, rows: I) -> Result<Summary>
    where
        I: IntoIterator<Item = R>,
        R: AsRef<[Value]>,
    {
        let mut sets = Vec::new();
        for row in rows {
            let values = row
                .as_ref()
                .iter()
                .map(Value::to_json)
                .collect::<Result<Vec<_>>>()?;
            sets.push(serde_json::Value::Array(values));
        }
        if sets.is_empty() {
            return Ok(Summary::default());
        }
        let sql = cstring(sql, "SQL")?;
        let sets = cstring(&serde_json::Value::Array(sets).to_string(), "parameters")?;
        // SAFETY: both strings outlive the call.
        let response = self.statement(|h| unsafe {
            ffi::TinySQLDatabaseExecuteBatch(h, sql.as_ptr(), sets.as_ptr())
        })?;
        Ok(summary(&response))
    }

    /// Executes a multi-statement script. Semicolons in literals, comments
    /// and `CREATE TRIGGER ... BEGIN ... END` bodies do not split. Stops at
    /// the first failing statement; earlier statements stay applied.
    pub fn execute_script(&self, script: &str) -> Result<Summary> {
        let script = cstring(script, "script")?;
        // SAFETY: the string outlives the call.
        let response =
            self.statement(|h| unsafe { ffi::TinySQLDatabaseExecuteScript(h, script.as_ptr()) })?;
        Ok(summary(&response))
    }

    /// Runs `body` inside `BEGIN ... COMMIT`, rolling back when it fails.
    pub fn transaction<T>(&self, body: impl FnOnce(&Database) -> Result<T>) -> Result<T> {
        if self.in_transaction() {
            return Err(Error::InvalidInput(
                "a transaction is already active".into(),
            ));
        }
        self.execute("BEGIN", &[])?;
        match body(self) {
            Ok(value) => {
                self.execute("COMMIT", &[])?;
                Ok(value)
            }
            Err(error) => {
                if self.in_transaction() {
                    let _ = self.execute("ROLLBACK", &[]);
                }
                Err(error)
            }
        }
    }

    /// Whether the database is inside `BEGIN ... COMMIT/ROLLBACK`, including
    /// transactions started with SQL text.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Acquire)
    }

    /// Writes committed state to a snapshot file (gzip if it ends in `.gz`).
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| Error::InvalidInput("path must be valid UTF-8".into()))?;
        let path = cstring(path, "path")?;
        let handle = self.handle()?;
        // SAFETY: the string outlives the call; the response is consumed.
        check(&consume(unsafe {
            ffi::TinySQLDatabaseSave(handle, path.as_ptr())
        })?)
    }

    /// Flushes durable storage modes; a no-op for memory and WAL modes.
    pub fn sync(&self) -> Result<()> {
        let handle = self.handle()?;
        // SAFETY: the response is consumed.
        check(&consume(unsafe { ffi::TinySQLDatabaseSync(handle) })?)
    }

    /// Closes the database, discarding an unfinished transaction.
    pub fn close(self) -> Result<()> {
        self.close_handle()
    }

    fn close_handle(&self) -> Result<()> {
        let handle = self.handle.swap(0, Ordering::AcqRel);
        if handle == 0 {
            return Ok(());
        }
        // SAFETY: the handle is closed exactly once; the response is consumed.
        check(&consume(unsafe { ffi::TinySQLDatabaseClose(handle) })?)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.close_handle();
    }
}

fn check(response: &serde_json::Map<String, serde_json::Value>) -> Result<()> {
    match response.get("error").and_then(serde_json::Value::as_str) {
        Some(message) if !message.is_empty() => Err(Error::Database(message.to_owned())),
        _ => Ok(()),
    }
}

fn count(response: &serde_json::Map<String, serde_json::Value>, key: &str) -> u64 {
    response
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

fn summary(response: &serde_json::Map<String, serde_json::Value>) -> Summary {
    Summary {
        statements: count(response, "statements"),
        rows_affected: count(response, "rowsAffected"),
    }
}

/// A materialized result set. Index it or iterate over its [`Row`]s.
#[derive(Clone, Debug, PartialEq)]
pub struct Rows {
    columns: Arc<[String]>,
    rows: Vec<Row>,
}

impl Rows {
    fn from_response(response: &mut serde_json::Map<String, serde_json::Value>) -> Result<Self> {
        let columns: Arc<[String]> = match response.remove("columns") {
            Some(serde_json::Value::Array(names)) => names
                .into_iter()
                .map(|name| match name {
                    serde_json::Value::String(name) => Ok(name),
                    _ => Err(Error::Protocol("column name is not a string".into())),
                })
                .collect::<Result<Vec<_>>>()?
                .into(),
            None => Arc::from(Vec::new()),
            Some(_) => return Err(Error::Protocol("columns is not an array".into())),
        };
        let rows = match response.remove("rows") {
            Some(serde_json::Value::Array(rows)) => rows
                .into_iter()
                .map(|row| match row {
                    serde_json::Value::Array(values) => Ok(Row {
                        columns: Arc::clone(&columns),
                        values: values
                            .into_iter()
                            .map(Value::from_json)
                            .collect::<Result<_>>()?,
                    }),
                    _ => Err(Error::Protocol("row is not an array".into())),
                })
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
            Some(_) => return Err(Error::Protocol("rows is not an array".into())),
        };
        Ok(Rows { columns, rows })
    }

    /// Column names in result order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Row> {
        self.rows.iter()
    }
}

impl Index<usize> for Rows {
    type Output = Row;
    fn index(&self, index: usize) -> &Row {
        &self.rows[index]
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

/// One result row with positional and case-insensitive named access.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    columns: Arc<[String]>,
    values: Vec<Value>,
}

/// A column reference: a zero-based index or a column name.
pub trait ColumnIndex {
    fn position(&self, columns: &[String]) -> Result<usize>;
}

impl ColumnIndex for usize {
    fn position(&self, columns: &[String]) -> Result<usize> {
        if *self < columns.len() {
            Ok(*self)
        } else {
            Err(Error::Column(self.to_string()))
        }
    }
}

impl ColumnIndex for &str {
    fn position(&self, columns: &[String]) -> Result<usize> {
        columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case(self))
            .ok_or_else(|| Error::Column((*self).to_owned()))
    }
}

impl Row {
    /// Converts the value of a column (index or name) into `T`.
    pub fn get<T: FromValue>(&self, column: impl ColumnIndex) -> Result<T> {
        T::from_value(&self.values[column.position(&self.columns)?])
    }

    /// The raw value of a column.
    pub fn value(&self, column: impl ColumnIndex) -> Result<&Value> {
        Ok(&self.values[column.position(&self.columns)?])
    }

    /// All values in column order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
