//! Error type of the crate.

use std::fmt;

/// Errors returned by tinySQL operations.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// The engine rejected the operation (syntax error, missing table,
    /// constraint violation, I/O error, ...).
    Database(String),
    /// A caller-supplied value cannot be passed to the engine, such as SQL
    /// text containing NUL or a non-finite REAL.
    InvalidInput(String),
    /// A result value has a different SQL type than requested.
    Type {
        expected: &'static str,
        found: &'static str,
    },
    /// A column name or index does not exist in the row.
    Column(String),
    /// The native library returned a response this crate cannot interpret.
    Protocol(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Database(message) => write!(f, "tinySQL: {message}"),
            Error::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Error::Type { expected, found } => write!(f, "expected {expected}, found {found}"),
            Error::Column(column) => write!(f, "no such column: {column}"),
            Error::Protocol(message) => write!(f, "tinySQL protocol error: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// Result alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;
