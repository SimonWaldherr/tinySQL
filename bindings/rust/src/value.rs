//! SQL values exchanged with the engine.

use crate::error::{Error, Result};

/// A SQL value used as a parameter or read from a result row.
///
/// Integers keep all 64 bits, `Real` keeps its floating-point type even for
/// integral values, and `Blob` preserves every byte including zeros.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
}

impl Value {
    /// The SQL-level type name, used in conversion errors.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Integer(_) => "INTEGER",
            Value::Real(_) => "REAL",
            Value::Text(_) => "TEXT",
            Value::Boolean(_) => "BOOLEAN",
            Value::Blob(_) => "BLOB",
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub(crate) fn to_json(&self) -> Result<serde_json::Value> {
        use serde_json::{json, Value as J};
        Ok(match self {
            Value::Null => J::Null,
            Value::Integer(i) => J::from(*i),
            Value::Real(f) => {
                if !f.is_finite() {
                    return Err(Error::InvalidInput("SQL REAL values must be finite".into()));
                }
                json!({ "real": f })
            }
            Value::Text(s) => J::String(s.clone()),
            Value::Boolean(b) => J::Bool(*b),
            Value::Blob(bytes) => json!({ "blob": crate::base64::encode(bytes) }),
        })
    }

    pub(crate) fn from_json(value: serde_json::Value) -> Result<Value> {
        use serde_json::Value as J;
        Ok(match value {
            J::Null => Value::Null,
            J::Bool(b) => Value::Boolean(b),
            J::String(s) => Value::Text(s),
            J::Number(n) => match n.as_i64() {
                Some(i) => Value::Integer(i),
                None => Value::Real(n.as_f64().ok_or_else(|| invalid(&n))?),
            },
            J::Object(mut object) if object.len() == 1 => {
                if let Some(real) = object.remove("real") {
                    Value::Real(real.as_f64().ok_or_else(|| invalid(&real))?)
                } else if let Some(J::String(blob)) = object.remove("blob") {
                    Value::Blob(crate::base64::decode(&blob)?)
                } else {
                    return Err(Error::Protocol("unknown tagged value".into()));
                }
            }
            other => return Err(invalid(&other)),
        })
    }
}

fn invalid(value: &dyn std::fmt::Display) -> Error {
    Error::Protocol(format!("unexpected value {value}"))
}

macro_rules! from_integer {
    ($($t:ty),*) => {$(
        impl From<$t> for Value {
            fn from(value: $t) -> Self { Value::Integer(i64::from(value)) }
        }
    )*};
}
from_integer!(i8, i16, i32, i64, u8, u16, u32);

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Real(f64::from(value))
    }
}
impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Real(value)
    }
}
impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.to_owned())
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value)
    }
}
impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Value::Text(value.clone())
    }
}
impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Value::Blob(value.to_vec())
    }
}
impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Blob(value)
    }
}
impl<const N: usize> From<&[u8; N]> for Value {
    fn from(value: &[u8; N]) -> Self {
        Value::Blob(value.to_vec())
    }
}
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        value.map_or(Value::Null, Into::into)
    }
}
impl From<&Value> for Value {
    fn from(value: &Value) -> Self {
        value.clone()
    }
}

/// Conversion from a result [`Value`] into a Rust type, used by
/// [`Row::get`](crate::Row::get).
pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Result<Self>;
}

fn mismatch<T>(expected: &'static str, value: &Value) -> Result<T> {
    Err(Error::Type {
        expected,
        found: value.type_name(),
    })
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(value.clone())
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            Value::Boolean(b) => Ok(i64::from(*b)),
            _ => mismatch("INTEGER", value),
        }
    }
}

macro_rules! from_value_integer {
    ($($t:ty),*) => {$(
        impl FromValue for $t {
            fn from_value(value: &Value) -> Result<Self> {
                let wide = i64::from_value(value)?;
                <$t>::try_from(wide).map_err(|_| Error::Type { expected: stringify!($t), found: "out-of-range INTEGER" })
            }
        }
    )*};
}
from_value_integer!(i8, i16, i32, u8, u16, u32, u64, usize, isize);

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Real(f) => Ok(*f),
            // Engine arithmetic may produce integral values in either form.
            Value::Integer(i) => Ok(*i as f64),
            _ => mismatch("REAL", value),
        }
    }
}

impl FromValue for f32 {
    fn from_value(value: &Value) -> Result<Self> {
        f64::from_value(value).map(|f| f as f32)
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            _ => mismatch("BOOLEAN", value),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.clone()),
            _ => mismatch("TEXT", value),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Blob(b) => Ok(b.clone()),
            Value::Text(s) => Ok(s.clone().into_bytes()),
            _ => mismatch("BLOB", value),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            other => T::from_value(other).map(Some),
        }
    }
}
