//! Safe wrapper over `sqlite-wasm-rs` raw FFI for WASM targets.
//!
//! Provides `Connection` and `Statement` types that mirror rusqlite's
//! ergonomics while using the sqlite-wasm-rs C-style API underneath.
//!
//! # Safety
//!
//! sqlite-wasm-rs is compiled with `SQLITE_THREADSAFE=0`. Since WASM is
//! single-threaded, this is fine. The `Connection` type is intentionally
//! `!Send + !Sync` — callers that need `StorageBackend`'s `Send + Sync`
//! bound should wrap it (see `WasmSqliteBackend`).

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::os::raw::{c_char, c_int};

use sqlite_wasm_rs as ffi;

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug)]
pub struct SqliteError {
    pub code: c_int,
    pub message: String,
}

impl std::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQLite error ({}): {}", self.code, self.message)
    }
}

impl std::error::Error for SqliteError {}

pub type Result<T> = std::result::Result<T, SqliteError>;

// ============================================================================
// StepResult
// ============================================================================

#[derive(Debug, PartialEq, Eq)]
pub enum StepResult {
    Row,
    Done,
}

// ============================================================================
// ColumnType
// ============================================================================

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
    Null,
}

// ============================================================================
// Connection
// ============================================================================

pub struct Connection {
    raw: *mut ffi::sqlite3,
    /// Prevent Send + Sync (sqlite-wasm-rs is single-threaded).
    _marker: PhantomData<*mut ()>,
}

impl Connection {
    /// Open a database at `path`. Creates it if it doesn't exist.
    pub fn open(path: &str) -> Result<Self> {
        let c_path = CString::new(path).map_err(|e| SqliteError {
            code: ffi::SQLITE_ERROR,
            message: format!("Invalid path: {e}"),
        })?;

        let mut db: *mut ffi::sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                c_path.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                std::ptr::null(),
            )
        };

        if rc != ffi::SQLITE_OK {
            let msg = if !db.is_null() {
                unsafe { errmsg(db) }
            } else {
                "Failed to open database".to_string()
            };
            // Close even on error to avoid leak
            if !db.is_null() {
                unsafe { ffi::sqlite3_close(db) };
            }
            return Err(SqliteError {
                code: rc,
                message: msg,
            });
        }

        Ok(Connection {
            raw: db,
            _marker: PhantomData,
        })
    }

    /// Execute one or more SQL statements (no result rows).
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let c_sql = CString::new(sql).map_err(|e| SqliteError {
            code: ffi::SQLITE_ERROR,
            message: format!("Invalid SQL (null byte): {e}"),
        })?;

        let rc = unsafe {
            ffi::sqlite3_exec(
                self.raw,
                c_sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        if rc != ffi::SQLITE_OK {
            return Err(SqliteError {
                code: rc,
                message: unsafe { errmsg(self.raw) },
            });
        }
        Ok(())
    }

    /// Prepare a single SQL statement.
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let c_sql = CString::new(sql).map_err(|e| SqliteError {
            code: ffi::SQLITE_ERROR,
            message: format!("Invalid SQL (null byte): {e}"),
        })?;

        let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                self.raw,
                c_sql.as_ptr(),
                -1,
                &mut stmt,
                std::ptr::null_mut(),
            )
        };

        if rc != ffi::SQLITE_OK {
            return Err(SqliteError {
                code: rc,
                message: unsafe { errmsg(self.raw) },
            });
        }

        Ok(Statement {
            raw: stmt,
            conn: self,
        })
    }

    /// Number of rows changed by the last INSERT/UPDATE/DELETE.
    pub fn changes(&self) -> i32 {
        unsafe { ffi::sqlite3_changes(self.raw) }
    }

    /// Close the connection. Consumes self.
    pub fn close(self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_close(self.raw) };
        // Prevent Drop from double-closing
        std::mem::forget(self);
        if rc != ffi::SQLITE_OK {
            return Err(SqliteError {
                code: rc,
                message: format!("Failed to close database: error code {rc}"),
            });
        }
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { ffi::sqlite3_close(self.raw) };
        }
    }
}

// ============================================================================
// Statement
// ============================================================================

pub struct Statement<'conn> {
    raw: *mut ffi::sqlite3_stmt,
    conn: &'conn Connection,
}

impl<'conn> Statement<'conn> {
    // -- Binding --

    /// Bind a text value. `idx` is 1-based.
    pub fn bind_text(&mut self, idx: c_int, val: &str) -> Result<()> {
        let c_val = CString::new(val).map_err(|e| SqliteError {
            code: ffi::SQLITE_ERROR,
            message: format!("Invalid text (null byte): {e}"),
        })?;
        let rc = unsafe {
            ffi::sqlite3_bind_text(
                self.raw,
                idx,
                c_val.as_ptr(),
                val.len() as c_int,
                ffi::SQLITE_TRANSIENT(),
            )
        };
        check_bind(rc, self.conn)
    }

    /// Bind a 64-bit integer. `idx` is 1-based.
    pub fn bind_int64(&mut self, idx: c_int, val: i64) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_bind_int64(self.raw, idx, val) };
        check_bind(rc, self.conn)
    }

    /// Bind a double. `idx` is 1-based.
    pub fn bind_double(&mut self, idx: c_int, val: f64) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_bind_double(self.raw, idx, val) };
        check_bind(rc, self.conn)
    }

    /// Bind a blob. `idx` is 1-based.
    pub fn bind_blob(&mut self, idx: c_int, val: &[u8]) -> Result<()> {
        let rc = unsafe {
            ffi::sqlite3_bind_blob(
                self.raw,
                idx,
                val.as_ptr().cast(),
                val.len() as c_int,
                ffi::SQLITE_TRANSIENT(),
            )
        };
        check_bind(rc, self.conn)
    }

    /// Bind NULL. `idx` is 1-based.
    pub fn bind_null(&mut self, idx: c_int) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_bind_null(self.raw, idx) };
        check_bind(rc, self.conn)
    }

    // -- Stepping --

    /// Advance the statement. Returns `Row` if there's a result row, `Done` if finished.
    pub fn step(&mut self) -> Result<StepResult> {
        let rc = unsafe { ffi::sqlite3_step(self.raw) };
        match rc {
            ffi::SQLITE_ROW => Ok(StepResult::Row),
            ffi::SQLITE_DONE => Ok(StepResult::Done),
            _ => Err(SqliteError {
                code: rc,
                message: unsafe { errmsg(self.conn.raw) },
            }),
        }
    }

    // -- Column accessors (0-based) --

    /// Get a text column value as a borrowed str. `idx` is 0-based.
    ///
    /// The returned str is borrowed from SQLite's internal buffer and is only
    /// valid until the next call to `step()`, `reset()`, or `column_*()` on
    /// this statement. Prefer `column_string()` which clones the value.
    fn column_text(&self, idx: c_int) -> &str {
        unsafe {
            let ptr = ffi::sqlite3_column_text(self.raw, idx);
            if ptr.is_null() {
                return "";
            }
            let c_str = CStr::from_ptr(ptr as *const c_char);
            c_str.to_str().unwrap_or("")
        }
    }

    /// Get a text column value as owned String. `idx` is 0-based.
    pub fn column_string(&self, idx: c_int) -> String {
        self.column_text(idx).to_string()
    }

    /// Get a 64-bit integer column value. `idx` is 0-based.
    pub fn column_int64(&self, idx: c_int) -> i64 {
        unsafe { ffi::sqlite3_column_int64(self.raw, idx) }
    }

    /// Get a double column value. `idx` is 0-based.
    pub fn column_double(&self, idx: c_int) -> f64 {
        unsafe { ffi::sqlite3_column_double(self.raw, idx) }
    }

    /// Get a blob column value. `idx` is 0-based.
    pub fn column_blob(&self, idx: c_int) -> Vec<u8> {
        unsafe {
            let ptr = ffi::sqlite3_column_blob(self.raw, idx);
            let len = ffi::sqlite3_column_bytes(self.raw, idx);
            if ptr.is_null() || len <= 0 {
                return Vec::new();
            }
            std::slice::from_raw_parts(ptr as *const u8, len as usize).to_vec()
        }
    }

    /// Get the column type. `idx` is 0-based.
    pub fn column_type(&self, idx: c_int) -> ColumnType {
        let t = unsafe { ffi::sqlite3_column_type(self.raw, idx) };
        match t {
            ffi::SQLITE_INTEGER => ColumnType::Integer,
            ffi::SQLITE_FLOAT => ColumnType::Float,
            ffi::SQLITE_TEXT => ColumnType::Text,
            ffi::SQLITE_BLOB => ColumnType::Blob,
            _ => ColumnType::Null,
        }
    }

    /// Reset the statement so it can be stepped again.
    pub fn reset(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_reset(self.raw) };
        if rc != ffi::SQLITE_OK {
            return Err(SqliteError {
                code: rc,
                message: unsafe { errmsg(self.conn.raw) },
            });
        }
        Ok(())
    }

    /// Clear all bindings.
    pub fn clear_bindings(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_clear_bindings(self.raw) };
        if rc != ffi::SQLITE_OK {
            return Err(SqliteError {
                code: rc,
                message: unsafe { errmsg(self.conn.raw) },
            });
        }
        Ok(())
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { ffi::sqlite3_finalize(self.raw) };
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract the error message from a database handle.
unsafe fn errmsg(db: *mut ffi::sqlite3) -> String {
    let ptr = ffi::sqlite3_errmsg(db);
    if ptr.is_null() {
        return "Unknown error".to_string();
    }
    CStr::from_ptr(ptr).to_string_lossy().into_owned()
}

fn check_bind(rc: c_int, conn: &Connection) -> Result<()> {
    if rc != ffi::SQLITE_OK {
        return Err(SqliteError {
            code: rc,
            message: unsafe { errmsg(conn.raw) },
        });
    }
    Ok(())
}
