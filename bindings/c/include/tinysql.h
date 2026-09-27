#ifndef TINYSQL_H
#define TINYSQL_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

/*
 * tinySQL C ABI, version 2. Build the library from the repository root:
 *
 *   go build -buildmode=c-archive -o libtinysql.a  ./bindings/c   (static)
 *   go build -buildmode=c-shared  -o libtinysql.so ./bindings/c   (shared)
 *
 * The Swift package, the Python package and the Rust crate all wrap exactly
 * these functions; see docs/language-bindings.md.
 *
 * Memory: inputs are borrowed, NUL-terminated UTF-8 strings; NULL is treated
 * as an empty string. Every returned char* is owned by the caller and MUST be
 * released with TinySQLDatabaseFree. Do not free a buffer while another
 * thread still reads it.
 *
 * Responses: every returned string is one JSON object. A nonempty "error"
 * member means the operation failed; apart from "inTransaction" no other
 * member is then present. Zero or empty members may be omitted.
 *
 * Transaction state (version 2): Execute, Query, Run, ExecuteBatch and
 * ExecuteScript responses, including their errors, contain
 * "inTransaction": true while the handle is inside BEGIN ... COMMIT/ROLLBACK.
 *
 * Handles: opaque nonzero IDs, never pointers, and never reused. Calls on one
 * handle are serialized; different handles run in parallel. Each handle pins
 * one SQL connection, so BEGIN/COMMIT/ROLLBACK span calls on that handle.
 *
 * Parameters: a JSON array of null, bool, signed int64, finite double,
 * string, {"real":number} (keeps the double type for integral values) or
 * {"blob":"base64"}. NULL/empty means no parameters. Bind with ?, $1 or :1.
 *
 * Result values: "columns" is an ordered array of names and "rows" an array
 * of positional arrays. Integers are JSON integers, doubles {"real":n},
 * BLOBs {"blob":"base64"}, booleans true/false, text and timestamps
 * (RFC 3339) JSON strings, NULL null.
 */

/* Version 1 ------------------------------------------------------------- */

/* NULL/empty path: new in-memory database. Otherwise load an existing
 * snapshot written by TinySQLDatabaseSave (".gz" suffix: gzip). Returns
 * {"handle": uint64}. Snapshots are never modified implicitly. */
char *TinySQLDatabaseOpen(const char *path);

/* Runs one statement without returning rows: {"rowsAffected": int64}. */
char *TinySQLDatabaseExecute(uint64_t handle, const char *sql, const char *parameters);

/* Runs one statement and returns {"columns": [...], "rows": [[...], ...]}. */
char *TinySQLDatabaseQuery(uint64_t handle, const char *sql, const char *parameters);

/* Writes committed state to a snapshot file; it does not commit a transaction. */
char *TinySQLDatabaseSave(uint64_t handle, const char *path);

/* Invalidates the handle, even on error, discarding an unfinished transaction
 * and flushing durable storage modes. */
char *TinySQLDatabaseClose(uint64_t handle);

void TinySQLDatabaseFree(char *buffer);

/* Version 2 ------------------------------------------------------------- */

/* Returns the ABI version implemented by the library (2). No allocation. */
int32_t TinySQLABIVersion(void);

/* Returns {"version": "engine version", "abi": 2}. */
char *TinySQLInfo(void);

/* Opens a database described by a JSON object (NULL/empty: in-memory):
 *   "mode":  "memory" | "snapshot" | "wal" | "advanced_wal" | "disk" | "json"
 *            | "index" | "hybrid" | "paged_index"  (default: "memory", or
 *            "snapshot" when a path is given)
 *   "path":  snapshot file, or storage directory for durable modes
 *   "readOnly": bool, "maxMemoryBytes": int, "syncOnMutate": bool,
 *   "compressFiles": bool, "checkpointEvery": int,
 *   "checkpointIntervalMs": int, "checkpointMaxBytes": int,
 *   "walSync": "full" | "normal", "encryptionKey": base64 of 32 bytes
 * Unknown members are rejected. Durable modes persist every acknowledged
 * write. Returns {"handle": uint64}. */
char *TinySQLDatabaseOpenWithOptions(const char *options);

/* Runs one statement and picks the result shape: statements that produce
 * rows (SELECT, WITH, VALUES, PRAGMA, EXPLAIN, CALL, ... RETURNING) return
 * "columns"/"rows"; all others return "rowsAffected". A response without
 * "columns" has no result set. */
char *TinySQLDatabaseRun(uint64_t handle, const char *sql, const char *parameters);

/* Prepares sql once and executes it for every parameter array in
 * parameterSets (a JSON array of parameter arrays). Stops at the first
 * failing row ("row N: ..."). Returns {"rowsAffected": n, "statements": rows}.
 * Wrap the call in BEGIN/COMMIT to make the batch atomic. */
char *TinySQLDatabaseExecuteBatch(uint64_t handle, const char *sql, const char *parameterSets);

/* Executes a multi-statement script. Semicolons in literals, identifiers,
 * comments and CREATE TRIGGER bodies do not split. Stops at the first error
 * ("statement N: ..."); earlier statements stay applied. Returns
 * {"rowsAffected": n, "statements": count}. */
char *TinySQLDatabaseExecuteScript(uint64_t handle, const char *script);

/* Flushes dirty tables of durable storage modes; a no-op otherwise. */
char *TinySQLDatabaseSync(uint64_t handle);

/* Legacy single-database ABI ------------------------------------------- */
/* Kept for callers of the former bindings/python library. Results use
 * {"status":"ok"|"error", ...}; free buffers with TinySQLFree. */
char *TinySQLVersion(void);
char *TinySQLExec(const char *sql);
char *TinySQLSave(const char *path);
char *TinySQLLoad(const char *path);
void TinySQLReset(void);
void TinySQLFree(char *buffer);

#ifdef __cplusplus
}
#endif
#endif
