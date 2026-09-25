#ifndef CTINYSQL_H
#define CTINYSQL_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

/* ABI v1. Inputs are borrowed, NUL-terminated UTF-8 strings. Every returned
 * string is owned by the caller and MUST be released with TinySQLDatabaseFree.
 * Responses are JSON objects; a nonempty "error" means the operation failed.
 * Handles are opaque IDs, never pointers. Calls on one handle are serialized.
 * NULL/empty path creates an in-memory DB; otherwise load an existing snapshot.
 * Open returns {"handle": uint64}. Close invalidates that handle, even on error.
 * Parameters: a JSON array of null, bool, signed int64, finite double, string,
 * or {"blob":"base64"}. Use {"real":number} to preserve the double type for
 * integral values. NULL/empty parameters means no parameters.
 * Execute returns {"rowsAffected": int64} (zero may be omitted).
 * Query returns ordered "columns" and positional "rows" arrays (empty arrays
 * may be omitted). BLOB and double results use the tagged objects above.
 * Save writes committed state to a snapshot; it does not commit a transaction.
 * The buffers themselves must not be freed concurrently with a reader.
 */
char *TinySQLDatabaseOpen(const char *path);
char *TinySQLDatabaseExecute(uint64_t handle, const char *sql, const char *parameters);
char *TinySQLDatabaseQuery(uint64_t handle, const char *sql, const char *parameters);
char *TinySQLDatabaseSave(uint64_t handle, const char *path);
char *TinySQLDatabaseClose(uint64_t handle);
void TinySQLDatabaseFree(char *buffer);

#ifdef __cplusplus
}
#endif
#endif
