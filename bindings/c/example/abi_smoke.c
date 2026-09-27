#include <stdio.h>
#include <string.h>
#include "tinysql.h"

static int check(char *response, const char *expect) {
    int ok = response && strstr(response, expect) != NULL;
    printf("%s %s\n", ok ? "ok  " : "FAIL", response ? response : "(null)");
    TinySQLDatabaseFree(response);
    return ok ? 0 : 1;
}

int main(void) {
    int failures = 0;
    if (TinySQLABIVersion() != 2) { puts("FAIL abi"); return 1; }
    failures += check(TinySQLInfo(), "\"abi\":2");
    char *opened = TinySQLDatabaseOpenWithOptions(NULL);
    unsigned long long handle = 0;
    sscanf(opened, "{\"handle\":%llu}", &handle);
    TinySQLDatabaseFree(opened);
    failures += check(TinySQLDatabaseExecuteScript(handle, "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (1, 'a;b')"), "\"statements\":2");
    failures += check(TinySQLDatabaseExecuteBatch(handle, "INSERT INTO t VALUES (?, ?)", "[[2,\"x\"],[3,null]]"), "\"rowsAffected\":2");
    failures += check(TinySQLDatabaseRun(handle, "SELECT id, name FROM t WHERE id >= ? ORDER BY id", "[1]"), "[[1,\"a;b\"],[2,\"x\"],[3,null]]");
    failures += check(TinySQLDatabaseRun(handle, "DELETE FROM t WHERE id = 3", NULL), "\"rowsAffected\":1");
    failures += check(TinySQLDatabaseQuery(handle, "SELECT nope FROM missing", NULL), "\"error\"");
    failures += check(TinySQLDatabaseClose(handle), "{}");
    failures += check(TinySQLDatabaseClose(handle), "invalid or closed");
    char *legacy = TinySQLExec("SELECT 1 AS one");
    int legacyFailed = strstr(legacy, "\"status\":\"ok\"") == NULL;
    printf("%s %s\n", legacyFailed ? "FAIL" : "ok  ", legacy);
    TinySQLFree(legacy);
    return failures + legacyFailed;
}
