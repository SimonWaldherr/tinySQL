// Integration tests for the audit-log hook in Execute (audit.go, and the
// recordAudit call in exec.go). Storage-level hash-chain correctness is
// covered by internal/storage/audit_test.go; these tests confirm Execute
// actually calls it, with the right text/user/success/error per statement.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAuditLogRecordsSuccessAndFailure(t *testing.T) {
	db := storage.NewDB()
	auditLog := storage.NewAuditLog()
	db.AttachAuditLog(auditLog)

	ctx := WithAuditText(context.Background(), `CREATE TABLE t (id INT)`)
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	badCtx := WithAuditText(context.Background(), `SELECT * FROM nonexistent`)
	Execute(badCtx, db, "default", mustParse(`SELECT * FROM nonexistent`)) // expected to fail; error is what we're checking for in the log

	entries := auditLog.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 audit entries, got %d", len(entries))
	}
	if entries[0].Statement != `CREATE TABLE t (id INT)` || !entries[0].Success {
		t.Errorf("unexpected first entry: %+v", entries[0])
	}
	if entries[1].Statement != `SELECT * FROM nonexistent` || entries[1].Success {
		t.Errorf("unexpected second entry (expected a failure): %+v", entries[1])
	}
	if entries[1].Error == "" {
		t.Error("expected the failed statement's Error field to be populated")
	}
	if err := auditLog.Verify(); err != nil {
		t.Errorf("expected the log to verify: %v", err)
	}
}

func TestAuditLogRecordsUser(t *testing.T) {
	db := storage.NewDB()
	auditLog := storage.NewAuditLog()
	db.AttachAuditLog(auditLog)

	// Bootstrap RBAC so there's a real acting user to record.
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER alice WITH PASSWORD 'pw' ROLE admin_role`)

	ctx := WithAuditText(WithUser(context.Background(), "alice"), `CREATE TABLE t (id INT)`)
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	entries := auditLog.Entries()
	last := entries[len(entries)-1]
	if last.User != "alice" {
		t.Errorf("expected the audit entry to record user 'alice', got %q", last.User)
	}
}

func TestAuditLogRecordsDeniedAccess(t *testing.T) {
	db := storage.NewDB()
	auditLog := storage.NewAuditLog()
	db.AttachAuditLog(auditLog)

	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'pw' ROLE admin_role`)

	// No user in context at all — should be denied and still audited.
	ctx := WithAuditText(context.Background(), `DROP TABLE t`)
	_, err := Execute(ctx, db, "default", mustParse(`DROP TABLE t`))
	if err == nil {
		t.Fatal("expected access denied with no user in context")
	}

	entries := auditLog.Entries()
	last := entries[len(entries)-1]
	if last.Success {
		t.Error("expected the denied attempt to be recorded as a failure")
	}
	if last.Statement != `DROP TABLE t` {
		t.Errorf("expected the denied attempt's statement text to be recorded, got %q", last.Statement)
	}
}

func TestAuditLogFallsBackToStatementTypeWithoutText(t *testing.T) {
	db := storage.NewDB()
	auditLog := storage.NewAuditLog()
	db.AttachAuditLog(auditLog)

	// No WithAuditText call — the fallback path.
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	entries := auditLog.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Statement == "" {
		t.Error("expected a non-empty fallback statement description")
	}
}

func TestAuditLogNoOpWithoutAttachedLog(t *testing.T) {
	db := storage.NewDB()
	// No AttachAuditLog call — must behave exactly as before, no panics,
	// no cost beyond a nil check.
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if db.AuditLog() != nil {
		t.Error("expected AuditLog() to return nil when never attached")
	}
}
