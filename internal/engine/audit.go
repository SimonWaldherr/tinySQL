// Audit logging hook for Execute (see internal/storage/audit.go for the
// hash-chained log itself).
//
// Execute receives a parsed Statement, not the original SQL text — there's
// no reliable way to reconstruct exact SQL from an AST (the trigger-body
// bug fixed earlier this session, where a from-scratch AST-to-SQL printer
// silently produced non-executable text, is exactly why this doesn't
// attempt that). Callers that already have the original text (cmd/server,
// internal/driver — both parse from a string themselves) can attach it via
// WithAuditText so the audit trail records the real query; callers that
// only ever construct/receive an AST get a best-effort "<*engine.Insert>"
// style fallback identifying the statement kind, which is still enough to
// see *that* a mutation happened even without the exact text.
package engine

import "context"

type auditTextContextKey struct{}

// WithAuditText attaches the original SQL text of the statement about to
// be executed, for the audit log to record verbatim. Compose with
// WithUser: engine.WithAuditText(engine.WithUser(ctx, user), sqlText).
func WithAuditText(ctx context.Context, sqlText string) context.Context {
	return context.WithValue(ctx, auditTextContextKey{}, sqlText)
}

func auditTextFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	s, ok := ctx.Value(auditTextContextKey{}).(string)
	return s, ok && s != ""
}
