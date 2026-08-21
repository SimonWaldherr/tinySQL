// SQLite transaction-control spellings. These used to fall through the driver
// into the engine and surface as `no such table "..."`, which is the least
// useful possible answer to `BEGIN IMMEDIATE`.
package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

// TestClassifyTransactionSQLSpellings pins the exact set of accepted spellings
// at the classifier level, where the whole table is cheap to state. The
// end-to-end behaviour is covered by the tests below.
func TestClassifyTransactionSQLSpellings(t *testing.T) {
	cases := []struct {
		sql        string
		wantAction txAction
		wantRO     bool
	}{
		// Pre-existing forms must keep classifying exactly as before.
		{"BEGIN", txBegin, false},
		{"BEGIN TRANSACTION", txBegin, false},
		{"START TRANSACTION", txBegin, false},
		{"BEGIN READ ONLY", txBegin, true},
		{"BEGIN TRANSACTION READ ONLY", txBegin, true},
		{"START TRANSACTION READ ONLY", txBegin, true},
		{"COMMIT", txCommit, false},
		{"COMMIT TRANSACTION", txCommit, false},
		{"ROLLBACK", txRollback, false},
		{"ROLLBACK TRANSACTION", txRollback, false},

		// SQLite's locking-strength spellings all mean plain BEGIN here: the
		// writer semaphore is held for the whole transaction, so tinySQL
		// already behaves like SQLite's IMMEDIATE.
		{"BEGIN DEFERRED", txBegin, false},
		{"BEGIN IMMEDIATE", txBegin, false},
		{"BEGIN EXCLUSIVE", txBegin, false},
		{"BEGIN DEFERRED TRANSACTION", txBegin, false},
		{"BEGIN IMMEDIATE TRANSACTION", txBegin, false},
		{"BEGIN EXCLUSIVE TRANSACTION", txBegin, false},
		{"BEGIN IMMEDIATE READ ONLY", txBegin, true},

		// END is SQLite's COMMIT synonym.
		{"END", txCommit, false},
		{"END TRANSACTION", txCommit, false},

		// Savepoints stay unrecognized ON PURPOSE. There is no nested
		// transaction support, so folding these onto BEGIN/COMMIT/ROLLBACK
		// would silently discard or silently keep work. txNone routes them to
		// the engine, which rejects them. Do not "fix" these rows.
		{"SAVEPOINT sp1", txNone, false},
		{"RELEASE sp1", txNone, false},
		{"RELEASE SAVEPOINT sp1", txNone, false},
		{"ROLLBACK TO sp1", txNone, false},
		{"ROLLBACK TO SAVEPOINT sp1", txNone, false},
		{"ROLLBACK TRANSACTION TO SAVEPOINT sp1", txNone, false},

		// Not transaction control at all.
		{"SELECT 1", txNone, false},
		{"BEGINNING", txNone, false},
		{"ENDS", txNone, false},
		{"COMMIT READ ONLY", txNone, false},
	}
	for _, tc := range cases {
		action, ro := classifyTransactionSQL(normalizeTransactionSQL(tc.sql))
		if action != tc.wantAction || ro != tc.wantRO {
			t.Errorf("classifyTransactionSQL(%q) = (%v, %v), want (%v, %v)",
				tc.sql, action, ro, tc.wantAction, tc.wantRO)
		}
	}
}

// TestLooksLikeTransactionControlRoutesEND guards the cheap first-word gate in
// execSQL: if END is not routed there, execTransactionControl never sees it and
// the classifier fix above is dead code on the Exec path.
func TestLooksLikeTransactionControlRoutesEND(t *testing.T) {
	for _, sql := range []string{
		"END", "end;", "  END TRANSACTION ", "BEGIN IMMEDIATE", "begin exclusive transaction",
	} {
		if !looksLikeTransactionControl(sql) {
			t.Errorf("looksLikeTransactionControl(%q) = false, want true", sql)
		}
	}
	for _, sql := range []string{"SELECT 1", "INSERT INTO t VALUES (1)", "CREATE TABLE t (a INT)"} {
		if looksLikeTransactionControl(sql) {
			t.Errorf("looksLikeTransactionControl(%q) = true, want false", sql)
		}
	}
}

func TestParseCacheCandidateIgnoresKeywordCaseWithoutChangingEligibility(t *testing.T) {
	for _, sql := range []string{"SELECT 1", "select 1", "  ExPlAiN SELECT 1"} {
		if !parseCacheCandidate(sql) {
			t.Errorf("parseCacheCandidate(%q) = false, want true", sql)
		}
	}
	for _, sql := range []string{"SELECTED 1", "INSERT INTO t VALUES (1)", "-- SELECT 1"} {
		if parseCacheCandidate(sql) {
			t.Errorf("parseCacheCandidate(%q) = true, want false", sql)
		}
	}
}

// TestSQLiteTransactionSpellingsCommitAndRollback runs the new spellings
// through the real Exec path: BEGIN IMMEDIATE ... END must durably commit, and
// BEGIN EXCLUSIVE ... ROLLBACK must discard.
func TestSQLiteTransactionSpellingsCommitAndRollback(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, "CREATE TABLE tx_sqlite (id INT)", nil); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// BEGIN IMMEDIATE + END (SQLite's COMMIT synonym).
	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE", nil); err != nil {
		t.Fatalf("BEGIN IMMEDIATE failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO tx_sqlite VALUES (1)", nil); err != nil {
		t.Fatalf("insert in tx failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "END", nil); err != nil {
		t.Fatalf("END failed: %v", err)
	}

	// BEGIN EXCLUSIVE TRANSACTION + ROLLBACK.
	if _, err := c.ExecContext(ctx, "BEGIN EXCLUSIVE TRANSACTION", nil); err != nil {
		t.Fatalf("BEGIN EXCLUSIVE TRANSACTION failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO tx_sqlite VALUES (2)", nil); err != nil {
		t.Fatalf("insert in second tx failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// BEGIN DEFERRED + END TRANSACTION.
	if _, err := c.ExecContext(ctx, "begin deferred;", nil); err != nil {
		t.Fatalf("begin deferred failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO tx_sqlite VALUES (3)", nil); err != nil {
		t.Fatalf("insert in third tx failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "end transaction;", nil); err != nil {
		t.Fatalf("end transaction failed: %v", err)
	}

	rows, err := c.QueryContext(ctx, "SELECT id FROM tx_sqlite ORDER BY id", nil)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	defer rows.Close()
	var got []int64
	dest := make([]driver.Value, 1)
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		switch v := dest[0].(type) {
		case int64:
			got = append(got, v)
		case int:
			got = append(got, int64(v))
		default:
			t.Fatalf("unexpected value type %T", dest[0])
		}
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("rows = %v, want [1 3] (the rolled-back 2 must be gone)", got)
	}
}

// TestENDCommitsViaQueryPath covers querySQL, which calls
// execTransactionControl without the looksLikeTransactionControl gate — a
// caller using Query("END") must commit too, not get a parse error.
func TestENDCommitsViaQueryPath(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, "CREATE TABLE tx_query_end (id INT)", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE TRANSACTION", nil); err != nil {
		t.Fatalf("BEGIN IMMEDIATE TRANSACTION failed: %v", err)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO tx_query_end VALUES (7)", nil); err != nil {
		t.Fatal(err)
	}
	qrows, err := c.QueryContext(ctx, "END", nil)
	if err != nil {
		t.Fatalf("Query(\"END\") failed: %v", err)
	}
	_ = qrows.Close()
	if c.inTx {
		t.Fatal("still in a transaction after END")
	}
}

// TestSavepointSpellingsStillRejected is the counterpart to the txNone rows in
// the classifier table: the driver must not have quietly started accepting
// savepoints. Only the error's *kind* matters here (some rejection), not its
// text, since the message comes from the engine.
func TestSavepointSpellingsStillRejected(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, "CREATE TABLE tx_sp (id INT)", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = c.ExecContext(ctx, "ROLLBACK", nil) }()
	for _, sql := range []string{"SAVEPOINT sp1", "RELEASE sp1", "ROLLBACK TO sp1"} {
		if _, err := c.ExecContext(ctx, sql, nil); err == nil {
			t.Fatalf("%q was accepted; savepoints are not implemented and must keep failing", sql)
		}
	}
	// A plain ROLLBACK must still work after those rejections, i.e. the
	// rejected statements did not consume or corrupt the transaction.
	if !c.inTx {
		t.Fatal("transaction was lost by a rejected savepoint statement")
	}
}
