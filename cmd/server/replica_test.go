package main

import (
	"context"
	"crypto/tls"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

// newAdvancedWALTestServer opens a fresh ModeAdvancedWAL database in a
// temp directory and wraps it in a *server the same way newServer does in
// production, so the returned server has a real *storage.AdvancedWAL
// attached (db.AdvancedWAL() != nil) -- required for Bootstrap and
// GetChangesSince to do anything other than return their
// errNoAdvancedWAL error.
func newAdvancedWALTestServer(t *testing.T) (*server, *storage.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica_test.wal")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeAdvancedWAL, Path: path})
	if err != nil {
		t.Fatalf("open advanced wal db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newServer(db, "default", "", nil, nil, nil), db
}

// startTestGRPCServer spins up a real primary gRPC server on an
// OS-assigned loopback port via the production startGRPCServer helper,
// registers the jsonCodec (normally done once in run()), and returns the
// address it actually bound to plus a stop func.
func startTestGRPCServer(t *testing.T, srv *server, db *storage.DB) string {
	t.Helper()
	encoding.RegisterCodec(jsonCodec{})

	errChan := make(chan error, 1)
	grpcSrv, addr, err := startGRPCServer(srv, db, "127.0.0.1:0", tls.VersionTLS12, errChan)
	if err != nil {
		t.Fatalf("start grpc server: %v", err)
	}
	t.Cleanup(grpcSrv.GracefulStop)
	return addr
}

// replicaTransport names one of the two changes-loop implementations that
// share the exact replicaChangesLoopFunc shape -- runReplicaPollLoop (unary
// polling, stages 1-5's original transport) and runReplicaStreamLoop (gRPC
// server-streaming, stage 6's addition) -- and are otherwise interchangeable
// as far as replication correctness goes. replicaTransports lets the
// loop/convergence tests below run their body against both without
// duplicating it: see stage 6's task description for why this is preferred
// over writing every test twice.
type replicaTransport struct {
	name        string
	changesLoop replicaChangesLoopFunc
}

var replicaTransports = []replicaTransport{
	{"poll", runReplicaPollLoop},
	{"stream", runReplicaStreamLoop},
}

func TestBootstrapAndGetChangesSince(t *testing.T) {
	srv, db := newAdvancedWALTestServer(t)
	ctx := context.Background()

	if resp, err := srv.Exec(ctx, &execRequest{Tenant: "default", SQL: "CREATE TABLE t (id INT, name TEXT)"}); err != nil || !resp.Success {
		t.Fatalf("create table: resp=%+v err=%v", resp, err)
	}
	if resp, err := srv.Exec(ctx, &execRequest{Tenant: "default", SQL: "INSERT INTO t VALUES (1, 'a')"}); err != nil || !resp.Success {
		t.Fatalf("insert 1: resp=%+v err=%v", resp, err)
	}

	addr := startTestGRPCServer(t, srv, db)
	opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

	replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
	if err != nil {
		t.Fatalf("runReplicaBootstrap: %v", err)
	}
	defer func() { _ = replicaDB.Close() }()

	if watermark == 0 {
		t.Fatal("expected a nonzero watermark after a committed insert")
	}
	if epoch == 0 {
		t.Fatal("expected a nonzero epoch from a fresh AdvancedWAL")
	}

	checkSrv := newServer(replicaDB, "default", "", nil, nil, nil)
	resp, err := checkSrv.Query(ctx, &queryRequest{Tenant: "default", SQL: "SELECT id FROM t"})
	if err != nil {
		t.Fatalf("query after bootstrap: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("query after bootstrap: %s", resp.Error)
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("rows after bootstrap = %d, want 1", len(resp.Rows))
	}

	// Write a second row on the primary after the snapshot was taken, then
	// exercise a single GetChangesSince poll cycle directly (the full
	// poll-loop happy-path end-to-end test is a later stage's job -- this
	// just proves the RPC and apply path work).
	if resp, err := srv.Exec(ctx, &execRequest{Tenant: "default", SQL: "INSERT INTO t VALUES (2, 'b')"}); err != nil || !resp.Success {
		t.Fatalf("insert 2: resp=%+v err=%v", resp, err)
	}

	conn, err := dialPeerGRPC(addr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		t.Fatalf("dialPeerGRPC: %v", err)
	}
	defer func() { _ = conn.Close() }()

	applied, resumeLSN, err := pollChangesSinceOnce(ctx, conn, "default", watermark, epoch, replicaDB, opts)
	if err != nil {
		t.Fatalf("pollChangesSinceOnce: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
	if resumeLSN <= watermark {
		t.Fatalf("resumeLSN = %d, want > watermark %d", resumeLSN, watermark)
	}

	resp, err = checkSrv.Query(ctx, &queryRequest{Tenant: "default", SQL: "SELECT id FROM t"})
	if err != nil {
		t.Fatalf("query after poll: %v", err)
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("rows after poll = %d, want 2", len(resp.Rows))
	}

	// A poll with nothing new must report zero applied and an unchanged
	// resume point.
	applied, resumeLSN2, err := pollChangesSinceOnce(ctx, conn, "default", resumeLSN, epoch, replicaDB, opts)
	if err != nil {
		t.Fatalf("second pollChangesSinceOnce: %v", err)
	}
	if applied != 0 {
		t.Fatalf("second poll applied = %d, want 0", applied)
	}
	if resumeLSN2 != resumeLSN {
		t.Fatalf("second poll resumeLSN = %d, want unchanged %d", resumeLSN2, resumeLSN)
	}
}

// TestReplicaPollLoop_CatchesUpAndStopsOnCancel drives a changes loop
// (runReplicaPollLoop and runReplicaStreamLoop, in turn -- see
// replicaTransports) directly against a real in-process primary, proving
// each loop's happy path: it converges on a row written after bootstrap,
// and returns promptly once its context is canceled.
func TestReplicaPollLoop_CatchesUpAndStopsOnCancel(t *testing.T) {
	for _, tr := range replicaTransports {
		t.Run(tr.name, func(t *testing.T) {
			srv, db := newAdvancedWALTestServer(t)
			ctx := context.Background()

			if resp, err := srv.Exec(ctx, &execRequest{Tenant: "default", SQL: "CREATE TABLE t (id INT)"}); err != nil || !resp.Success {
				t.Fatalf("create table: resp=%+v err=%v", resp, err)
			}

			addr := startTestGRPCServer(t, srv, db)
			opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

			replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
			if err != nil {
				t.Fatalf("runReplicaBootstrap: %v", err)
			}
			defer func() { _ = replicaDB.Close() }()

			pollCtx, cancel := context.WithCancel(ctx)
			loopDone := make(chan error, 1)
			go func() {
				loopDone <- tr.changesLoop(pollCtx, replicaDB, addr, "default", watermark, epoch, opts)
			}()

			if resp, err := srv.Exec(ctx, &execRequest{Tenant: "default", SQL: "INSERT INTO t VALUES (1)"}); err != nil || !resp.Success {
				t.Fatalf("insert: resp=%+v err=%v", resp, err)
			}

			checkSrv := newServer(replicaDB, "default", "", nil, nil, nil)
			deadline := time.Now().Add(3 * time.Second)
			for {
				resp, err := checkSrv.Query(ctx, &queryRequest{Tenant: "default", SQL: "SELECT id FROM t"})
				if err != nil {
					t.Fatalf("query replica: %v", err)
				}
				if len(resp.Rows) == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("replica did not catch up in time: rows=%d", len(resp.Rows))
				}
				time.Sleep(5 * time.Millisecond)
			}

			cancel()
			select {
			case err := <-loopDone:
				if err == nil {
					t.Fatal("expected the changes loop to return ctx.Err() after cancel, got nil")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the changes loop did not return promptly after cancel")
			}
		})
	}
}

// mustExecOK runs sql against srv's Exec path and fails the test unless it
// succeeds -- a thin wrapper so end-to-end tests can express a sequence of
// primary-side DML statements as one-liners.
func mustExecOK(t *testing.T, srv *server, ctx context.Context, sql string) {
	t.Helper()
	resp, err := srv.Exec(ctx, &execRequest{Tenant: "default", SQL: sql})
	if err != nil || !resp.Success {
		t.Fatalf("exec %q: resp=%+v err=%v", sql, resp, err)
	}
}

// queryRowsOrFatal runs sql against srv's Query path and returns the
// resulting rows, failing the test on any error (RPC-level or SQL-level).
func queryRowsOrFatal(t *testing.T, srv *server, ctx context.Context, sql string) []map[string]any {
	t.Helper()
	resp, err := srv.Query(ctx, &queryRequest{Tenant: "default", SQL: sql})
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	if resp.Error != "" {
		t.Fatalf("query %q: %s", sql, resp.Error)
	}
	return resp.Rows
}

// TestReplicaEndToEndInsertUpdateConvergence drives the full one-way
// replication happy path for inserts and updates (deletes are stage 4's
// job, not this one). Some writes happen on the primary before the replica
// ever bootstraps, proving the snapshot-then-catch-up sequencing has no gap
// and no double-application right at the watermark boundary. Further writes
// after bootstrap -- including the same row (id=2) updated twice in a row,
// so only correctly ordered WAL application yields the right final value --
// must then be picked up by the poll loop. The assertion at the end compares
// every column of every row between primary and replica, not just row
// counts, so the replica's table contents must end up row-for-row identical
// to the primary's.
func TestReplicaEndToEndInsertUpdateConvergence(t *testing.T) {
	for _, tr := range replicaTransports {
		t.Run(tr.name, func(t *testing.T) {
			srv, db := newAdvancedWALTestServer(t)
			ctx := context.Background()

			// Writes before the replica exists at all.
			mustExecOK(t, srv, ctx, "CREATE TABLE accounts (id INT, name TEXT, balance INT)")
			mustExecOK(t, srv, ctx, "INSERT INTO accounts VALUES (1, 'alice', 100)")
			mustExecOK(t, srv, ctx, "INSERT INTO accounts VALUES (2, 'bob', 200)")
			mustExecOK(t, srv, ctx, "UPDATE accounts SET balance = 150 WHERE id = 1")

			addr := startTestGRPCServer(t, srv, db)
			opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

			// Bootstrap only now, after the writes above already happened.
			replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
			if err != nil {
				t.Fatalf("runReplicaBootstrap: %v", err)
			}
			defer func() { _ = replicaDB.Close() }()
			if watermark == 0 {
				t.Fatal("expected a nonzero watermark after committed writes preceding bootstrap")
			}

			// More writes after the bootstrap snapshot was taken: a fresh insert, a
			// row updated twice in a row, and an update to a row that was already
			// part of the bootstrap snapshot.
			mustExecOK(t, srv, ctx, "INSERT INTO accounts VALUES (3, 'carol', 300)")
			mustExecOK(t, srv, ctx, "UPDATE accounts SET balance = 250 WHERE id = 2")
			mustExecOK(t, srv, ctx, "UPDATE accounts SET balance = 275 WHERE id = 2")
			mustExecOK(t, srv, ctx, "UPDATE accounts SET name = 'alice2' WHERE id = 1")

			pollCtx, cancel := context.WithCancel(ctx)
			loopDone := make(chan error, 1)
			go func() {
				loopDone <- tr.changesLoop(pollCtx, replicaDB, addr, "default", watermark, epoch, opts)
			}()

			checkSrv := newServer(replicaDB, "default", "", nil, nil, nil)
			const sql = "SELECT id, name, balance FROM accounts ORDER BY id"
			primaryRows := queryRowsOrFatal(t, srv, ctx, sql)

			var replicaRows []map[string]any
			deadline := time.Now().Add(5 * time.Second)
			for {
				replicaRows = queryRowsOrFatal(t, checkSrv, ctx, sql)
				if len(replicaRows) == len(primaryRows) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("replica did not catch up in time: got %d rows, want %d", len(replicaRows), len(primaryRows))
				}
				time.Sleep(5 * time.Millisecond)
			}

			cancel()
			select {
			case err := <-loopDone:
				if err == nil {
					t.Fatal("expected the changes loop to return ctx.Err() after cancel, got nil")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the changes loop did not return promptly after cancel")
			}

			if !reflect.DeepEqual(primaryRows, replicaRows) {
				t.Fatalf("replica rows do not match primary row-for-row:\nprimary=%+v\nreplica=%+v", primaryRows, replicaRows)
			}
		})
	}
}

// TestReplicaEndToEndDeleteConvergence extends the insert/update happy path
// (TestReplicaEndToEndInsertUpdateConvergence) with DELETE propagation,
// including a genuine duplicate-by-content case: two rows that are
// value-identical to each other in every column.
//
// Known, accepted limitation exercised here (not fixed, per design): both
// applyOperation (local crash recovery) and ApplyWALRecord (this
// replication feed, which is just a thin wrapper around applyOperation)
// locate the row a WALOpDelete record removes by value-equality against
// that record's BeforeImage -- there is no row-identity tracking. Because
// the duplicate pair below is byte-for-byte identical, a WHERE predicate
// is a pure function of a row's own column values, so it is necessarily
// true for both members of the pair or neither -- the primary's own live
// DML engine (which deletes by row index during a table scan, not by
// content match) cannot single out just one of them either. A WHERE that
// captures the pair's shared content therefore deletes both together as
// part of the same DELETE statement, logging one WALOpDelete record per
// row with an identical BeforeImage payload. Replaying those two
// identical-content records on the replica means each replay's choice of
// *which* physical row satisfies the content match first is arbitrary --
// but since the two rows are indistinguishable by value, that
// per-record ambiguity has no effect on the resulting row multiset: two
// deletes against two indistinguishable rows leaves zero of them, on both
// primary and replica, every time. That is what this test asserts: not
// that the ambiguity is resolved, but that primary and replica resolve it
// the same way and converge on an identical final row set.
func TestReplicaEndToEndDeleteConvergence(t *testing.T) {
	for _, tr := range replicaTransports {
		t.Run(tr.name, func(t *testing.T) {
			srv, db := newAdvancedWALTestServer(t)
			ctx := context.Background()

			// Writes before the replica exists: two ordinary rows plus a duplicate
			// pair (id=3, label='dup', qty=50) inserted twice -- identical in
			// every column.
			mustExecOK(t, srv, ctx, "CREATE TABLE widgets (id INT, label TEXT, qty INT)")
			mustExecOK(t, srv, ctx, "INSERT INTO widgets VALUES (1, 'alpha', 10)")
			mustExecOK(t, srv, ctx, "INSERT INTO widgets VALUES (2, 'beta', 20)")
			mustExecOK(t, srv, ctx, "INSERT INTO widgets VALUES (3, 'dup', 50)")
			mustExecOK(t, srv, ctx, "INSERT INTO widgets VALUES (3, 'dup', 50)")

			addr := startTestGRPCServer(t, srv, db)
			opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

			replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
			if err != nil {
				t.Fatalf("runReplicaBootstrap: %v", err)
			}
			defer func() { _ = replicaDB.Close() }()
			if watermark == 0 {
				t.Fatal("expected a nonzero watermark after committed writes preceding bootstrap")
			}

			// After the snapshot: one more fresh insert, then a single DELETE
			// statement whose WHERE clause deletes a subset of the table -- the
			// 'alpha' row plus the duplicate pair's shared content (both rows,
			// per the comment above) -- leaving 'beta' and 'gamma' behind.
			mustExecOK(t, srv, ctx, "INSERT INTO widgets VALUES (4, 'gamma', 40)")
			mustExecOK(t, srv, ctx, "DELETE FROM widgets WHERE label = 'alpha' OR label = 'dup'")

			pollCtx, cancel := context.WithCancel(ctx)
			loopDone := make(chan error, 1)
			go func() {
				loopDone <- tr.changesLoop(pollCtx, replicaDB, addr, "default", watermark, epoch, opts)
			}()

			checkSrv := newServer(replicaDB, "default", "", nil, nil, nil)
			const sql = "SELECT id, label, qty FROM widgets ORDER BY id"
			primaryRows := queryRowsOrFatal(t, srv, ctx, sql)
			if len(primaryRows) != 2 {
				t.Fatalf("primary rows after delete = %d, want 2 (beta, gamma): %+v", len(primaryRows), primaryRows)
			}

			var replicaRows []map[string]any
			deadline := time.Now().Add(5 * time.Second)
			for {
				replicaRows = queryRowsOrFatal(t, checkSrv, ctx, sql)
				if len(replicaRows) == len(primaryRows) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("replica did not catch up in time: got %d rows, want %d", len(replicaRows), len(primaryRows))
				}
				time.Sleep(5 * time.Millisecond)
			}

			cancel()
			select {
			case err := <-loopDone:
				if err == nil {
					t.Fatal("expected the changes loop to return ctx.Err() after cancel, got nil")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the changes loop did not return promptly after cancel")
			}

			if !reflect.DeepEqual(primaryRows, replicaRows) {
				t.Fatalf("replica rows do not match primary row-for-row after delete:\nprimary=%+v\nreplica=%+v", primaryRows, replicaRows)
			}
		})
	}
}

// TestReplicaDeleteNetsOutWithinOneBatch covers a narrower case than
// TestReplicaEndToEndDeleteConvergence above: a row that is inserted and
// then deleted entirely after the replica's bootstrap watermark, so
// neither the insert nor the delete is in the bootstrap snapshot -- both
// exist only in the WAL stream the replica polls, and (with no poll in
// between) land in the very same GetChangesSince batch. This isolates the
// "insert then delete of the same row nets out to absent" case from the
// bootstrap-boundary and duplicate-content concerns the other end-to-end
// tests cover, using a single direct pollChangesSinceOnce call (rather
// than the full poll loop) so the batching is explicit and controlled by
// the test rather than incidental to loop timing.
func TestReplicaDeleteNetsOutWithinOneBatch(t *testing.T) {
	srv, db := newAdvancedWALTestServer(t)
	ctx := context.Background()

	mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT, name TEXT)")

	addr := startTestGRPCServer(t, srv, db)
	opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

	replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
	if err != nil {
		t.Fatalf("runReplicaBootstrap: %v", err)
	}
	defer func() { _ = replicaDB.Close() }()

	// The row with id=1 is inserted and then deleted -- both after the
	// bootstrap watermark above, so both operations live only in the WAL
	// records this single poll below will fetch. A second row (id=2) that
	// is only ever inserted, never deleted, proves the poll batch as a
	// whole still applies correctly rather than everything being wiped.
	mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (1, 'ephemeral')")
	mustExecOK(t, srv, ctx, "DELETE FROM t WHERE id = 1")
	mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (2, 'stays')")

	conn, err := dialPeerGRPC(addr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		t.Fatalf("dialPeerGRPC: %v", err)
	}
	defer func() { _ = conn.Close() }()

	applied, resumeLSN, err := pollChangesSinceOnce(ctx, conn, "default", watermark, epoch, replicaDB, opts)
	if err != nil {
		t.Fatalf("pollChangesSinceOnce: %v", err)
	}
	if applied != 3 {
		t.Fatalf("applied = %d, want 3 (insert id=1, delete id=1, insert id=2)", applied)
	}
	if resumeLSN <= watermark {
		t.Fatalf("resumeLSN = %d, want > watermark %d", resumeLSN, watermark)
	}

	checkSrv := newServer(replicaDB, "default", "", nil, nil, nil)
	const sql = "SELECT id, name FROM t ORDER BY id"
	primaryRows := queryRowsOrFatal(t, srv, ctx, sql)
	replicaRows := queryRowsOrFatal(t, checkSrv, ctx, sql)

	if len(primaryRows) != 1 {
		t.Fatalf("primary rows = %d, want 1 (id=1 inserted then deleted, id=2 stays): %+v", len(primaryRows), primaryRows)
	}
	if !reflect.DeepEqual(primaryRows, replicaRows) {
		t.Fatalf("replica rows do not match primary after insert+delete within one poll batch:\nprimary=%+v\nreplica=%+v", primaryRows, replicaRows)
	}
}

// TestReplicaDeleteNetsOutWithinOneBatch_Stream is
// TestReplicaDeleteNetsOutWithinOneBatch's streaming-transport counterpart:
// the exact same insert-then-delete-within-one-batch scenario, but read
// back via a single GetChanges stream message (openChangesStream +
// recvChangesOnce) instead of a single GetChangesSince unary call --
// proving _TinySQL_GetChanges_Handler batches a burst of WAL records
// already committed by the time the stream opens into one pushed message,
// exactly like the unary handler's single response.
func TestReplicaDeleteNetsOutWithinOneBatch_Stream(t *testing.T) {
	srv, db := newAdvancedWALTestServer(t)
	ctx := context.Background()

	mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT, name TEXT)")

	addr := startTestGRPCServer(t, srv, db)
	opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

	replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
	if err != nil {
		t.Fatalf("runReplicaBootstrap: %v", err)
	}
	defer func() { _ = replicaDB.Close() }()

	// Same three statements as the unary version, all committed before the
	// stream below is even opened, so they land in the primary's very
	// first pushed message.
	mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (1, 'ephemeral')")
	mustExecOK(t, srv, ctx, "DELETE FROM t WHERE id = 1")
	mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (2, 'stays')")

	conn, err := dialPeerGRPC(addr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		t.Fatalf("dialPeerGRPC: %v", err)
	}
	defer func() { _ = conn.Close() }()

	stream, cancel, err := openChangesStream(ctx, conn, "default", watermark, opts)
	if err != nil {
		t.Fatalf("openChangesStream: %v", err)
	}
	defer cancel()

	applied, resumeLSN, err := recvChangesOnce(stream, epoch, replicaDB)
	if err != nil {
		t.Fatalf("recvChangesOnce: %v", err)
	}
	if applied != 3 {
		t.Fatalf("applied = %d, want 3 (insert id=1, delete id=1, insert id=2)", applied)
	}
	if resumeLSN <= watermark {
		t.Fatalf("resumeLSN = %d, want > watermark %d", resumeLSN, watermark)
	}

	checkSrv := newServer(replicaDB, "default", "", nil, nil, nil)
	const sql = "SELECT id, name FROM t ORDER BY id"
	primaryRows := queryRowsOrFatal(t, srv, ctx, sql)
	replicaRows := queryRowsOrFatal(t, checkSrv, ctx, sql)

	if len(primaryRows) != 1 {
		t.Fatalf("primary rows = %d, want 1 (id=1 inserted then deleted, id=2 stays): %+v", len(primaryRows), primaryRows)
	}
	if !reflect.DeepEqual(primaryRows, replicaRows) {
		t.Fatalf("replica rows do not match primary after insert+delete within one stream batch:\nprimary=%+v\nreplica=%+v", primaryRows, replicaRows)
	}
}

func TestBootstrapRequiresAdvancedWAL(t *testing.T) {
	db := storage.NewDB()
	defer func() { _ = db.Close() }()
	srv := newServer(db, "default", "", nil, nil, nil)

	_, err := srv.Bootstrap(context.Background(), &bootstrapRequest{Tenant: "default"})
	if err == nil {
		t.Fatal("expected an error without an AdvancedWAL attached")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition", got)
	}
}

func TestGetChangesSinceRequiresAdvancedWAL(t *testing.T) {
	db := storage.NewDB()
	defer func() { _ = db.Close() }()
	srv := newServer(db, "default", "", nil, nil, nil)

	_, err := srv.GetChangesSince(context.Background(), &getChangesSinceRequest{Tenant: "default"})
	if err == nil {
		t.Fatal("expected an error without an AdvancedWAL attached")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition", got)
	}
}

// TestGetChangesStreamRequiresAdvancedWAL is
// TestGetChangesSinceRequiresAdvancedWAL's streaming counterpart:
// _TinySQL_GetChanges_Handler must reject a database with no AdvancedWAL
// attached the same way the unary GetChangesSince does (via the shared
// s.computeChangesSince helper), and it must do so as the very first thing
// the stream reports back rather than hanging waiting for WAL data that can
// never come.
func TestGetChangesStreamRequiresAdvancedWAL(t *testing.T) {
	db := storage.NewDB()
	defer func() { _ = db.Close() }()
	srv := newServer(db, "default", "", nil, nil, nil)

	addr := startTestGRPCServer(t, srv, db)
	opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

	conn, err := dialPeerGRPC(addr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		t.Fatalf("dialPeerGRPC: %v", err)
	}
	defer func() { _ = conn.Close() }()

	stream, cancel, err := openChangesStream(context.Background(), conn, "default", 0, opts)
	if err != nil {
		t.Fatalf("openChangesStream: %v", err)
	}
	defer cancel()

	if _, _, err := recvChangesOnce(stream, 0, nil); err == nil {
		t.Fatal("expected an error without an AdvancedWAL attached")
	} else if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition (err=%v)", got, err)
	}
}

// TestBootstrapRequiresAuth proves Bootstrap (and by the same code path,
// GetChangesSince) is covered by the server's existing auth check --
// grpc.UnaryInterceptor is installed once on the whole *grpc.Server in
// startGRPCServer, so it applies to every method on the TinySQL service,
// not just Exec/Query.
func TestBootstrapRequiresAuth(t *testing.T) {
	srv, db := newAdvancedWALTestServer(t)
	srv.authToken = "secret"

	addr := startTestGRPCServer(t, srv, db)

	if _, _, _, err := runReplicaBootstrap(context.Background(), addr, "default", replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}); err == nil {
		t.Fatal("expected an error when no auth token is supplied")
	}

	opts := replicaOptions{AuthToken: "secret", MaxRecvMsgSize: defaultMaxGRPCMsgBytes}
	if _, _, _, err := runReplicaBootstrap(context.Background(), addr, "default", opts); err != nil {
		t.Fatalf("bootstrap with correct auth token: %v", err)
	}
}

// TestGetChangesStreamRequiresAuth is TestBootstrapRequiresAuth's streaming
// counterpart: it proves _TinySQL_GetChanges_Handler's own inline auth
// check (grpc.UnaryInterceptor does not run for streaming RPCs -- see its
// doc comment) rejects a stream opened with no auth token, and accepts one
// opened with the correct token.
func TestGetChangesStreamRequiresAuth(t *testing.T) {
	srv, db := newAdvancedWALTestServer(t)
	ctx := context.Background()
	mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT)")

	addr := startTestGRPCServer(t, srv, db)
	srv.authToken = "secret"

	// Bootstrap with the correct token purely to get a real watermark/epoch
	// pair to open the stream at -- Bootstrap's own auth gate is already
	// covered by TestBootstrapRequiresAuth above.
	authedOpts := replicaOptions{AuthToken: "secret", MaxRecvMsgSize: defaultMaxGRPCMsgBytes}
	replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", authedOpts)
	if err != nil {
		t.Fatalf("runReplicaBootstrap: %v", err)
	}
	defer func() { _ = replicaDB.Close() }()

	unauthedOpts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}
	conn, err := dialPeerGRPC(addr, unauthedOpts.MaxRecvMsgSize, unauthedOpts.TransportCreds)
	if err != nil {
		t.Fatalf("dialPeerGRPC: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// No auth token supplied: the handler's inline auth check must reject
	// the stream before ever reading a WAL record.
	noAuthStream, cancel, err := openChangesStream(ctx, conn, "default", watermark, unauthedOpts)
	if err != nil {
		t.Fatalf("openChangesStream (no auth): %v", err)
	}
	if _, _, err := recvChangesOnce(noAuthStream, epoch, nil); err == nil {
		t.Fatal("expected an error when no auth token is supplied")
	} else if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("status code = %v, want Unauthenticated (err=%v)", got, err)
	}
	cancel()

	// With the correct token, and a record already committed past
	// watermark, the stream must succeed and deliver it on the first
	// pushed message.
	mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (1)")
	authedStream, cancel2, err := openChangesStream(ctx, conn, "default", watermark, authedOpts)
	if err != nil {
		t.Fatalf("openChangesStream (authed): %v", err)
	}
	defer cancel2()

	applied, resumeLSN, err := recvChangesOnce(authedStream, epoch, replicaDB)
	if err != nil {
		t.Fatalf("recvChangesOnce (authed): %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
	if resumeLSN <= watermark {
		t.Fatalf("resumeLSN = %d, want > watermark %d", resumeLSN, watermark)
	}
}

// TestReplicaLoopRebootstrapsAndConvergesAfterCheckpointOutrunsReplica
// exercises the checkpoint-outran-replica case end to end through
// runReplicaLoopWithTransport (the orchestrator both runReplicaLoop and
// runReplica use in production, parameterized here to run against each of
// replicaTransports in turn): the primary checkpoints past a running
// replica's current position, and the replica must detect this on its very
// next poll/push, transparently re-bootstrap from scratch, and converge
// with the primary -- rather than erroring out or looping forever retrying
// an unserviceable LSN range.
//
// The onBootstrap hook is used purely to sequence the test deterministically
// (see its comment inline below) -- it plays no role in the behavior under
// test, which is runReplicaLoopWithTransport's reaction to
// errReplicaNeedsRebootstrap.
func TestReplicaLoopRebootstrapsAndConvergesAfterCheckpointOutrunsReplica(t *testing.T) {
	for _, tr := range replicaTransports {
		t.Run(tr.name, func(t *testing.T) {
			srv, db := newAdvancedWALTestServer(t)
			ctx := context.Background()

			mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT, name TEXT)")
			mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (1, 'a')")

			addr := startTestGRPCServer(t, srv, db)
			opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

			var latest atomic.Pointer[storage.DB]
			var bootstrapCount int32
			firstBootstrapDone := make(chan struct{})
			proceedWithPoll := make(chan struct{})
			onBootstrap := func(rdb *storage.DB) {
				latest.Store(rdb)
				// Only the very first bootstrap blocks: it hands control back to
				// the main test goroutine (via firstBootstrapDone) and waits (via
				// proceedWithPoll) until the primary-side writes and checkpoint
				// below are done, guaranteeing the changes loop's first
				// poll/push -- not its bootstrap -- is what hits the
				// now-too-far-behind watermark. Without this, the primary
				// writes/checkpoint could race against an already-in-flight
				// poll/push and land before or after it unpredictably.
				if atomic.AddInt32(&bootstrapCount, 1) == 1 {
					close(firstBootstrapDone)
					<-proceedWithPoll
				}
			}

			loopCtx, cancel := context.WithCancel(ctx)
			loopDone := make(chan error, 1)
			go func() {
				_, err := runReplicaLoopWithTransport(loopCtx, addr, "default", opts, onBootstrap, tr.changesLoop)
				loopDone <- err
			}()

			select {
			case <-firstBootstrapDone:
			case <-time.After(3 * time.Second):
				t.Fatal("initial bootstrap did not happen in time")
			}

			// The replica's loop is blocked inside onBootstrap and has made no
			// GetChangesSince call/GetChanges stream request yet, so these
			// primary-side writes and the checkpoint that truncates them out of
			// the live WAL cannot race with anything the replica does.
			mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (2, 'b')")
			mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (3, 'c')")
			wal := db.AdvancedWAL()
			if wal == nil {
				t.Fatal("expected an AdvancedWAL on the primary")
			}
			if err := wal.Checkpoint(db); err != nil {
				t.Fatalf("primary checkpoint: %v", err)
			}

			close(proceedWithPoll)

			// The replica's first poll/push (using its stale, now-checkpointed-away
			// watermark) must trigger a second bootstrap rather than erroring out
			// or retrying forever.
			deadline := time.Now().Add(5 * time.Second)
			for atomic.LoadInt32(&bootstrapCount) < 2 {
				if time.Now().After(deadline) {
					t.Fatal("replica did not re-bootstrap after the primary checkpoint outran it")
				}
				time.Sleep(5 * time.Millisecond)
			}

			cancel()
			var loopErr error
			select {
			case loopErr = <-loopDone:
			case <-time.After(3 * time.Second):
				t.Fatal("the changes loop did not return promptly after cancel")
			}
			if loopErr == nil {
				t.Fatal("expected the changes loop to return ctx.Err() after cancel, got nil")
			}

			// Reading `latest`/the DB only now, after loopDone confirms the
			// changes loop's goroutine has fully exited, needs no further
			// synchronization: nothing else touches either anymore.
			finalDB := latest.Load()
			if finalDB == nil {
				t.Fatal("expected a final replica DB after re-bootstrap")
			}
			defer func() { _ = finalDB.Close() }()

			checkSrv := newServer(finalDB, "default", "", nil, nil, nil)
			const sql = "SELECT id, name FROM t ORDER BY id"
			primaryRows := queryRowsOrFatal(t, srv, ctx, sql)
			if len(primaryRows) != 3 {
				t.Fatalf("primary rows = %d, want 3", len(primaryRows))
			}
			replicaRows := queryRowsOrFatal(t, checkSrv, ctx, sql)
			if !reflect.DeepEqual(primaryRows, replicaRows) {
				t.Fatalf("replica rows do not match primary after re-bootstrap:\nprimary=%+v\nreplica=%+v", primaryRows, replicaRows)
			}
		})
	}
}

// TestReplicaLoopRebootstrapsOnEpochMismatchAfterPrimaryReset simulates a
// primary being wiped and restored from backup while a replica is
// mid-poll, using the exact same address the replica already knows about:
// the replica's poll must detect the epoch mismatch and re-bootstrap,
// converging on the reset primary's data, rather than silently continuing
// to poll a stale LSN against the reset primary's unrelated history (which
// storage.ErrReplicaTooFarBehind alone would not catch here -- see
// getChangesSinceResponse.Epoch's doc comment: the reset primary's fresh
// WAL restarts LSN numbering from 1, so the replica's stale, numerically
// larger sinceLSN does not trip codes.OutOfRange).
//
// The reset itself is simulated by swapping the *server's db field to a
// brand-new AdvancedWAL-backed database at a different path, in place,
// without tearing down the gRPC listener: Bootstrap/GetChangesSince both
// read s.db.AdvancedWAL() fresh on every call, so this takes effect
// immediately, and a fresh WAL file always mints a new epoch (see
// storage.AdvancedWAL's epoch field doc comment) -- exactly what a real
// backup restore looks like to a replica that only ever talks to a gRPC
// address, never to the underlying files directly.
func TestReplicaLoopRebootstrapsOnEpochMismatchAfterPrimaryReset(t *testing.T) {
	for _, tr := range replicaTransports {
		t.Run(tr.name, func(t *testing.T) {
			srv, db := newAdvancedWALTestServer(t)
			ctx := context.Background()

			mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT, name TEXT)")
			mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (1, 'a')")

			addr := startTestGRPCServer(t, srv, db)
			opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes}

			var latest atomic.Pointer[storage.DB]
			var bootstrapCount int32
			firstBootstrapDone := make(chan struct{})
			proceedWithPoll := make(chan struct{})
			onBootstrap := func(rdb *storage.DB) {
				latest.Store(rdb)
				if atomic.AddInt32(&bootstrapCount, 1) == 1 {
					close(firstBootstrapDone)
					<-proceedWithPoll
				}
			}

			loopCtx, cancel := context.WithCancel(ctx)
			loopDone := make(chan error, 1)
			go func() {
				_, err := runReplicaLoopWithTransport(loopCtx, addr, "default", opts, onBootstrap, tr.changesLoop)
				loopDone <- err
			}()

			select {
			case <-firstBootstrapDone:
			case <-time.After(3 * time.Second):
				t.Fatal("initial bootstrap did not happen in time")
			}

			// Simulate the reset while the replica's loop is blocked inside
			// onBootstrap, before it has made any GetChangesSince call/GetChanges
			// stream request.
			oldDB := srv.db
			resetPath := filepath.Join(t.TempDir(), "reset_primary.wal")
			resetDB, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeAdvancedWAL, Path: resetPath})
			if err != nil {
				t.Fatalf("open reset primary db: %v", err)
			}
			t.Cleanup(func() { _ = resetDB.Close() })
			srv.db = resetDB
			_ = oldDB.Close()

			mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT, name TEXT)")
			mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (99, 'reset')")

			close(proceedWithPoll)

			deadline := time.Now().Add(5 * time.Second)
			for atomic.LoadInt32(&bootstrapCount) < 2 {
				if time.Now().After(deadline) {
					t.Fatal("replica did not re-bootstrap after the primary's epoch changed")
				}
				time.Sleep(5 * time.Millisecond)
			}

			cancel()
			var loopErr error
			select {
			case loopErr = <-loopDone:
			case <-time.After(3 * time.Second):
				t.Fatal("the changes loop did not return promptly after cancel")
			}
			if loopErr == nil {
				t.Fatal("expected the changes loop to return ctx.Err() after cancel, got nil")
			}

			finalDB := latest.Load()
			if finalDB == nil {
				t.Fatal("expected a final replica DB after re-bootstrap")
			}
			defer func() { _ = finalDB.Close() }()

			checkSrv := newServer(finalDB, "default", "", nil, nil, nil)
			resp, err := checkSrv.Query(ctx, &queryRequest{Tenant: "default", SQL: "SELECT id, name FROM t"})
			if err != nil {
				t.Fatalf("query after re-bootstrap: %v", err)
			}
			if resp.Error != "" {
				t.Fatalf("query after re-bootstrap: %s", resp.Error)
			}
			if len(resp.Rows) != 1 || resp.Rows[0]["name"] != "reset" {
				t.Fatalf("rows after re-bootstrap = %+v, want exactly the reset primary's row (name=reset)", resp.Rows)
			}
		})
	}
}

// GetChanges is a long-lived server-streaming RPC, but openChangesStream built
// its context with replicaCallContext, which applies opts.CallTimeout. On a
// stream that deadline bounds the whole stream rather than one round trip, so
// the stream was guaranteed to die with DeadlineExceeded after CallTimeout
// (10s by default in production) and reconnect as if the network had blipped.
//
// A short CallTimeout here makes that failure fast: the stream must still be
// readable well after it would have expired.
func TestChangesStreamOutlivesTheCallTimeout(t *testing.T) {
	srv, db := newAdvancedWALTestServer(t)
	ctx := context.Background()
	mustExecOK(t, srv, ctx, "CREATE TABLE t (id INT)")

	addr := startTestGRPCServer(t, srv, db)
	const callTimeout = 150 * time.Millisecond
	opts := replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes, CallTimeout: callTimeout}

	replicaDB, watermark, epoch, err := runReplicaBootstrap(ctx, addr, "default", opts)
	if err != nil {
		t.Fatalf("runReplicaBootstrap: %v", err)
	}
	defer func() { _ = replicaDB.Close() }()

	conn, err := dialPeerGRPC(addr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		t.Fatalf("dialPeerGRPC: %v", err)
	}
	defer func() { _ = conn.Close() }()

	stream, cancel, err := openChangesStream(ctx, conn, "default", watermark, opts)
	if err != nil {
		t.Fatalf("openChangesStream: %v", err)
	}
	defer cancel()

	// Well past the deadline the stream used to inherit.
	time.Sleep(4 * callTimeout)

	mustExecOK(t, srv, ctx, "INSERT INTO t VALUES (1)")

	// The primary pushes an empty batch whenever it has nothing new, so read
	// on until the insert's records arrive. The point of the test is that no
	// read ever fails with DeadlineExceeded.
	deadline := time.Now().Add(10 * time.Second)
	total := 0
	for total == 0 && time.Now().Before(deadline) {
		applied, _, err := recvChangesOnce(stream, epoch, replicaDB)
		if err != nil {
			t.Fatalf("stream died %v after opening, having outlived %d call timeouts of %v: %v",
				time.Since(deadline.Add(-10*time.Second)), 4, callTimeout, err)
		}
		total += applied
	}
	if total == 0 {
		t.Fatal("stream delivered no records after the insert")
	}
}

// A unary RPC must keep its per-call deadline; only the stream opts out.
func TestReplicaCallContextKeepsUnaryDeadline(t *testing.T) {
	unary, cancelUnary := replicaCallContext(context.Background(), replicaOptions{CallTimeout: time.Second})
	defer cancelUnary()
	if _, ok := unary.Deadline(); !ok {
		t.Error("a unary call context lost its deadline")
	}

	streamed, cancelStream := replicaCallContext(context.Background(), replicaOptions{CallTimeout: 0})
	defer cancelStream()
	if deadline, ok := streamed.Deadline(); ok {
		t.Errorf("a stream call context must carry no deadline, got %v", deadline)
	}
	// Still cancellable, so deferring the cancel really does release it.
	cancelStream()
	if streamed.Err() == nil {
		t.Error("cancelling a stream call context did not cancel it")
	}
}
