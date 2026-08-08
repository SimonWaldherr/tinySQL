// Replica-side CLI mode for one-way WAL-based replication: -replica-of
// tells this process to bootstrap a snapshot from a primary tinySQL server
// and then continuously poll for and apply new committed WAL records,
// instead of running as a normal primary/standalone server. See main.go's
// server.Bootstrap and server.GetChangesSince for the primary-side RPCs
// this drives, and internal/storage/wal_feed.go for the underlying
// snapshot/apply primitives (SnapshotWithWatermark, ReadCommittedSince,
// ApplyWALRecord).
package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// errReplicaNeedsRebootstrap is returned by pollChangesSinceOnce and
// runReplicaPollLoop when continuing to poll incrementally is no longer
// safe, for either of two reasons:
//
//   - the primary reported codes.OutOfRange (storage.ErrReplicaTooFarBehind
//     on the primary side, via GetChangesSince): the requested LSN range has
//     already been checkpointed away, so it can no longer be served
//     incrementally; or
//   - the primary's current WAL epoch (see storage.AdvancedWAL.Epoch) no
//     longer matches the epoch this replica saw at its last bootstrap,
//     meaning the primary's WAL/checkpoint files were wiped or restored
//     from backup out from under it (see getChangesSinceResponse.Epoch's
//     doc comment for why this can happen with no OutOfRange status at all).
//
// Either way, incremental application can no longer be trusted to be
// gap-free, so a full re-bootstrap (see runReplicaLoop) is required rather
// than continuing to poll or surfacing this as a fatal, unrecoverable error.
var errReplicaNeedsRebootstrap = errors.New("replica must re-bootstrap: primary is too far ahead of the replica's last known position, or its WAL epoch changed")

// flagReplicaOf selects the replica-side CLI mode. Empty (the default)
// means normal primary/standalone mode, unchanged. When set, run() hands
// off to runReplica instead of starting the usual HTTP/gRPC serve loop.
var flagReplicaOf = flag.String("replica-of", "", "gRPC address of a primary tinySQL server to replicate from (the primary must be running with a DSN mode=advanced_wal); when set, this process bootstraps an in-memory copy of the primary and continuously polls for and applies new committed WAL records instead of serving as a normal primary/standalone server")

// flagReplicaTransport selects which transport -replica-of uses to fetch
// WAL changes from the primary after bootstrapping: "stream" (the
// default) opens one long-lived gRPC server-streaming GetChanges call
// (runReplicaStreamLoop); "poll" repeatedly invokes the unary
// GetChangesSince RPC instead (runReplicaPollLoop, stages 1-5's original
// transport). Both apply the exact same replication semantics -- this
// flag only changes how records get from the primary to the replica, kept
// selectable in case gRPC streaming proves awkward against this server's
// hand-rolled JSON codec in some environment.
var flagReplicaTransport = flag.String("replica-transport", "stream", `Transport -replica-of uses to fetch WAL changes from the primary: "stream" (default, gRPC server-streaming) or "poll" (unary polling, kept as a fallback)`)

const (
	// replicaMinPollBackoff/replicaMaxPollBackoff bound runReplicaPollLoop's
	// backoff: it starts at replicaMinPollBackoff when a poll returns zero
	// records, doubles on every further empty/failed poll up to
	// replicaMaxPollBackoff, and resets to replicaMinPollBackoff the instant
	// a poll returns any records.
	replicaMinPollBackoff = 25 * time.Millisecond
	replicaMaxPollBackoff = 1 * time.Second
)

// replicaOptions bundles the client-connection parameters
// runReplicaBootstrap and runReplicaPollLoop need to reach a primary
// tinySQL server, mirroring the parameters grpcQuery (main.go) already
// takes for federation peer calls: an auth token forwarded the same way (a
// bearer token via outgoing gRPC metadata), a per-call timeout, a max
// receive message size, and optional TLS transport credentials built the
// same way loadPeerTLSCredentials builds them for federation peers.
type replicaOptions struct {
	AuthToken      string
	CallTimeout    time.Duration
	MaxRecvMsgSize int
	TransportCreds credentials.TransportCredentials
}

// dialPeerGRPC opens a client connection to a tinySQL gRPC server (primary
// or peer) using the same JSON-codec-forced dial pattern grpcQuery (main.go)
// uses for federation peer calls, factored out here so both
// runReplicaBootstrap and runReplicaPollLoop share one dial path. The
// gRPC-level codec only wraps the outer request/response envelope --
// SnapshotGob/RecordsGob inside those envelopes are gob-encoded separately
// (see the type comment on getChangesSinceResponse in main.go).
func dialPeerGRPC(addr string, maxRecvMsg int, transportCreds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("empty primary address")
	}
	if transportCreds == nil {
		transportCreds = insecure.NewCredentials()
	}
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(
			grpc.ForceCodec(jsonCodec{}),
			grpc.MaxCallRecvMsgSize(maxRecvMsg),
		),
	}
	return grpc.NewClient(addr, dialOptions...)
}

// replicaCallContext derives a per-call context from ctx, applying opts's
// call timeout (if any) and bearer-token auth metadata (if any) -- the same
// two things grpcQuery (main.go) applies to a federation peer call. The
// returned cancel is always safe to defer, even when it is a no-op.
func replicaCallContext(ctx context.Context, opts replicaOptions) (context.Context, context.CancelFunc) {
	cancel := func() {}
	if opts.CallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.CallTimeout)
	}
	if opts.AuthToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+opts.AuthToken)
	}
	return ctx, cancel
}

// runReplicaBootstrap fetches an initial snapshot, its WAL LSN watermark,
// and the primary's current WAL epoch from the primary at primaryAddr via
// the Bootstrap RPC (server.Bootstrap in main.go), decodes the snapshot into
// a ready, in-memory *storage.DB via storage.LoadFromBytes, and returns that
// DB together with the watermark LSN and epoch a subsequent
// runReplicaPollLoop call should resume from (epoch is compared against
// every later GetChangesSince response -- see errReplicaNeedsRebootstrap's
// doc comment).
//
// The returned DB has no WAL attached and runs in ModeMemory: a v1 replica
// that restarts, or that a poll detects has fallen out of sync with the
// primary, simply calls runReplicaBootstrap again from scratch rather than
// resuming from local durable state (see runReplicaLoop).
func runReplicaBootstrap(ctx context.Context, primaryAddr, tenant string, opts replicaOptions) (db *storage.DB, watermark uint64, epoch uint64, err error) {
	conn, err := dialPeerGRPC(primaryAddr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("connect to primary %s: %w", primaryAddr, err)
	}
	defer func() { _ = conn.Close() }()

	callCtx, cancel := replicaCallContext(ctx, opts)
	defer cancel()

	var resp bootstrapResponse
	req := &bootstrapRequest{Tenant: tenant}
	if err := conn.Invoke(callCtx, "/tinysql.TinySQL/Bootstrap", req, &resp); err != nil {
		return nil, 0, 0, fmt.Errorf("bootstrap from primary %s: %w", primaryAddr, err)
	}

	db, err = storage.LoadFromBytes(resp.SnapshotGob)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("decode snapshot from primary %s: %w", primaryAddr, err)
	}
	return db, resp.WatermarkLSN, resp.Epoch, nil
}

// pollChangesSinceOnce performs one GetChangesSince RPC call against conn,
// decodes whatever WAL records come back, and applies each in order via
// storage.ApplyWALRecord. It returns how many records were applied plus
// the resume LSN the caller should pass as sinceLSN on its next call --
// resp.ResumeLSN unchanged (i.e. still sinceLSN) when nothing new was
// found, matching storage.ReadCommittedSince's own contract on the primary
// side.
//
// expectedEpoch is whatever epoch the caller's last bootstrap returned. Two
// distinct conditions both map to the same errReplicaNeedsRebootstrap
// signal rather than a generic error or a normal retry: the primary
// returning codes.OutOfRange (its WAL already checkpointed away the
// requested range), and resp.Epoch not matching expectedEpoch (the primary
// is not the same WAL/checkpoint incarnation this replica last saw at all --
// see errReplicaNeedsRebootstrap's doc comment for why both need checking).
func pollChangesSinceOnce(ctx context.Context, conn *grpc.ClientConn, tenant string, sinceLSN, expectedEpoch uint64, db *storage.DB, opts replicaOptions) (applied int, resumeLSN uint64, err error) {
	callCtx, cancel := replicaCallContext(ctx, opts)
	defer cancel()

	var resp getChangesSinceResponse
	req := &getChangesSinceRequest{Tenant: tenant, SinceLSN: sinceLSN}
	if err := conn.Invoke(callCtx, "/tinysql.TinySQL/GetChangesSince", req, &resp); err != nil {
		if status.Code(err) == codes.OutOfRange {
			return 0, sinceLSN, errReplicaNeedsRebootstrap
		}
		return 0, sinceLSN, fmt.Errorf("get changes since %d: %w", sinceLSN, err)
	}

	if resp.Epoch != expectedEpoch {
		return 0, sinceLSN, errReplicaNeedsRebootstrap
	}

	applied, err = applyChangesResponse(db, &resp)
	if err != nil {
		return 0, sinceLSN, err
	}

	return applied, resp.ResumeLSN, nil
}

// applyChangesResponse gob-decodes resp.RecordsGob (if any) into WAL
// records and applies each in order via storage.ApplyWALRecord, returning
// how many were applied. Shared by pollChangesSinceOnce (unary
// GetChangesSince transport) and recvChangesOnce (streaming GetChanges
// transport) so both decode and apply a getChangesSinceResponse
// identically -- the two transports differ only in how this response
// reaches the replica, not in what is done with it once it has.
func applyChangesResponse(db *storage.DB, resp *getChangesSinceResponse) (applied int, err error) {
	var records []storage.WALRecord
	if len(resp.RecordsGob) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(resp.RecordsGob)).Decode(&records); err != nil {
			return 0, fmt.Errorf("decode WAL records: %w", err)
		}
	}

	for i := range records {
		if _, err := storage.ApplyWALRecord(db, &records[i]); err != nil {
			return 0, fmt.Errorf("apply WAL record lsn=%d: %w", records[i].LSN, err)
		}
	}

	return len(records), nil
}

// runReplicaPollLoop repeatedly calls GetChangesSince against the primary
// at primaryAddr starting at fromLSN, applying every record returned (via
// pollChangesSinceOnce) and advancing its resume point to each call's
// resume LSN, until ctx is done or a poll signals
// errReplicaNeedsRebootstrap, in which case it returns that error
// immediately -- it is runReplicaLoop's job to act on it, not this
// function's, so it never retries or backs off on that specific signal.
//
// fromEpoch is the epoch the caller's bootstrap returned; every poll
// compares the primary's current epoch against it (see
// pollChangesSinceOnce).
//
// Backoff: a poll that returns zero records waits before polling again,
// starting at 25ms and doubling on every further empty/failed poll up to a
// 1s ceiling; a poll that returns any records resets the wait back to 25ms
// and polls again immediately (no wait), so an active replica drains a
// burst of primary writes as fast as it can rather than pacing itself at
// the idle interval. A transient RPC error (network blip, primary
// momentarily unreachable) is treated the same as an empty poll for
// backoff purposes and retried rather than aborting the loop -- only ctx
// being done, or errReplicaNeedsRebootstrap, ends it.
func runReplicaPollLoop(ctx context.Context, db *storage.DB, primaryAddr, tenant string, fromLSN, fromEpoch uint64, opts replicaOptions) error {
	conn, err := dialPeerGRPC(primaryAddr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		return fmt.Errorf("connect to primary %s: %w", primaryAddr, err)
	}
	defer func() { _ = conn.Close() }()

	sinceLSN := fromLSN
	backoff := replicaMinPollBackoff

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		applied, resumeLSN, pollErr := pollChangesSinceOnce(ctx, conn, tenant, sinceLSN, fromEpoch, db, opts)
		if pollErr != nil {
			if errors.Is(pollErr, errReplicaNeedsRebootstrap) {
				return errReplicaNeedsRebootstrap
			}
			if !replicaSleep(ctx, backoff) {
				return ctx.Err()
			}
			backoff = replicaNextBackoff(backoff)
			continue
		}
		sinceLSN = resumeLSN

		if applied > 0 {
			backoff = replicaMinPollBackoff
			continue
		}

		if !replicaSleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = replicaNextBackoff(backoff)
	}
}

// openChangesStream opens one GetChanges stream (the streaming counterpart
// of GetChangesSince -- see _TinySQL_GetChanges_Handler in main.go) against
// conn and sends the initial request (Tenant, SinceLSN) the handler expects
// as the very first message on the stream. It uses the same
// jsonCodec-forced dial (conn is expected to come from dialPeerGRPC) and
// the same per-call timeout/auth-metadata handling (replicaCallContext) as
// every other RPC in this file.
//
// The caller drives the stream onward via recvChangesOnce and is
// responsible for calling the returned cancel func once done with it
// (typically deferred) to release the per-call context; conn itself is
// owned by the caller and is not touched here.
func openChangesStream(ctx context.Context, conn *grpc.ClientConn, tenant string, sinceLSN uint64, opts replicaOptions) (grpc.ClientStream, context.CancelFunc, error) {
	callCtx, cancel := replicaCallContext(ctx, opts)

	desc := &grpc.StreamDesc{StreamName: "GetChanges", ServerStreams: true}
	stream, err := conn.NewStream(callCtx, desc, "/tinysql.TinySQL/GetChanges")
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("open GetChanges stream: %w", err)
	}
	if err := stream.SendMsg(&getChangesSinceRequest{Tenant: tenant, SinceLSN: sinceLSN}); err != nil {
		cancel()
		return nil, nil, fmt.Errorf("send initial GetChanges request: %w", err)
	}
	return stream, cancel, nil
}

// recvChangesOnce reads exactly one getChangesSinceResponse pushed by the
// primary over stream, decodes and applies every WAL record it carries (via
// applyChangesResponse) to db in order, and returns how many were applied
// plus the resume LSN to pass as sinceLSN the next time a stream is opened.
// This is the streaming transport's equivalent of a single
// pollChangesSinceOnce call -- same epoch check, same
// codes.OutOfRange/errReplicaNeedsRebootstrap mapping (see
// pollChangesSinceOnce's doc comment for why both that and an epoch
// mismatch map to the same signal) -- except the "request" side already
// happened once, in openChangesStream, rather than on every call.
func recvChangesOnce(stream grpc.ClientStream, expectedEpoch uint64, db *storage.DB) (applied int, resumeLSN uint64, err error) {
	var resp getChangesSinceResponse
	if err := stream.RecvMsg(&resp); err != nil {
		if status.Code(err) == codes.OutOfRange {
			return 0, 0, errReplicaNeedsRebootstrap
		}
		return 0, 0, fmt.Errorf("recv GetChanges: %w", err)
	}

	if resp.Epoch != expectedEpoch {
		return 0, 0, errReplicaNeedsRebootstrap
	}

	applied, err = applyChangesResponse(db, &resp)
	if err != nil {
		return 0, 0, err
	}
	return applied, resp.ResumeLSN, nil
}

// streamChangesOnce opens one GetChanges stream against conn starting at
// sinceLSN and applies every record from every response the primary pushes
// (via recvChangesOnce, in a loop) until the stream itself ends: ctx being
// done, errReplicaNeedsRebootstrap (the primary reported codes.OutOfRange,
// or its epoch no longer matches expectedEpoch), or any other transport
// error terminates it. It returns the last LSN it successfully advanced to,
// so runReplicaStreamLoop can open a fresh stream resuming from exactly
// there rather than from sinceLSN again.
func streamChangesOnce(ctx context.Context, conn *grpc.ClientConn, tenant string, sinceLSN, expectedEpoch uint64, db *storage.DB, opts replicaOptions) (resumeLSN uint64, err error) {
	stream, cancel, err := openChangesStream(ctx, conn, tenant, sinceLSN, opts)
	if err != nil {
		return sinceLSN, err
	}
	defer cancel()

	resumeLSN = sinceLSN
	for {
		_, next, err := recvChangesOnce(stream, expectedEpoch, db)
		if err != nil {
			return resumeLSN, err
		}
		resumeLSN = next
	}
}

// runReplicaStreamLoop is the gRPC server-streaming counterpart of
// runReplicaPollLoop: instead of repeatedly invoking a unary GetChangesSince
// RPC and sleeping between calls itself, it opens one long-lived GetChanges
// stream (via streamChangesOnce) and lets the primary push new committed
// WAL records as soon as they exist, backing off server-side with the same
// schedule when there is nothing new (see _TinySQL_GetChanges_Handler in
// main.go). This is a transport difference only: replication semantics --
// application order, and errReplicaNeedsRebootstrap's two triggers -- are
// identical to runReplicaPollLoop, which is what lets runReplicaLoop (via
// runReplicaLoopWithTransport) treat the two loops interchangeably.
//
// A transient error opening or reading from the stream (network blip,
// primary momentarily unreachable) is retried with the same backoff
// schedule runReplicaPollLoop uses for a failed poll, by opening a fresh
// stream (a fresh streamChangesOnce call) resuming from the last
// successfully applied LSN, rather than aborting the loop -- only ctx being
// done, or errReplicaNeedsRebootstrap, ends it.
func runReplicaStreamLoop(ctx context.Context, db *storage.DB, primaryAddr, tenant string, fromLSN, fromEpoch uint64, opts replicaOptions) error {
	conn, err := dialPeerGRPC(primaryAddr, opts.MaxRecvMsgSize, opts.TransportCreds)
	if err != nil {
		return fmt.Errorf("connect to primary %s: %w", primaryAddr, err)
	}
	defer func() { _ = conn.Close() }()

	sinceLSN := fromLSN
	backoff := replicaMinPollBackoff

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		resumeLSN, streamErr := streamChangesOnce(ctx, conn, tenant, sinceLSN, fromEpoch, db, opts)
		sinceLSN = resumeLSN

		if streamErr == nil {
			// streamChangesOnce's own loop only returns a nil error if it
			// never entered -- which cannot happen -- but treat it the same
			// as ctx ending defensively rather than spinning on a nil error.
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		if errors.Is(streamErr, errReplicaNeedsRebootstrap) {
			return errReplicaNeedsRebootstrap
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !replicaSleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = replicaNextBackoff(backoff)
	}
}

// replicaChangesLoopFunc is the shape shared by runReplicaPollLoop and
// runReplicaStreamLoop: given a bootstrapped db and the watermark/epoch
// that bootstrap returned, apply every committed WAL record from fromLSN
// onward until ctx is done or errReplicaNeedsRebootstrap. Factoring this out
// lets runReplicaLoopWithTransport (and tests) select the transport without
// duplicating the bootstrap/rebootstrap orchestration below.
type replicaChangesLoopFunc func(ctx context.Context, db *storage.DB, primaryAddr, tenant string, fromLSN, fromEpoch uint64, opts replicaOptions) error

// runReplicaLoop bootstraps an in-memory copy of the primary at primaryAddr
// and polls for and applies committed WAL changes until ctx is done,
// transparently discarding that copy and bootstrapping again from scratch
// on any rebootstrap signal -- see runReplicaLoopWithTransport, which this
// is a thin wrapper around using runReplicaStreamLoop (gRPC
// server-streaming, the default transport -replica-of uses; see
// flagReplicaTransport) as its changes-loop implementation.
func runReplicaLoop(ctx context.Context, primaryAddr, tenant string, opts replicaOptions, onBootstrap func(*storage.DB)) (*storage.DB, error) {
	return runReplicaLoopWithTransport(ctx, primaryAddr, tenant, opts, onBootstrap, runReplicaStreamLoop)
}

// runReplicaLoopWithTransport bootstraps an in-memory copy of the primary at
// primaryAddr (via runReplicaBootstrap) and then applies committed WAL
// changes via changesLoop -- runReplicaPollLoop (unary polling) or
// runReplicaStreamLoop (gRPC server-streaming); both share the
// replicaChangesLoopFunc shape and are otherwise interchangeable here --
// until ctx is done, transparently discarding that copy and bootstrapping
// again from scratch whenever changesLoop returns errReplicaNeedsRebootstrap
// instead of surfacing that as a fatal error -- this is the
// checkpoint-outran-replica and WAL-epoch-mismatch safety net's actual
// recovery action (see errReplicaNeedsRebootstrap's doc comment for the two
// conditions that trigger it), identical regardless of which changesLoop is
// in use.
//
// onBootstrap, if non-nil, is called with the freshly bootstrapped DB every
// time this (re-)bootstraps, before it starts applying changes --
// runReplica passes nil; tests use it to observe (and deterministically
// sequence around) each bootstrap, including any rebootstrap.
//
// It returns whichever in-memory *storage.DB the most recent bootstrap
// produced -- still valid and current even when ctx ending is what stopped
// the loop -- together with whatever error ended it (ctx.Err() in the
// normal case). The caller owns closing the returned DB.
func runReplicaLoopWithTransport(ctx context.Context, primaryAddr, tenant string, opts replicaOptions, onBootstrap func(*storage.DB), changesLoop replicaChangesLoopFunc) (*storage.DB, error) {
	for {
		db, watermark, epoch, err := runReplicaBootstrap(ctx, primaryAddr, tenant, opts)
		if err != nil {
			return nil, err
		}
		log.Printf("replica bootstrapped from %s at watermark lsn=%d epoch=%d", primaryAddr, watermark, epoch)
		if onBootstrap != nil {
			onBootstrap(db)
		}

		err = changesLoop(ctx, db, primaryAddr, tenant, watermark, epoch, opts)
		if errors.Is(err, errReplicaNeedsRebootstrap) {
			log.Printf("replica fell out of sync with primary %s, re-bootstrapping", primaryAddr)
			_ = db.Close()
			continue
		}
		return db, err
	}
}

// replicaChangesLoopFor maps a -replica-transport flag value to the
// changesLoop implementation runReplicaLoopWithTransport should use --
// "stream" (default) for runReplicaStreamLoop, "poll" for
// runReplicaPollLoop. See flagReplicaTransport's doc comment.
func replicaChangesLoopFor(transport string) (replicaChangesLoopFunc, error) {
	switch strings.ToLower(strings.TrimSpace(transport)) {
	case "", "stream":
		return runReplicaStreamLoop, nil
	case "poll":
		return runReplicaPollLoop, nil
	default:
		return nil, fmt.Errorf("invalid -replica-transport %q (valid: stream, poll)", transport)
	}
}

// replicaNextBackoff doubles d, capped at replicaMaxPollBackoff.
func replicaNextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > replicaMaxPollBackoff {
		return replicaMaxPollBackoff
	}
	return d
}

// replicaSleep waits for d or until ctx is done, whichever comes first, and
// reports whether the full wait elapsed (false means ctx ended it early).
func replicaSleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// runReplica implements the replica-side CLI mode selected by -replica-of:
// bootstrap a snapshot from the primary at replicaOf, then poll for and
// apply committed WAL changes until the process receives SIGINT/SIGTERM. It
// reuses -auth and the -peer-tls* flags for the replica-to-primary
// connection, the same way federation peer calls (grpcQuery) do.
//
// The replica's database lives entirely in memory for v1: it has no WAL of
// its own and is never durable across restarts. Restarting the process
// simply re-bootstraps from the primary rather than resuming from local
// state -- replica-side durability and serving the replicated data back out
// over HTTP/gRPC are both explicitly out of scope for this stage.
func runReplica(replicaOf string) error {
	tenant := strings.TrimSpace(*flagTenant)
	if tenant == "" {
		tenant = "default"
	}

	changesLoop, err := replicaChangesLoopFor(*flagReplicaTransport)
	if err != nil {
		return err
	}

	minTLSVersion, err := parseTLSMinVersion(*flagTLSMinVersion)
	if err != nil {
		return err
	}
	if err := validateRunPeerTLSFlags(); err != nil {
		return err
	}
	peerDialCreds, err := buildRunPeerDialCreds(minTLSVersion)
	if err != nil {
		return err
	}

	opts := replicaOptions{
		AuthToken:      strings.TrimSpace(*flagAuth),
		CallTimeout:    *flagPeerTimeout,
		MaxRecvMsgSize: *flagGRPCMaxRecv,
		TransportCreds: peerDialCreds,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := runReplicaLoopWithTransport(ctx, replicaOf, tenant, opts, nil, changesLoop)
	if db != nil {
		defer func() { _ = db.Close() }()
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("replica loop: %w", err)
	}
	return nil
}
