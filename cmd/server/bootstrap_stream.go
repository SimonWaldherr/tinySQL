package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const bootstrapChunkBytes = 64 * 1024

func (s *server) acquireBootstrap(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.bootstrapSem == nil {
		return func() {}, nil
	}
	select {
	case s.bootstrapSem <- struct{}{}:
		return func() { <-s.bootstrapSem }, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

type bootstrapChunk struct {
	Data         []byte `json:"data,omitempty" protobuf:"1"`
	WatermarkLSN uint64 `json:"watermark_lsn,omitempty" protobuf:"2"`
	Epoch        uint64 `json:"epoch,omitempty" protobuf:"3"`
	Complete     bool   `json:"complete,omitempty" protobuf:"4"`
}

func _TinySQL_BootstrapStream_Handler(srv any, stream grpc.ServerStream) (err error) {
	s, ok := srv.(*server)
	if !ok {
		return status.Error(codes.Internal, "unexpected server type")
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("panic in gRPC BootstrapStream: %v", rec)
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	if err := s.authorizeGRPCStream(stream.Context()); err != nil {
		return err
	}
	var req bootstrapRequest
	if err := stream.RecvMsg(&req); err != nil {
		return err
	}
	chunkSize := min(bootstrapChunkBytes, *flagGRPCMaxSend-128)
	if req.ChunkBytes < 0 {
		return status.Error(codes.InvalidArgument, "chunk_bytes must be nonnegative")
	}
	if req.ChunkBytes > 0 {
		chunkSize = int(min(int64(chunkSize), req.ChunkBytes))
	}
	if chunkSize < 1 {
		return status.Error(codes.ResourceExhausted, "gRPC message limit is too small for snapshot chunks")
	}
	ctx, cancel := s.replicationContext(stream.Context())
	defer cancel()
	release, err := s.acquireBootstrap(ctx)
	if err != nil {
		return err
	}
	defer release()
	unlock, err := s.acquireDatabase()
	if err != nil {
		return err
	}
	defer unlock()
	wal := s.db.AdvancedWAL()
	if wal == nil {
		return status.Error(codes.FailedPrecondition, errNoAdvancedWAL.Error())
	}
	w := &bootstrapChunkWriter{ctx: ctx, stream: stream, size: chunkSize}
	watermark, epoch, err := storage.WriteReplicationSnapshot(ctx, s.db, wal, w)
	if err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Errorf(codes.Internal, "stream snapshot: %v", err)
	}
	if err := w.flush(); err != nil {
		return err
	}
	return stream.SendMsg(&bootstrapChunk{Complete: true, WatermarkLSN: watermark, Epoch: epoch})
}

type bootstrapChunkWriter struct {
	ctx    context.Context
	stream grpc.ServerStream
	size   int
	buffer []byte
}

func (w *bootstrapChunkWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if err := w.ctx.Err(); err != nil {
			return written, err
		}
		if w.buffer == nil {
			w.buffer = make([]byte, 0, w.size)
		}
		n := min(len(p), w.size-len(w.buffer))
		w.buffer = append(w.buffer, p[:n]...)
		p = p[n:]
		written += n
		if len(w.buffer) == w.size {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *bootstrapChunkWriter) flush() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if len(w.buffer) == 0 {
		return nil
	}
	if err := w.stream.SendMsg(&bootstrapChunk{Data: w.buffer}); err != nil {
		return err
	}
	w.buffer = nil // sent messages retain independent payload ownership
	return nil
}

type bootstrapChunkReader struct {
	stream  grpc.ClientStream
	data    []byte
	final   bootstrapChunk
	started bool
}

func (r *bootstrapChunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.data) == 0 {
		if r.final.Complete {
			return 0, io.EOF
		}
		var chunk bootstrapChunk
		if err := r.stream.RecvMsg(&chunk); err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		r.started = true
		if chunk.Complete {
			if len(chunk.Data) != 0 || chunk.Epoch == 0 {
				return 0, errors.New("invalid bootstrap completion")
			}
			var extra bootstrapChunk
			if err := r.stream.RecvMsg(&extra); !errors.Is(err, io.EOF) {
				if err != nil {
					return 0, err
				}
				return 0, errors.New("unexpected bootstrap message after completion")
			}
			r.final = chunk
			return 0, io.EOF
		}
		if len(chunk.Data) == 0 || chunk.Epoch != 0 || chunk.WatermarkLSN != 0 {
			return 0, errors.New("invalid bootstrap data chunk")
		}
		r.data = chunk.Data
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func bootstrapFromStream(ctx context.Context, conn *grpc.ClientConn, tenant string, opts replicaOptions) (*storage.DB, uint64, uint64, error) {
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/tinysql.TinySQL/BootstrapStream")
	if err != nil {
		return nil, 0, 0, err
	}
	size := bootstrapChunkBytes
	if opts.MaxRecvMsgSize > 0 {
		size = min(size, opts.MaxRecvMsgSize-128)
	}
	if size < 1 {
		return nil, 0, 0, fmt.Errorf("gRPC receive limit is too small for bootstrap chunks")
	}
	if err := stream.SendMsg(&bootstrapRequest{Tenant: tenant, ChunkBytes: int64(size)}); err != nil {
		return nil, 0, 0, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, 0, 0, err
	}
	reader := &bootstrapChunkReader{stream: stream}
	db, err := storage.LoadReplicationSnapshot(ctx, reader)
	if err != nil {
		// Only an old server with no streaming method permits unary fallback.
		if reader.started && status.Code(err) == codes.Unimplemented {
			err = fmt.Errorf("incomplete bootstrap stream: %v", err)
		}
		return nil, 0, 0, err
	}
	return db, reader.final.WatermarkLSN, reader.final.Epoch, nil
}
