package importer

import (
	"context"
	"errors"
	"io"
)

// ErrInputTooLarge is returned before an importer consumes data beyond the
// configured ImportOptions.MaxInputBytes limit.
var ErrInputTooLarge = errors.New("import input exceeds configured size limit")

// limitInput makes cancellation and source-size limits apply uniformly to
// streaming import readers. It intentionally does not buffer input.
func limitInput(ctx context.Context, src io.Reader, opts *ImportOptions) io.Reader {
	var r io.Reader = contextReader{ctx: ctx, r: src}
	if opts != nil && opts.MaxInputBytes > 0 {
		r = &maxBytesReader{r: r, remaining: opts.MaxInputBytes}
	}
	return r
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		default:
		}
	}
	return r.r.Read(p)
}

type maxBytesReader struct {
	r         io.Reader
	remaining int64
}

func (r *maxBytesReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		var probe [1]byte
		n, err := r.r.Read(probe[:])
		if n > 0 {
			return 0, ErrInputTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	return n, err
}
