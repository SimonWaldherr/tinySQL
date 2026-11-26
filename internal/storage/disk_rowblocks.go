// The row-block table file layout (StorageConfig.RowBlockFiles).
//
// The classic table file is ONE gob message holding the whole diskTable. The
// encoder builds that message completely in memory before it writes a byte
// (peak RAM of a few times the table), on one core, and encoding/gob refuses
// messages of 8 GiB or more in both directions, so a table that grows past that
// can neither be saved nor loaded.
//
// The row-block layout keeps every row out of any single message:
//
//	magic    8 bytes   "\x00TSQLRB1"; no gob stream starts with a zero byte
//	flags    1 byte    bit 0: header and blocks are gzip-compressed; unknown
//	                   bits make the file unreadable (it comes from a newer writer)
//	header   frame     gob(diskRowBlockHeader): everything but the rows (schema,
//	                   secondary indexes, FTS/ANN images, statistics) plus the
//	                   number of rows of each block
//	block 0  frame     gob(diskRowBlock): an independent gob stream, about
//	...                rowBlockTargetBytes of rows
//	block n-1 frame
//
// A frame is  uint64 payload length (LE) | uint32 checksum (LE) | payload. The
// checksum is CRC-32C over a binding followed by the payload: for the header the
// magic and flags byte, for block i the header's checksum and the index i. A
// damaged byte anywhere is caught by its own frame; blocks that were reordered
// or taken from another version of the file (a different header) fail their
// binding even though each is intact. Blocks are encoded by up to GOMAXPROCS
// workers into ordered buffers and written in order, so the bytes of the row
// blocks do not depend on the worker count; they are decoded in parallel on
// load. The block boundaries depend only on the rows (an estimate of their
// bytes, and a cap on the rows per block), never on the workers.
//
// The bounds hold for the ROWS. The header is still one gob message and holds
// the secondary indexes and, unless StorageConfig.SkipSearchIndexPersistence is
// set, the FTS/ANN images, so for a table whose images are large the 8 GiB
// message limit and the whole-message memory of the save apply to the header (and
// a table in the classic layout that already crossed the limit cannot be loaded
// to be converted: switch the layout on before the table gets there).
//
// Files in this layout cannot be read by binaries that predate it: they fail on
// the leading zero byte. Readers of this version accept both layouts and pick
// by the magic, whatever the configuration says.
package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

var rowBlockMagic = [8]byte{0, 'T', 'S', 'Q', 'L', 'R', 'B', '1'}

const rowBlockFlagGzip = 1

// rowBlockTargetBytes is the estimated size of the rows of one block, and
// rowBlockWorkers the number of encode/decode workers. Variables so that tests
// can force many tiny blocks and fixed worker counts.
var (
	rowBlockTargetBytes = 32 << 20
	// rowBlockMaxRows caps the rows of one block whatever their estimated size,
	// so that a table of rows the estimate undervalues cannot put its whole
	// content into one message.
	rowBlockMaxRows = 1 << 18
	rowBlockWorkers = func() int { return runtime.GOMAXPROCS(0) }
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// diskRowBlockHeader is the header frame's content: the table without its rows.
type diskRowBlockHeader struct {
	Table     diskTable // Rows is always nil
	BlockRows []int
}

// diskRowBlock is one block frame's content.
type diskRowBlock struct {
	Rows [][]any
}

// estimateRowBytes is a cheap, deterministic estimate of a row's encoded size,
// used only to choose block boundaries.
func estimateRowBytes(row []any) int {
	n := 8
	for _, v := range row {
		n += estimateCellBytes(v, 0)
	}
	return n
}

// estimateCellBytes estimates one cell. Nested values (a []any or map[string]any
// cell) are walked to a bounded depth, so a row that holds a large nested value
// is not taken for a small one; the walk is deterministic (a sum), whatever the
// map iteration order.
func estimateCellBytes(v any, depth int) int {
	switch x := v.(type) {
	case nil:
		return 1
	case string:
		return len(x) + 2
	case []float64:
		return 8*len(x) + 4
	case []byte:
		return len(x) + 2
	case []any:
		n := 8
		if depth >= 4 {
			return n + 64
		}
		for _, e := range x {
			n += estimateCellBytes(e, depth+1)
		}
		return n
	case map[string]any:
		n := 8
		if depth >= 4 {
			return n + 64
		}
		for k, e := range x {
			n += len(k) + 2 + estimateCellBytes(e, depth+1)
		}
		return n
	default:
		return 9
	}
}

type rowSpan struct{ start, end int }

// splitRowBlocks cuts rows into runs of about target estimated bytes, at least
// one row and at most rowBlockMaxRows rows each.
func splitRowBlocks(rows [][]any, target int) []rowSpan {
	var spans []rowSpan
	start, acc := 0, 0
	for i, r := range rows {
		acc += estimateRowBytes(r)
		if acc >= target || i+1-start >= rowBlockMaxRows {
			spans = append(spans, rowSpan{start, i + 1})
			start, acc = i+1, 0
		}
	}
	if start < len(rows) {
		spans = append(spans, rowSpan{start, len(rows)})
	}
	return spans
}

// encodeFramePayload gob-encodes v, gzip-compressed when asked.
func encodeFramePayload(v any, compress bool) ([]byte, error) {
	var buf bytes.Buffer
	if !compress {
		if err := gob.NewEncoder(&buf).Encode(v); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	gz := gzip.NewWriter(&buf)
	if err := gob.NewEncoder(gz).Encode(v); err != nil {
		_ = gz.Close()
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// frameChecksum is the CRC-32C of binding followed by payload.
func frameChecksum(binding, payload []byte) uint32 {
	return crc32.Update(crc32.Checksum(binding, crc32cTable), crc32cTable, payload)
}

// blockBinding is the binding of block idx: the header frame's checksum and the
// block's position, so a block cannot be moved or taken from another file.
func blockBinding(headerCRC uint32, idx int) []byte {
	var b [12]byte
	binary.LittleEndian.PutUint32(b[0:4], headerCRC)
	binary.LittleEndian.PutUint64(b[4:12], uint64(idx))
	return b[:]
}

// writeFrame writes one frame whose checksum covers binding and payload, and
// returns that checksum.
func writeFrame(w io.Writer, binding, payload []byte) (uint32, error) {
	var head [12]byte
	crc := frameChecksum(binding, payload)
	binary.LittleEndian.PutUint64(head[0:8], uint64(len(payload)))
	binary.LittleEndian.PutUint32(head[8:12], crc)
	if _, err := w.Write(head[:]); err != nil {
		return 0, err
	}
	_, err := w.Write(payload)
	return crc, err
}

// writeRowBlocks writes dt to tmp in the row-block layout and makes it durable.
// dt holds the table's original rows. On error tmp is removed.
func (c tableFileConfig) writeRowBlocks(tmp string, dt diskTable) error {
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	durable := false
	defer func() {
		if !durable {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	target, err := testFileTarget(f, tmp)
	if err != nil {
		return err
	}
	bw := borrowDiskWriter(target)
	defer releaseDiskWriter(bw)

	spans := splitRowBlocks(dt.Rows, rowBlockTargetBytes)
	rows := dt.Rows
	hdr := diskRowBlockHeader{Table: dt, BlockRows: make([]int, len(spans))}
	hdr.Table.Rows = nil
	for i, s := range spans {
		hdr.BlockRows[i] = s.end - s.start
	}
	flags := byte(0)
	if c.gzip {
		flags = rowBlockFlagGzip
	}
	prefix := append(append([]byte(nil), rowBlockMagic[:]...), flags)
	if _, err = bw.Write(prefix); err != nil {
		return err
	}
	payload, err := encodeFramePayload(hdr, c.gzip)
	if err != nil {
		return fmt.Errorf("encode table header: %w", err)
	}
	headerCRC, err := writeFrame(bw, prefix, payload)
	if err != nil {
		return err
	}
	payload = nil

	hasJSON := hasDiskJSONColumns(dt.Cols)
	prepareBlock := func(i int) [][]any {
		block := rows[spans[i].start:spans[i].end]
		if hasJSON {
			block = diskRowsFromTable(dt.Cols, block)
		}
		return block
	}
	encodeBlock := func(_ int, block [][]any) ([]byte, error) {
		return encodeFramePayload(diskRowBlock{Rows: block}, c.gzip)
	}
	workers := min(rowBlockWorkers(), len(spans))
	if workers <= 1 {
		for i := range spans {
			p, e := encodeBlock(i, prepareBlock(i))
			if e != nil {
				return fmt.Errorf("encode row block %d: %w", i, e)
			}
			if _, err = writeFrame(bw, blockBinding(headerCRC, i), p); err != nil {
				return err
			}
		}
	} else if err = writeBlocksParallel(bw, headerCRC, len(spans), workers, prepareBlock, encodeBlock); err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	durable = true
	return nil
}

type blockResult struct {
	payload []byte
	err     error
}

// writeBlocksParallel prepares and writes blocks in order on the caller's
// goroutine, keeping custom JSON marshaling and its panics there. Workers
// encode at most 2*workers blocks ahead and finish before the call unwinds.
func writeBlocksParallel(w io.Writer, headerCRC uint32, n, workers int, prepare func(int) [][]any, encode func(int, [][]any) ([]byte, error)) error {
	type encodeJob struct {
		idx  int
		rows [][]any
	}
	window := min(n, 2*workers)
	results := make([]chan blockResult, window)
	for i := range results {
		results[i] = make(chan blockResult, 1)
	}
	jobs := make(chan encodeJob)
	var wg sync.WaitGroup
	for k := 0; k < workers; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				p, err := encode(job.idx, job.rows)
				results[job.idx%window] <- blockResult{p, err}
			}
		}()
	}
	defer func() {
		close(jobs)
		// Each submitted job owns a buffered result slot, including on error.
		wg.Wait()
	}()
	var pending encodeJob
	prepared := false
	for next, written := 0, 0; written < n; {
		var submit chan<- encodeJob
		var result <-chan blockResult
		if next < n && next-written < window {
			if !prepared {
				pending = encodeJob{idx: next, rows: prepare(next)}
				prepared = true
			}
			submit = jobs
		}
		if written < next {
			result = results[written%window]
		}
		select {
		case submit <- pending:
			next++
			pending = encodeJob{}
			prepared = false
		case res := <-result:
			if res.err != nil {
				return fmt.Errorf("encode row block %d: %w", written, res.err)
			}
			if _, err := writeFrame(w, blockBinding(headerCRC, written), res.payload); err != nil {
				return err
			}
			written++
		}
	}
	return nil
}

// ──── reading ───────────────────────────────────────────────────────────

var errRowBlockCorrupt = errors.New("row-block table file is corrupt or truncated")

// hasRowBlockMagic reports whether the first bytes are the row-block magic.
func hasRowBlockMagic(b []byte) bool {
	return len(b) >= len(rowBlockMagic) && bytes.Equal(b[:len(rowBlockMagic)], rowBlockMagic[:])
}

// readFrame reads one frame and verifies its checksum against binding (see
// frameChecksum). It returns the payload and the checksum. The payload buffer
// is exact-sized for ordinary blocks and grows with the bytes that actually
// arrive for larger frames, so a corrupt length cannot make a short file
// allocate gigabytes.
func readFrame(r io.Reader, binding []byte) ([]byte, uint32, error) {
	const directFrameBytes = 64 << 20
	var head [12]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, fmt.Errorf("%w: missing frame", errRowBlockCorrupt)
		}
		return nil, 0, err
	}
	n := binary.LittleEndian.Uint64(head[0:8])
	want := binary.LittleEndian.Uint32(head[8:12])
	if n > 1<<40 {
		return nil, 0, fmt.Errorf("%w: implausible frame length %d", errRowBlockCorrupt, n)
	}
	var payload []byte
	if n <= directFrameBytes {
		// Buffer.ReadFrom can grow a filled buffer before the final EOF probe.
		payload = make([]byte, int(n))
		if read, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, 0, fmt.Errorf("%w: frame cut short (%d of %d bytes)", errRowBlockCorrupt, read, n)
			}
			return nil, 0, err
		}
	} else {
		var buf bytes.Buffer
		buf.Grow(directFrameBytes)
		if _, err := buf.ReadFrom(io.LimitReader(r, int64(n))); err != nil {
			return nil, 0, err
		}
		if uint64(buf.Len()) != n {
			return nil, 0, fmt.Errorf("%w: frame cut short (%d of %d bytes)", errRowBlockCorrupt, buf.Len(), n)
		}
		payload = buf.Bytes()
	}
	if frameChecksum(binding, payload) != want {
		return nil, 0, fmt.Errorf("%w: checksum mismatch", errRowBlockCorrupt)
	}
	return payload, want, nil
}

func decodeFramePayload(payload []byte, compressed bool, v any) error {
	var r io.Reader = bytes.NewReader(payload)
	if compressed {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gr.Close()
		r = gr
	}
	return gob.NewDecoder(r).Decode(v)
}

// decodeRowBlockFile reads a whole row-block file, magic included, from r.
func decodeRowBlockFile(r io.Reader) (diskTable, error) {
	var prefix [9]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return diskTable{}, fmt.Errorf("%w: %v", errRowBlockCorrupt, err)
	}
	if !hasRowBlockMagic(prefix[:]) {
		return diskTable{}, errors.New("not a row-block table file")
	}
	if prefix[8]&^byte(rowBlockFlagGzip) != 0 {
		return diskTable{}, fmt.Errorf("%w: unknown flags %#x (written by a newer version?)", errRowBlockCorrupt, prefix[8])
	}
	compressed := prefix[8]&rowBlockFlagGzip != 0

	payload, headerCRC, err := readFrame(r, prefix[:])
	if err != nil {
		return diskTable{}, err
	}
	var hdr diskRowBlockHeader
	if err := decodeFramePayload(payload, compressed, &hdr); err != nil {
		return diskTable{}, fmt.Errorf("%w: header: %v", errRowBlockCorrupt, err)
	}
	payload = nil
	offsets := make([]int, len(hdr.BlockRows)+1)
	for i, n := range hdr.BlockRows {
		if n < 0 || offsets[i]+n < offsets[i] {
			return diskTable{}, fmt.Errorf("%w: bad block row count", errRowBlockCorrupt)
		}
		offsets[i+1] = offsets[i] + n
	}
	total := offsets[len(hdr.BlockRows)]
	var rows [][]any
	if total > 0 {
		// The header's counts are trusted only as far as the blocks confirm
		// them; a corrupt header is caught by its checksum first.
		rows = make([][]any, total)
	}

	workers := min(rowBlockWorkers(), len(hdr.BlockRows))
	if workers < 1 {
		workers = 1
	}
	var (
		wg       sync.WaitGroup
		failed   atomic.Pointer[error]
		window   = make(chan struct{}, 2*workers)
		jobs     = make(chan blockJob)
		fail     = func(err error) { failed.CompareAndSwap(nil, &err) }
		decodeAt = func(idx int, data []byte) error {
			var blk diskRowBlock
			if err := decodeFramePayload(data, compressed, &blk); err != nil {
				return fmt.Errorf("%w: block %d: %v", errRowBlockCorrupt, idx, err)
			}
			if len(blk.Rows) != hdr.BlockRows[idx] {
				return fmt.Errorf("%w: block %d holds %d rows, header says %d", errRowBlockCorrupt, idx, len(blk.Rows), hdr.BlockRows[idx])
			}
			copy(rows[offsets[idx]:], blk.Rows)
			return nil
		}
	)
	for k := 0; k < workers; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if failed.Load() == nil {
					if err := decodeAt(j.idx, j.data); err != nil {
						fail(err)
					}
				}
				<-window
			}
		}()
	}
	var readErr error
	for i := range hdr.BlockRows {
		if failed.Load() != nil {
			break
		}
		data, _, err := readFrame(r, blockBinding(headerCRC, i))
		if err != nil {
			readErr = fmt.Errorf("block %d of %d: %w", i, len(hdr.BlockRows), err)
			break
		}
		window <- struct{}{}
		jobs <- blockJob{idx: i, data: data}
	}
	close(jobs)
	wg.Wait()
	if readErr != nil {
		return diskTable{}, readErr
	}
	if e := failed.Load(); e != nil {
		return diskTable{}, *e
	}
	var extra [1]byte
	if n, _ := r.Read(extra[:]); n != 0 {
		return diskTable{}, fmt.Errorf("%w: data after the last block", errRowBlockCorrupt)
	}
	dt := hdr.Table
	dt.Rows = rows
	return dt, nil
}

// blockJob is one frame payload waiting for a decode worker.
type blockJob struct {
	idx  int
	data []byte
}
