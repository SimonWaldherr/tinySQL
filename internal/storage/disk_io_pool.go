package storage

import (
	"bufio"
	"compress/gzip"
	"io"
	"sync"
)

// Fixed-size transport buffers and compression state may be reused across
// backends. Each operation owns its borrowed object until it has finished all
// I/O; returned buffers never back decoded table values. Pools are disposable
// under GC and do not change flush, fsync or atomic rename ordering.
var diskReaderPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, 64*1024) }}
var diskWriterPool = sync.Pool{New: func() any { return bufio.NewWriterSize(io.Discard, 64*1024) }}
var diskGzipWriterPool = sync.Pool{New: func() any { return gzip.NewWriter(io.Discard) }}

func borrowDiskReader(source io.Reader) *bufio.Reader {
	reader := diskReaderPool.Get().(*bufio.Reader)
	reader.Reset(source)
	return reader
}
func releaseDiskReader(reader *bufio.Reader) {
	reader.Reset(nil)
	diskReaderPool.Put(reader)
}
func borrowDiskWriter(target io.Writer) *bufio.Writer {
	writer := diskWriterPool.Get().(*bufio.Writer)
	writer.Reset(target)
	return writer
}
func releaseDiskWriter(writer *bufio.Writer) {
	writer.Reset(io.Discard)
	diskWriterPool.Put(writer)
}
func borrowDiskGzipWriter(target io.Writer) *gzip.Writer {
	writer := diskGzipWriterPool.Get().(*gzip.Writer)
	writer.Reset(target)
	return writer
}
func releaseDiskGzipWriter(writer *gzip.Writer) {
	// Detach the file or encryption buffer, including after a failed encode.
	// Close/Flush and their errors are handled by the owning operation first.
	writer.Reset(io.Discard)
	diskGzipWriterPool.Put(writer)
}
