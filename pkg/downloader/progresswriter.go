package downloader

import (
	"io"
	"sync/atomic"
)

type progressWriterAt struct {
	writer   io.WriterAt
	progress int64
	fileSize int64
	callback func(n int64)
}

func (pw *progressWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := pw.writer.WriteAt(p, off)
	if err == nil {
		atomic.AddInt64(&pw.progress, int64(n))
		pw.callback(int64(n))
	}
	return n, err
}
