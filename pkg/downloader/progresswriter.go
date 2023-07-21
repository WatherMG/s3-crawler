package downloader

import (
	"io"
)

type progressWriterAt struct {
	writer   io.WriterAt
	callback func(n int64)
}

func (pw *progressWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := pw.writer.WriteAt(p, off)
	if err == nil {
		bytes := int64(n)
		pw.callback(bytes)
	}
	return n, err
}
