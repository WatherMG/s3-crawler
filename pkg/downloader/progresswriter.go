package downloader

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type progressWriterAt struct {
	writer   io.WriterAt
	callback func(bytes int64)
	total    int64
	written  int64
}

func NewProgressWriterAt(writer io.WriterAt, total int64, progressCallback func(int64)) io.WriterAt {
	pw := &progressWriterAt{
		writer:   writer,
		callback: progressCallback,
		total:    total,
	}
	return pw
}

func (pw *progressWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := pw.writer.WriteAt(p, off)
	if err == nil || errors.Is(err, io.EOF) {
		bytes := int64(n)
		pw.written += bytes
		pw.callback(bytes)
	}
	return n, err
}

func (pw *progressWriterAt) Close() error {
	if pw.written != pw.total {
		if closer, ok := pw.writer.(io.Closer); ok {
			return closer.Close()
		}
		if err := os.Remove(pw.writer.(interface{ Name() string }).Name()); err != nil {
			return err
		}
		return fmt.Errorf("written bytes not equal file size")
	}
	if closer, ok := pw.writer.(io.Closer); ok {
		return closer.Close()
	}
	pw.writer = nil
	return nil
}

func (pw *progressWriterAt) BytesWritten() int {
	return int(pw.written)
}
