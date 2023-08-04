package archives

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"s3-crawler/pkg/files"
	"s3-crawler/pkg/utils"
)

var supportedArchiveExtensions = []string{".gz", ".gzip"}

type Archiver interface {
	decompress(file *files.File, data *files.FileCollection) error
}

type Gzip struct {
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

func (g *Gzip) decompress(file *files.File, data *files.FileCollection) error {
	compressedData := getBuffer()
	defer putBuffer(compressedData)
	compressedData.Write(file.Data.Bytes())

	archive, err := gzip.NewReader(compressedData)
	if err != nil {
		return fmt.Errorf("gzip reader error: %w", err)
	}
	defer archive.Close()

	buffer := getBuffer()
	defer putBuffer(buffer)

	if _, err = io.Copy(buffer, archive); err != nil {
		return err
	}

	file.Data.Reset()
	file.Data.Grow(buffer.Len())
	if _, err = buffer.WriteTo(file.Data); err != nil {
		return err
	}
	data.DataChan <- file

	return nil
}

// ProcessFile выбирает функцию для работы декомпрессора в зависимости от типа файла.
func ProcessFile(file *files.File, data *files.FileCollection) error {
	var archive Archiver
	switch file.Extension {
	case ".gz", ".gzip":
		archive = &Gzip{}
		return archive.decompress(file, data)
	default:
		return fmt.Errorf("unsupported archive type")
	}
}

func IsSupportedArchive(name string) bool {
	return utils.HasValidExtension(name, supportedArchiveExtensions)
}
