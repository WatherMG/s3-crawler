package archives

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"s3-crawler/pkg/files"
	"s3-crawler/pkg/utils"
)

var supportedArchiveExtensions = []string{".gz", ".gzip"}

type Archive interface {
	Decompress(destination string) error
}

type Gzip struct {
	Path string
}
type Tar struct {
	Path string
}
type Zip struct {
	Path string
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		buffer := make([]byte, files.Buffer32KB)
		return &buffer
	},
}

func (g Gzip) Decompress(destination string) error {
	filename := filepath.Base(g.Path)
	target := filepath.Join(destination, strings.TrimSuffix(filename, filepath.Ext(filename))+"_unpacked")

	reader, err := os.Open(g.Path)
	if err != nil {
		return err
	}
	defer reader.Close()

	archive, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer archive.Close()

	writer, err := os.Create(target)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Get a buffer from the pool
	buffer := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(buffer)

	_, err = io.CopyBuffer(writer, archive, *buffer)
	return err
}

func (t Tar) Decompress(destination string) error {
	filename := filepath.Base(t.Path)
	target := filepath.Join(destination, "decompressed", strings.TrimSuffix(filename, filepath.Ext(filename)))
	fmt.Println(target)
	return nil
}

func (z Zip) Decompress(destination string) error {
	filename := filepath.Base(z.Path)
	target := filepath.Join(destination, "decompressed", strings.TrimSuffix(filename, filepath.Ext(filename)))
	fmt.Println(target)
	return nil
}

func DecompressFile(path, destination string) error {
	var archive Archive
	if err := utils.CreatePath(destination); err != nil {
		return err
	}
	switch filepath.Ext(path) {
	case ".gz", ".gzip":
		archive = Gzip{Path: path}
		return archive.Decompress(destination)
	default:
		return fmt.Errorf("unsupported archive type")
	}
}

func IsSupportedArchive(name string) bool {
	return utils.HasValidExtension(name, supportedArchiveExtensions)
}
