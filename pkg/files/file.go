package files

import (
	"bytes"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	Buffer8KB = 1 << (13 + iota)
	Buffer16KB
	Buffer32KB
	Buffer64KB
	Buffer128KB
	Buffer256KB
	Buffer512KB
	KiB = 1 << 10
	MiB = 1 << 20

	decompressedDir    = "decompressed"
	decompressedSuffix = ""
	delimiter          = '_'
)

// archives is a map of known archive extensions.
var archives map[string]bool

func init() {
	archives = map[string]bool{
		".zip":  false,
		".rar":  false,
		".tar":  false,
		".gz":   true,
		".gzip": true,
		".bz2":  false,
		".7z":   false,
	}
}

// File represents a file with a Key, Size, and ETag.
type File struct {
	Data        *Data
	Key         string // Key is the key of the file.
	Name        string // Name is the name of the file.
	ETag        string // ETag is the ETag of the file.
	Extension   string
	Path        string // Path to save file
	Size        int64  // Size is the size of the file in bytes.
	IsSmallFile bool
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

type Data struct {
	*bytes.Buffer
}

func (d *Data) WriteAt(p []byte, off int64) (int, error) {
	if int64(d.Len()) < off {
		d.Grow(int(off) - d.Len())
	}
	return d.Write(p)
}

func NewBuffer() *Data {
	return &Data{
		Buffer: getBuffer(),
	}
}

// filePool is a pool of File objects for reuse.
var filePool = sync.Pool{
	New: func() interface{} {
		return &File{}
	},
}

// NewFileFromObject creates a new File objects from the given S3 object.
func NewFileFromObject(obj types.Object, localPath string, isFlattenName, isWithDirName, isDecompress bool) *File {
	file := filePool.Get().(*File)
	file.Key = *obj.Key
	file.Extension = filepath.Ext(file.Key)
	file.defineSavePath(localPath, isFlattenName, isWithDirName, isDecompress)
	file.Size = obj.Size
	file.ETag = (*obj.ETag)[1 : len(*obj.ETag)-1] // strings.Trim(*obj.ETag, "\"")
	return file
}

func (file *File) defineSavePath(localPath string, isFlattenName, isWithDirName, isDecompress bool) {
	path := filepath.Dir(file.Key)
	fileName := filepath.Base(file.Key)
	var builder strings.Builder

	if isFlattenName {
		parts := strings.Split(path, string(filepath.Separator))
		for i, part := range parts {
			if i > 0 {
				builder.WriteRune(delimiter)
			}
			builder.WriteString(part)
		}
		builder.WriteRune(delimiter)
		builder.WriteString(fileName)
		fileName = builder.String()
		builder.Reset()
		path = ""
	}
	if file.IsArchive() && isDecompress {
		builder.WriteString(path)
		builder.WriteRune(filepath.Separator)
		builder.WriteString(decompressedDir)
		if isWithDirName {
			builder.WriteRune(filepath.Separator)
			builder.WriteString(fileName)
		}
		fileName = strings.TrimSuffix(fileName, file.Extension) + decompressedSuffix
		path = builder.String()
		builder.Reset()
	}

	file.Path = filepath.Join(localPath, path)
	file.Name = fileName
}

// NewFile creates a new File objects from the pool.
func NewFile() *File {
	file := filePool.Get().(*File)
	return file
}

// ReturnToPool returns the File objects to the pool for reuse.
func (file *File) ReturnToPool() {
	file.reset()
	filePool.Put(file)
}

// reset resets the fields of the File objects to their initial values.
func (file *File) reset() {
	if !file.isEmpty() {
		file.Key = ""
		file.Name = ""
		file.Size = 0
		file.ETag = ""
		file.Extension = ""
		file.Path = ""
		if file.Data != nil && file.Data.Buffer != nil {
			file.Data.Buffer.Reset()
			putBuffer(file.Data.Buffer)
		}
	}
}

func (file *File) IsArchive() bool {
	return archives[file.Extension]
}
func (file *File) isEmpty() bool {
	return file.Key == "" &&
		file.Name == "" &&
		file.ETag == "" &&
		file.Extension == "" &&
		file.Size == 0 &&
		file.Path == "" &&
		file.Data == nil &&
		(file.Data.Buffer == nil || file.Data.Len() == 0)
}
