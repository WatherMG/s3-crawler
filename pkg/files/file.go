package files

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"s3-crawler/pkg/utils"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	Buffer8KB = 1 << (13 + iota)
	Buffer16KB
	Buffer32KB
	Buffer64KB
	Buffer128KB
	Buffer256KB
	KiB = 1 << 10
	MiB = 1 << 20
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
	Key       string // Key is the key of the file.
	Name      string // Name is the name of the file.
	ETag      string // ETag is the ETag of the file.
	Extension string
	Path      string // Path to save file
	Size      int64  // Size is the size of the file in bytes.
}

func (f *File) String() string {
	var buf = &strings.Builder{}
	buf.WriteRune('[')
	buf.WriteString(fmt.Sprintf("Key: %s, ", f.Key))
	buf.WriteString(fmt.Sprintf("Size: %s]", utils.FormatBytes(f.Size)))
	return buf.String()
}

// filePool is a pool of File objects for reuse.
var filePool = sync.Pool{
	New: func() interface{} {
		return &File{}
	},
}

// NewFileFromObject creates a new File objects from the given S3 object.
func NewFileFromObject(obj types.Object, localPath string) *File {
	file := filePool.Get().(*File)
	file.reset()
	file.Key = *obj.Key
	file.Name = strings.ReplaceAll(file.Key, "/", "_")
	file.Size = obj.Size
	file.ETag = strings.Trim(*obj.ETag, "\"")
	file.Extension = filepath.Ext(file.Name)
	file.Path = filepath.Join(localPath, file.Name)

	return file
}

// NewFile creates a new File objects from the pool.
func NewFile() *File {
	file := filePool.Get().(*File)
	file.reset()

	return file
}

// ReturnToPool returns the File objects to the pool for reuse.
func (f *File) ReturnToPool() {
	f.reset()
	filePool.Put(f)
}

// reset resets the fields of the File objects to their initial values.
func (f *File) reset() {
	f.Key = ""
	f.Name = ""
	f.Size = 0
	f.ETag = ""
	f.Extension = ""
}

func (f *File) IsArchive() bool {
	return archives[f.Extension]
}
