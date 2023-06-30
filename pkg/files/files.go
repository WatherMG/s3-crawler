package files

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const MiB = 1024 * 1024 // MiB is a constant representing the number of bytes in a mebibyte.

// Objects represents a collection of File objects.
type Objects struct {
	Objects    chan *File // Objects is a channel of File objects.
	totalBytes int64      // totalBytes is the total number of bytes in the Objects collection.
	count      uint32     // count is the current count of objects in the Objects collection.
	sync.Mutex            // Mutex is used to synchronize access to shared resources.
}

var objects *Objects // objects is a pointer to the singleton instance of the Objects structure.
var once sync.Once   // once is used to synchronize and ensure that objects initialization happens only once.

// GetInstance returns the singleton objects of the Objects structure.
func GetInstance(capacity uint32) *Objects {
	once.Do(func() {
		objects = &Objects{
			Objects:    make(chan *File, capacity),
			totalBytes: 0,
			count:      0,
		}
	})
	return objects
}

// File represents a file with a Key, Size, and ETag.
type File struct {
	Key  string // Key is the key of the file.
	Size int64  // Size is the size of the file in bytes.
	ETag string // ETag is the ETag of the file.
}

// filePool is a pool of File objects for reuse.
var filePool = sync.Pool{
	New: func() interface{} {
		return &File{}
	},
}

// NewFileFromObject creates a new File objects from the given S3 object.
func NewFileFromObject(obj types.Object) *File {
	file := filePool.Get().(*File)
	file.reset()
	file.Key = *obj.Key
	file.Size = obj.Size
	file.ETag = strings.Trim(*obj.ETag, "\"")
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
	f.Size = 0
	f.ETag = ""
}

// GetTotalBytes returns the total number of bytes in the Objects collection in MiB.
func (o *Objects) GetTotalBytes() float64 {
	return float64(o.totalBytes) / MiB
}

// GetAverageSpeed returns the average speed of the Objects collection in MiB/s.
func (o *Objects) GetAverageSpeed(duration time.Duration) float64 {
	return (float64(o.totalBytes) / duration.Seconds()) / MiB
}

// AddBytes adds the given number of bytes to the total number of bytes in the Objects collection.
func (o *Objects) AddBytes(bytes int64) {
	atomic.AddInt64(&o.totalBytes, bytes)
}

// GetBytes returns the total number of bytes in the Objects collection.
func (o *Objects) GetBytes() int64 {
	return o.totalBytes
}

// ResetBytes resets the total number of bytes in the Objects collection to zero.
func (o *Objects) ResetBytes() {
	o.Lock()
	defer o.Unlock()
	o.totalBytes = 0
}

// GetCount returns the current count of objects in the Objects collection.
func (o *Objects) GetCount() uint32 {
	return o.count
}

// SetCount sets the current count of objects in the Objects collection to the given value.
func (o *Objects) SetCount(count uint32) {
	o.Lock()
	defer o.Unlock()
	o.count = count
}
