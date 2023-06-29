package files

import (
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Objects struct {
	Objects    chan *File
	totalBytes int64
	count      uint32
	mu         sync.Mutex
}

func NewObjects(capacity uint32) *Objects {
	return &Objects{
		Objects:    make(chan *File, capacity),
		totalBytes: 0,
		count:      0,
	}
}

type File struct {
	Key  string
	Size int64
	ETag string
}

var FilePool = sync.Pool{
	New: func() interface{} {
		return &File{}
	},
}

func NewFile(obj types.Object) *File {
	file := FilePool.Get().(*File)
	file.Key = *obj.Key
	file.Size = obj.Size
	file.ETag = strings.Trim(*obj.ETag, "\"")
	return file
}

func (f *File) ReturnToPool() {
	FilePool.Put(f)
}

func (o *Objects) GetTotalBytes() float64 {
	return float64(o.totalBytes) / math.Pow(1024, 2)
}

func (o *Objects) GetAverageSpeed(duration time.Duration) float64 {
	return (float64(o.totalBytes) / duration.Seconds()) / math.Pow(1024, 2)
}

func (o *Objects) GetBytes() int64 {
	return o.totalBytes
}

func (o *Objects) AddBytes(bytes int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.totalBytes += bytes
}

func (o *Objects) GetCount() uint32 {
	return o.count
}

func (o *Objects) SetCount(count uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.count = count
}

func (o *Objects) ResetBytes() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.totalBytes = 0
}
