package files

import (
	"sync"
	"time"
)

// Objects represents a collection of File objects.
type Objects struct {
	ProcessedChan chan *File // ProcessedChan is a channel of File objects.
	totalBytes    int64      // totalBytes is the total number of bytes in the ProcessedChan collection.
	count         uint32     // count is the current count of objects in the ProcessedChan collection.
	progress      int64
	mu            sync.Mutex
}

// NewObject returns the singleton objects of the Objects structure.
func NewObject(capacity uint16) *Objects {
	var objects *Objects // objects is a pointer to the singleton instance of the ProcessedChan structure.
	var once sync.Once   // once is used to synchronize and ensure that objects initialization happens only once.

	once.Do(func() {
		objects = &Objects{
			ProcessedChan: make(chan *File, capacity),
		}
	})

	return objects
}

func (objects *Objects) AddFile(file *File) {
	objects.mu.Lock()
	defer objects.mu.Unlock()
	objects.ProcessedChan <- file
	objects.totalBytes += file.Size
	objects.count++
}

func (objects *Objects) GetStats(duration time.Duration) (count uint32, bytesInMiB, averageSpeed, progress, remainMBToDownload float64) {
	objects.mu.Lock()
	defer objects.mu.Unlock()
	count = objects.count
	bytesInMiB = float64(objects.totalBytes) / MiB
	progress = float64(objects.progress) / MiB
	remainMBToDownload = bytesInMiB - progress
	averageSpeed = progress / duration.Seconds()
	return
}

func (objects *Objects) SetProgress(progress int64) {
	objects.mu.Lock()
	defer objects.mu.Unlock()
	objects.progress += progress
}

func (objects *Objects) Count() uint32 {
	objects.mu.Lock()
	defer objects.mu.Unlock()
	return objects.count
}
