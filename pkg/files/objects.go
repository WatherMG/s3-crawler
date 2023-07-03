package files

import (
	"sync"
	"sync/atomic"
	"time"
)

// Objects represents a collection of File objects.
type Objects struct {
	sync.Mutex            // Mutex is used to synchronize access to shared resources.
	totalBytes int64      // totalBytes is the total number of bytes in the Objects collection.
	Objects    chan *File // Objects is a channel of File objects.
	count      int        // count is the current count of objects in the Objects collection.
}

var objects *Objects // objects is a pointer to the singleton instance of the Objects structure.
var once sync.Once   // once is used to synchronize and ensure that objects initialization happens only once.

// NewObject returns the singleton objects of the Objects structure.
func NewObject(capacity uint32) *Objects {
	once.Do(func() {
		objects = &Objects{
			Objects:    make(chan *File, capacity),
			totalBytes: 0,
			count:      0,
		}
	})

	return objects
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

// Size returns the total number of bytes in the Objects collection.
func (o *Objects) Size() int64 {
	return o.totalBytes
}

// ResetBytes resets the total number of bytes in the Objects collection to zero.
func (o *Objects) ResetBytes() {
	o.Lock()
	defer o.Unlock()
	o.totalBytes = 0
}

// Count returns the current count of objects in the Objects collection.
func (o *Objects) Count() int {
	return o.count
}

// SetCount sets the current count of objects in the Objects collection to the given value.
func (o *Objects) SetCount(count int) {
	o.Lock()
	defer o.Unlock()
	o.count = count
}
