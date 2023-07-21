package files

import (
	"sync"
	"sync/atomic"
	"time"
)

// FileCollection represents a collection of File objects.
type FileCollection struct {
	DownloadChan    chan *File // DownloadChan is a channel of File objects.
	ArchivesChan    chan string
	totalBytes      int64        // totalBytes is the total number of bytes in the DownloadChan collection.
	count           uint32       // count is the current count of objects in the DownloadChan collection.
	progress        atomic.Int64 // progress is the current sum of a bytes downloaded from bucket
	progressMap     map[*File]int64
	downloadedFiles map[string]bool
	mu              sync.RWMutex
	wg              sync.WaitGroup
}

// NewFileCollection returns a new instance of the FileCollection structure with the specified capacity.
func NewFileCollection() *FileCollection {
	return &FileCollection{
		ArchivesChan:    make(chan string, 512),
		progressMap:     make(map[*File]int64),
		downloadedFiles: make(map[string]bool),
		mu:              sync.RWMutex{},
		wg:              sync.WaitGroup{},
	}
}

func (fc *FileCollection) GetDataToDownload() {
	for file := range fc.progressMap {
		fc.DownloadChan <- file
	}
}

func (fc *FileCollection) AddToProgress(file *File) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.progressMap[file] = 0
	fc.totalBytes += file.Size
	fc.count++
}

// GetStatistics returns statistics about the files in the collection.
func (fc *FileCollection) GetStatistics(duration time.Duration) (count, downloadedCount, remainingCount uint32, totalBytes, progressBytes int64, averageSpeed float64, progressRatio float64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	count = fc.count
	downloadedCount = uint32(len(fc.downloadedFiles))
	remainingCount = count - downloadedCount
	totalBytes = fc.totalBytes
	progressBytes = fc.progress.Load()
	progressRatio = float64(progressBytes) / float64(totalBytes)
	averageSpeed = float64(progressBytes) / duration.Seconds()
	return
}

// UpdateProgress updates the progress of the file collection by adding the specified number of bytes to the progress.
func (fc *FileCollection) UpdateProgress(writtenBytes int64) {
	fc.progress.Add(writtenBytes)
}

func (fc *FileCollection) MarkAsDownloaded(name string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.downloadedFiles[name] = true
}

// Count returns the current count of files in the collection.
func (fc *FileCollection) Count() uint32 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.count
}

func (fc *FileCollection) IsDownloaded(name string) bool {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	_, ok := fc.downloadedFiles[name]
	return ok
}
