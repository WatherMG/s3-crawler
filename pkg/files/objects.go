package files

import (
	"sync"
	"time"
)

// FileCollection represents a collection of File objects.
type FileCollection struct {
	DownloadChan    chan *File // DownloadChan is a channel of File objects.
	totalBytes      int64      // totalBytes is the total number of bytes in the DownloadChan collection.
	count           uint32     // count is the current count of objects in the DownloadChan collection.
	progress        int64      // progress is the current sum of a bytes downloaded from bucket
	progressMap     map[*File]int64
	downloadedFiles map[*File]bool
	mu              sync.RWMutex
	wg              sync.WaitGroup
}

// NewFileCollection returns a new instance of the FileCollection structure with the specified capacity.
func NewFileCollection(capacity uint16) *FileCollection {
	return &FileCollection{
		progressMap:     make(map[*File]int64),
		downloadedFiles: make(map[*File]bool),
		mu:              sync.RWMutex{},
		wg:              sync.WaitGroup{},
	}
}

// Add adds a file to the collection and updates the total bytes and count.
func (fc *FileCollection) Add(file *File) {
	fc.DownloadChan <- file
}

func (fc *FileCollection) GetDataToDownload() {
	fc.wg.Add(len(fc.progressMap))
	fc.mu.RLock()
	for file := range fc.progressMap {
		go func(file *File) {
			defer fc.wg.Done()
			fc.DownloadChan <- file
		}(file)
	}
	fc.mu.RUnlock()
	fc.wg.Wait()
	close(fc.DownloadChan)
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
	progressBytes = fc.progress
	progressRatio = float64(progressBytes) / float64(totalBytes)
	averageSpeed = float64(progressBytes) / duration.Seconds()
	return
}

// UpdateProgress updates the progress of the file collection by adding the specified number of bytes to the progress.
func (fc *FileCollection) UpdateProgress(file *File, progress int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if _, ok := fc.progressMap[file]; !ok {
		fc.progressMap[file] = 0
	}
	fc.progressMap[file] += progress
	fc.progress += progress
}

func (fc *FileCollection) MarkAsDownloaded(file *File) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.downloadedFiles[file] = true
}

// Count returns the current count of files in the collection.
func (fc *FileCollection) Count() uint32 {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.count
}
