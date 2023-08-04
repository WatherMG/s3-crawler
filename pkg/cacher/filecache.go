package cacher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/printprogress"
	"s3-crawler/pkg/utils"
)

type FileCache struct {
	mu         sync.RWMutex
	printer    *printprogress.Status
	Files      map[string]*files.File
	skipped    int
	loadTime   time.Duration
	totalSize  int64
	totalCount uint32
	withParts  bool
}

var fileCache *FileCache // fileCache is a pointer to the singleton instance of the FileCache structure.
var once sync.Once       // once is used to synchronize and ensure that objects initialization happens only once.

// NewCache returns the singleton objects of the FileCache structure.
func NewCache(ctx context.Context, cfg *configuration.Configuration) *FileCache {
	once.Do(func() {
		fileCache = &FileCache{
			Files:   make(map[string]*files.File),
			printer: printprogress.NewStatusPrinter(ctx, cfg.Progress.Delay, true),
		}
	})

	return fileCache
}

func (c *FileCache) AddFile(key string, file *files.File) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Files[key] = file
	c.totalCount++
	c.totalSize += file.Size
}

func (c *FileCache) HasFile(key, etag string, size int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cachedInfo, ok := c.Files[key]

	return ok && cachedInfo.ETag == etag && cachedInfo.Size == size
}

func (c *FileCache) RemoveFile(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if file, ok := c.Files[key]; ok {
		delete(c.Files, key)
		c.totalCount--
		file.ReturnToPool()
	}
}

func (c *FileCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, file := range c.Files {
		delete(c.Files, key)
		file.ReturnToPool()
	}

	c.totalCount = 0
}

// String implements the fmt.Stringer interface and provides a custom string representation of the FileCache structure.
func (c *FileCache) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var b strings.Builder
	if c.totalCount > 0 {
		// Total number of files in the cache
		fmt.Fprintf(&b, "Data: [%d] file(s). Skipped [%d] file(s). ", c.totalCount, c.skipped)
		// Total size of files in the cache
		fmt.Fprintf(&b, "Total size: [%s]. ", utils.FormatBytes(c.totalSize))
		// Load time of files in the cache
		fmt.Fprintf(&b, "Loaded in: %s\n", c.loadTime.Truncate(time.Millisecond))
	} else {
		fmt.Fprintf(&b, "Requested files not found in the cache. Total scaned %d file(s). Elapsed: %s\n", c.skipped, c.loadTime.Truncate(time.Millisecond))
	}
	return b.String()
}
