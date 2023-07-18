package cacher

import (
	"sync"

	"s3-crawler/pkg/files"
)

type FileCache struct {
	Files      map[string]*files.File
	totalCount uint32
	mu         sync.RWMutex
	withParts  bool
	TotalSize  int64
}

var fileCache *FileCache // fileCache is a pointer to the singleton instance of the FileCache structure.
var once sync.Once       // once is used to synchronize and ensure that objects initialization happens only once.

// NewCache returns the singleton objects of the FileCache structure.
func NewCache() *FileCache {
	once.Do(func() {
		fileCache = &FileCache{
			Files: make(map[string]*files.File),
		}
	})

	return fileCache
}

func (c *FileCache) AddFile(key string, info *files.File) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Files[key] = info
	c.totalCount++
	c.TotalSize += info.Size
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
		file.ReturnToPool()
		delete(c.Files, key)
		c.totalCount--
	}
}

func (c *FileCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, file := range c.Files {
		file.ReturnToPool()
		delete(c.Files, key)
	}

	c.totalCount = 0
}
