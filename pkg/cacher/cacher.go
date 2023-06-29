package cacher

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"s3-crawler/pkg/files"
)

const chunkSize = 8 * 1024 * 1024 // Используется для корректного разбиения на чанки для составления хеша локального файла, как на s3

type FileCache struct {
	Files map[string]*files.File
	Count int64
	mu    sync.Mutex
}

func NewCache() *FileCache {
	return &FileCache{
		Files: make(map[string]*files.File),
	}
}

func (c *FileCache) AddFile(key string, info *files.File) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Files[key] = info
	c.Count++
}

func (c *FileCache) HasFile(key string, info *files.File) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	fileName := strings.ReplaceAll(key, "/", "_")
	cachedInfo, ok := c.Files[fileName]
	if ok {
		fmt.Printf("S3 : %s %s\nLoc: %s %s\n", info.ETag, info.Key, cachedInfo.ETag, cachedInfo.Key)
	}
	return ok && cachedInfo.ETag == info.ETag && cachedInfo.Size == info.Size
}

func (c *FileCache) RemoveFile(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.Files, key)
	c.Count--
}

func (c *FileCache) LoadFromDir(dir string) error {
	filesChan := make(chan string)
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup

	c.startWorkers(numWorkers, &wg, filesChan)
	err := c.walkDir(dir, filesChan)
	close(filesChan)
	wg.Wait()
	return err
}

func (c *FileCache) startWorkers(numWorkers int, wg *sync.WaitGroup, filesChan chan string) {
	for i := 0; i <= numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range filesChan {
				c.processFile(path)
			}
		}()
	}
}

func (c *FileCache) processFile(path string) {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Printf("Error stating file %s: %s\n", path, err.Error())
		return
	}
	if !info.IsDir() {
		etag, err := calcEtag(path, chunkSize)
		if err != nil {
			fmt.Printf("Error calculating ETag for file %s: %s\n", path, err.Error())
			return
		}

		file := files.FilePool.Get().(*files.File)
		file.Key = info.Name()
		file.ETag = etag
		file.Size = info.Size()
		c.AddFile(info.Name(), file)
	}
}

func (c *FileCache) walkDir(dir string, filesChan chan string) error {
	return filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			filesChan <- path
		}
		return nil
	})
}

var chunkPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, chunkSize)
	},
}

func calcEtag(filePath string, partSize int64) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	filesize := info.Size()
	size := partSize
	if filesize < size {
		size = filesize
	}
	var md5Digest [][]byte
	for {
		chunk := chunkPool.Get().([]byte)[:size]
		n, err := file.Read(chunk)
		if err != nil && !errors.Is(err, io.EOF) {
			chunkPool.Put(chunk)
			return "", err
		}
		if n == 0 {
			chunkPool.Put(chunk)
			break
		}
		chunk = chunk[:n]
		hash := md5.Sum(chunk)
		md5Digest = append(md5Digest, hash[:])
		chunkPool.Put(chunk)
	}

	if len(md5Digest) == 1 {
		return hex.EncodeToString(md5Digest[0]), nil
	}
	var finalMD5 []byte
	for _, digest := range md5Digest {
		finalMD5 = append(finalMD5, digest...)
	}
	hash := md5.Sum(finalMD5)
	return hex.EncodeToString(hash[:]) + "-" + strconv.Itoa(len(md5Digest)), nil
}
