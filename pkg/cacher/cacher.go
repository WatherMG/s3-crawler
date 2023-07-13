package cacher

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
)

func (c *FileCache) LoadFromDir(cfg *configuration.Configuration) error {
	start := time.Now()
	numWorkers := cfg.NumCPU
	filesChan := make(chan string, cfg.NumCPU)
	extensions := strings.Split(cfg.Extension, ",")
	nameMask := strings.ToLower(cfg.NameMask)

	var wg sync.WaitGroup
	c.withParts = cfg.HashWithParts

	c.startWorkers(numWorkers, &wg, filesChan)
	err := c.walkDir(cfg.LocalPath, nameMask, filesChan, extensions)
	close(filesChan)
	wg.Wait()

	log.Printf("Cache loaded from %s.\n", time.Since(start))
	log.Printf("Total files in cache: %d. Total size %d MB.\n", c.totalCount, c.TotalSize/files.MiB)

	return err
}

func (c *FileCache) startWorkers(numWorkers uint8, wg *sync.WaitGroup, filesChan chan string) {
	for i := uint8(0); i <= numWorkers; i++ {
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
		etag, err := getHash(path, c.withParts)
		if err != nil {
			fmt.Printf("Error calculating ETag for file %s: %s\n", path, err.Error())
			return
		}

		file := files.NewFile()
		file.Key = info.Name()
		file.ETag = etag
		file.Size = info.Size()
		c.AddFile(info.Name(), file)
		c.TotalSize += file.Size
	}
}

func (c *FileCache) walkDir(dir, nameMask string, filesChan chan string, extensions []string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			if c.isValidObject(path, nameMask, extensions) {
				filesChan <- path
			}
		}
		return nil
	})
}

func (c *FileCache) isValidObject(path, nameMask string, extensions []string) bool {
	name := strings.ToLower(filepath.Base(path))
	var hasValidExt bool

	for _, ext := range extensions {
		if strings.HasSuffix(name, ext) {
			hasValidExt = true
			break
		}
	}

	hasValidName := strings.Contains(name, nameMask)

	return hasValidExt && hasValidName
}

var bufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, files.Buffer32KB)
		return &b
	},
}

func getHash(filePath string, withParts bool) (string, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return "", err
	}

	fileSize := fileInfo.Size()

	buf := bufPool.Get().(*[]byte)
	defer bufPool.Put(buf)

	if !withParts || fileSize <= files.ChunkSize {
		hash := md5.New()

		if _, err := io.CopyBuffer(hash, file, *buf); err != nil {
			return "", err
		}

		return hex.EncodeToString(hash.Sum(nil)), nil
	}

	parts := fileSize / files.ChunkSize
	if fileSize%files.ChunkSize != 0 {
		parts++
	}
	finalHash := md5.New()
	for i := int64(0); i < parts; i++ {
		partReader := io.NewSectionReader(file, i*files.ChunkSize, files.ChunkSize)
		partHash := md5.New()
		if _, err := io.CopyBuffer(partHash, partReader, *buf); err != nil {
			return "", err
		}
		finalHash.Write(partHash.Sum(nil))
	}
	return hex.EncodeToString(finalHash.Sum(nil)) + "-" + strconv.FormatInt(parts, 10), nil
}
