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
	"sync/atomic"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/printprogress"
	"s3-crawler/pkg/utils"
)

const status = "Loading and comparing cache"

func (c *FileCache) LoadFromDir(cfg *configuration.Configuration) error {
	c.printer.Send(fmt.Sprintf(status))
	defer c.printer.Stop()
	if _, err := os.Stat(cfg.LocalPath); os.IsNotExist(err) {
		return nil
	}
	start := time.Now()
	numWorkers := cfg.NumCPU * 5
	filesChan := make(chan string, numWorkers)
	extensions := strings.Split(cfg.Extension, ",")
	nameMask := strings.ToLower(cfg.NameMask)
	chunkSize := cfg.GetChunkSize()

	var wg sync.WaitGroup
	c.withParts = cfg.IsHashWithParts

	c.startWorkers(numWorkers, &wg, filesChan, chunkSize)
	err := c.walkDir(cfg.LocalPath, nameMask, filesChan, extensions)
	close(filesChan)
	wg.Wait()

	c.loadTime = time.Since(start)
	log.Print("Cache info: ", c)

	return err
}

func (c *FileCache) startWorkers(numWorkers uint8, wg *sync.WaitGroup, filesChan chan string, chunkSize int64) {
	var currentFile atomic.Int32

	for i := uint8(0); i <= numWorkers; i++ {
		wg.Add(1)
		go func(currentFile *atomic.Int32, printer *printprogress.Status) {
			defer wg.Done()
			for path := range filesChan {
				c.processFile(path, chunkSize)
				currentFile.Add(1)
				if currentFile.Load()%1000 == 0 {
					printer.Send(fmt.Sprintf("%s. Current file %d", status, currentFile.Load()))
				}
			}
		}(&currentFile, c.printer)
	}

}

func (c *FileCache) processFile(path string, chunkSize int64) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fmt.Printf("Error get file info %s: %s\n", path, err.Error())
		return
	}

	if !info.IsDir() {
		etag, err := getHash(path, c.withParts, chunkSize)
		if err != nil {
			fmt.Printf("Error calculating ETag for file %s: %s\n", path, err.Error())
			return
		}

		file := files.NewFile()
		file.Name = info.Name()
		file.ETag = etag
		file.Size = info.Size()
		c.AddFile(file.Name, file)
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
			} else {
				c.skipped++
			}
		}
		return nil
	})
}

func (c *FileCache) isValidObject(path, nameMask string, extensions []string) bool {
	name := strings.ToLower(filepath.Base(path))

	hasValidExt := utils.HasValidExtension(path, extensions)
	hasValidName := utils.HasValidName(name, nameMask)

	return hasValidExt && hasValidName
}

var bufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, files.Buffer32KB)
		return &b
	},
}

func getHash(filePath string, withParts bool, chunkSize int64) (string, error) {
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

	if !withParts || fileSize <= chunkSize {
		hash := md5.New()

		if _, err := io.CopyBuffer(hash, file, *buf); err != nil {
			return "", err
		}

		return hex.EncodeToString(hash.Sum(nil)), nil
	}

	parts := fileSize / chunkSize
	if fileSize%chunkSize != 0 {
		parts++
	}
	finalHash := md5.New()
	for i := int64(0); i < parts; i++ {
		partReader := io.NewSectionReader(file, i*chunkSize, chunkSize)
		partHash := md5.New()
		if _, err := io.CopyBuffer(partHash, partReader, *buf); err != nil {
			return "", err
		}
		finalHash.Write(partHash.Sum(nil))
	}
	return hex.EncodeToString(finalHash.Sum(nil)) + "-" + strconv.FormatInt(parts, 10), nil
}
