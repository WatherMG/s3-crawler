package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/configuration"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Downloader struct {
	client *s3.Client
	cfg    *configuration.Configuration
	*Bucket
	bufPool *sync.Pool
	once    sync.Once
}

type DownloadResult struct {
	name  string
	bytes int
	part  int
	err   error
}

type File struct {
	Key  string
	Size int64
}

func (b *Bucket) getTotalBytes() float64 {
	return float64(b.totalBytes) / math.Pow(1024, 2)
}

func (b *Bucket) getAverageSpeed(duration time.Duration) float64 {
	return (float64(b.totalBytes) / duration.Seconds()) / math.Pow(1024, 2)
}

type Bucket struct {
	Files      chan *File
	totalBytes int
	Length     int
}

func (b *Bucket) Len() int {
	return len(b.Files)
}

func NewBucket(capacity int) *Bucket {
	return &Bucket{
		totalBytes: 0,
		Files:      make(chan *File, capacity),
		Length:     0,
	}
}

func NewDownloader(client *s3.Client, cfg *configuration.Configuration, capacity int) *Downloader {
	return &Downloader{
		client: client,
		cfg:    cfg,
		Bucket: NewBucket(capacity),
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (d *Downloader) Download(ctx context.Context, data []*File) error {
	resultCh := make(chan DownloadResult, d.cfg.Downloaders)

	var counter int
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < d.cfg.Downloaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range d.Files {
				fileSize := file.Size
				fileName := file.Key

				if fileSize > 10*1024*1024 {
					partSize := int64(1 * 1024 * 1024)
					partsCount := int64(math.Ceil(float64(fileSize) / float64(partSize)))

					var partWg sync.WaitGroup
					for j := int64(0); j < partsCount; j++ {
						partNumber := j
						startByte := j * partSize
						endByte := startByte + partSize - 1
						if endByte > fileSize-1 {
							endByte = fileSize - 1
						}
						partWg.Add(1)
						go func(partNumber, fileSize int64, fileName string) {
							defer partWg.Done()
							numBytes, err := d.DownloadFilePart(fileName, startByte, endByte)
							resultCh <- DownloadResult{fileName, numBytes, int(partNumber + 1), err}
						}(partNumber, fileSize, fileName)
					}
					partWg.Wait()
				} else {
					go func(fileSize int64, fileName string) {
						b, err := d.DownloadFile(ctx, fileName)
						resultCh <- DownloadResult{fileName, b, 0, err}
					}(fileSize, fileName)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range resultCh {
			switch {
			case result.err != nil:
				log.Printf("Download error:%v", result.err)
			case result.part > 0:
				mu.Lock()
				d.totalBytes += result.bytes
				mu.Unlock()
			default:
				fmt.Printf("Downloaded %s (%d bytes)\n", result.name, result.bytes)
				mu.Lock()
				d.totalBytes += result.bytes
				counter++
				mu.Unlock()
				if counter == len(d.Files) {
					close(resultCh)
				}
			}
		}
	}()
	go func() {
		for _, file := range data {
			d.Files <- file
		}
		close(d.Files)
	}()

	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Downloaded all files in %s. Total bytes: %.3fMB\n", elapsed, d.getTotalBytes())
	fmt.Printf("Average speed = %.3fMBps", d.getAverageSpeed(elapsed))
	return nil
}

// DownloadFile downloads a file from s3
func (d *Downloader) DownloadFile(ctx context.Context, key string) (int, error) {
	input := &s3.GetObjectInput{
		Bucket: &d.cfg.BucketName,
		Key:    &key,
	}
	result, err := d.client.GetObject(ctx, input)
	if err != nil {
		return 0, err
	}
	defer result.Body.Close()

	fileName := strings.ReplaceAll(key, "/", "_")
	// Create a local file
	path := filepath.Join(d.cfg.LocalPath, fileName)
	if d.cfg.Prefix != "" {
		path = filepath.Join(d.cfg.LocalPath, d.cfg.Prefix+fileName)
	}
	err = os.MkdirAll(d.cfg.LocalPath, 0755)
	if err != nil {
		return 0, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	numBytes, err := io.CopyN(file, result.Body, result.ContentLength)
	if err != nil {
		return 0, err
	}

	return int(numBytes), nil
}

func (d *Downloader) DownloadFilePart(key string, startByte, endByte int64) (int, error) {
	input := &s3.GetObjectInput{
		Bucket: &d.cfg.BucketName,
		Key:    &key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
	}
	result, err := d.client.GetObject(context.TODO(), input)
	if err != nil {
		return 0, err
	}
	defer result.Body.Close()

	fileName := strings.ReplaceAll(key, "/", "_")
	path := filepath.Join(d.cfg.LocalPath, fileName)
	if d.cfg.Prefix != "" {
		path = filepath.Join(d.cfg.LocalPath, d.cfg.Prefix+fileName)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	numBytes, err := io.CopyN(file, result.Body, endByte-startByte+1)
	if err != nil {
		return 0, err
	}
	return int(numBytes), nil
}
