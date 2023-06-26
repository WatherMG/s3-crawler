package downloader

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/files"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/schollz/progressbar/v3"
)

// DownloadFile downloads a file from s3
func (d *Downloader) DownloadFile(ctx context.Context, key string) (int64, error) {
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

	return numBytes, nil
}

func (d *Downloader) DownloadFilePart(key string, startByte, endByte int64) (int64, error) {
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
	return numBytes, nil
}

func (d *Downloader) DownloadFiles(ctx context.Context, data *files.Files) {
	start := time.Now()
	var wg sync.WaitGroup
	fileCh := make(chan *files.Object, d.cfg.Pagination.MaxKeys) // TODO: вместо срезов объектов использовать канал ил ListFiles
	sem := make(chan struct{}, d.cfg.Downloaders)                // количество одновременно работающих горутин для загрузки

	go func() {
		defer close(fileCh)
		for _, file := range data.Objects {
			fileCh <- file
		}
	}()

	err := os.MkdirAll(d.cfg.LocalPath, 0755)
	if err != nil {
		log.Printf("MkdirAll error: %v", err)
	}

	bar := progressbar.Default(int64(len(data.Objects)))
	data.TotalBytes = 0
	bar.Describe("Download:")

	for file := range fileCh {
		sem <- struct{}{}
		wg.Add(1)
		go func(file *files.Object) {
			defer func() {
				<-sem
				wg.Done()
			}()
			numBytes, err := d.Download(ctx, file.Key)
			if err != nil {
				log.Printf("Download error %s: %v", file.Key, err)
			}
			data.TotalBytes += numBytes
			bar.Add(1)
		}(file)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Downloaded all files in %s. Total bytes: %.3fMB\n", elapsed, data.GetTotalBytes())
	fmt.Printf("Average speed = %.3fMBps", data.GetAverageSpeed(elapsed))
}
