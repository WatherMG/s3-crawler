package downloader

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/s3client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
)

const DownloadersModifier int = 1 << 6

type Downloader struct {
	cfg     *configuration.Configuration
	manager *manager.Downloader
	client  *s3client.Client
	wg      *sync.WaitGroup
}

var downloader *Downloader
var once sync.Once

func NewDownloader(client *s3client.Client, cfg *configuration.Configuration) *Downloader {
	once.Do(func() {
		downloader = &Downloader{
			client: client,
			cfg:    cfg,
			wg:     &sync.WaitGroup{},
		}
	})
	return downloader
}

func (downloader *Downloader) DownloadFiles(ctx context.Context, data *files.Objects) error {
	activeFiles := sync.Map{}
	start := time.Now()
	go downloader.printProgress(ctx, data, &activeFiles, start)

	for file := range data.ProcessedChan {
		downloader.wg.Add(1)
		go func(file *files.File) {
			defer downloader.wg.Done()
			if err := downloader.downloadFile(ctx, file, data, &activeFiles); err != nil {
				log.Printf("Download error: %v", err)
				return
			}
		}(file)
	}

	downloader.wg.Wait()

	elapsed := time.Since(start)
	_, bytesInMiB, averageSpeed, _, _ := data.GetStats(elapsed)
	fmt.Printf("\n\nDownloaded all files in %s. Total filesize: %.3fMB\n", elapsed, bytesInMiB)
	fmt.Printf("Average speed = %.3fMBps\n", averageSpeed)

	return nil
}

func (downloader *Downloader) downloadFile(ctx context.Context, file *files.File, data *files.Objects, activeFiles *sync.Map) error {
	activeFiles.Store(file.Name, true)

	input := &s3.GetObjectInput{
		Bucket: aws.String(downloader.cfg.BucketName),
		Key:    aws.String(file.Key),
	}

	newDownloader := manager.NewDownloader(downloader.client, func(d *manager.Downloader) {
		d.Logger = logging.NewStandardLogger(os.Stdout)
		// d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(files.MiB)
	})

	path := filepath.Join(downloader.cfg.LocalPath, file.Name)
	destinationFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	parts := file.Size / files.ChunkSize
	if file.Size%files.ChunkSize != 0 {
		parts++
	}

	if file.Size <= files.ChunkSize {
		newDownloader.PartSize = file.Size
		newDownloader.Concurrency = 1
	} else {
		newDownloader.PartSize = files.ChunkSize
		newDownloader.Concurrency = int(parts)
	}

	numBytes, err := newDownloader.Download(ctx, destinationFile, input)
	if err != nil {
		return err
	}

	data.SetProgress(numBytes)
	activeFiles.Delete(file.Name)
	file.ReturnToPool()

	return nil
}

func (downloader *Downloader) printProgress(ctx context.Context, data *files.Objects, activeFiles *sync.Map, start time.Time) {
	delay := (1000 / time.Duration(downloader.cfg.NumCPU)) * time.Millisecond
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			remainDownloads := 0
			activeFiles.Range(func(_, b any) bool {
				if b == true {
					remainDownloads++
				}
				return true
			})
			switch {
			case remainDownloads > 0:
				dataCount, bytesInMiB, _, progress, remainMBToDownload := data.GetStats(time.Since(start))
				fmt.Printf("\u001B[2K\rDownloaded %.2f/%.2f MB (%.2f%%). ", progress, bytesInMiB, progress/bytesInMiB*100)
				fmt.Printf("Total [%d] file(s). Remain [%d] file(s) (%.2f MB).", dataCount, remainDownloads, remainMBToDownload)
			default:
				if data.ProcessedChan != nil && remainDownloads != 0 {
					for _, r := range `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` {
						fmt.Printf("\u001B[2K\r%c Waiting metadata to download from bucket.", r)
						time.Sleep(delay)
					}
				}
			}
		}
	}
}
