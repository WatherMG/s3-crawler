package downloader

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"s3-crawler/pkg/archives"
	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/printprogress"
	"s3-crawler/pkg/s3client"
	"s3-crawler/pkg/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
)

const maxRetries = 3

type Downloader struct {
	cfg *configuration.Configuration
	*s3client.Client
	smallFileDownloader *manager.Downloader
	printer             printprogress.ProgressPrinter
	wg                  sync.WaitGroup
	activeFiles         atomic.Int32
}

var downloader *Downloader
var once sync.Once

func NewDownloader(client *s3client.Client, cfg *configuration.Configuration) *Downloader {
	once.Do(func() {
		downloader = &Downloader{
			Client:  client,
			cfg:     cfg,
			wg:      sync.WaitGroup{},
			printer: printprogress.NewPrinter(cfg),
			smallFileDownloader: manager.NewDownloader(client, func(d *manager.Downloader) {
				d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(files.Buffer32KB)
				d.Concurrency = 1
			}),
		}
	})
	return downloader
}

func (downloader *Downloader) DownloadFiles(ctx context.Context, data *files.FileCollection) error {
	start := time.Now()
	workers := downloader.cfg.GetDownloaders()
	data.DownloadChan = make(chan *files.File, workers)
	data.ArchivesChan = make(chan string, workers)

	go downloader.printer.StartProgressTicker(ctx, data, start, &downloader.activeFiles)

	for i := 0; i < workers; i++ {
		downloader.wg.Add(1)
		go func() {
			defer downloader.wg.Done()
			for file := range data.DownloadChan {
				err := downloader.downloadFile(ctx, file, data)
				if err != nil {
					log.Printf("Download error: %v", err)
					continue
				}
				file.ReturnToPool()
			}
		}()
	}

	data.GetDataToDownload()
	close(data.DownloadChan)

	downloader.wg.Wait()
	close(data.ArchivesChan)

	elapsed := time.Since(start)
	_, _, _, bytes, _, averageSpeed, _ := data.GetStatistics(elapsed)
	// clear line
	fmt.Print("\u001B[2K\r")
	if data.Count() > 0 {
		result := fmt.Sprintf("Downloaded all files in %s. Total filesize: %s\n", elapsed, utils.FormatBytes(bytes))
		result += fmt.Sprintf("Average speed = %s/s\n", utils.FormatBytes(int64(averageSpeed)))
		fmt.Print(result)
	} else {
		fmt.Printf("Nothing to download. Exit...\n")
	}
	return nil
}

func (downloader *Downloader) downloadFile(ctx context.Context, file *files.File, data *files.FileCollection) error {
	downloader.activeFiles.Add(1)
	defer downloader.activeFiles.Add(-1)

	destinationFile, err := os.Create(file.Path)
	if err != nil {
		return fmt.Errorf("file: %s, path: %s: %w", file, file.Path, err)
	}
	defer destinationFile.Close()

	pw := &progressWriterAt{
		writer: destinationFile,
		callback: func(n int64) {
			data.UpdateProgress(n)
		},
	}
	if err = downloader.download(ctx, file, pw); err != nil {
		return err
	}

	data.MarkAsDownloaded(file.Name)

	if downloader.cfg.IsDecompress && file.IsArchive() && archives.IsSupportedArchive(file.Name) {
		data.ArchivesChan <- file.Path
	}
	return nil
}

func (downloader *Downloader) download(ctx context.Context, file *files.File, w io.WriterAt) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(downloader.cfg.BucketName),
		Key:    aws.String(file.Key),
	}

	chunkSize := downloader.cfg.GetChunkSize()

	var currentDownloader *manager.Downloader
	if file.Size <= chunkSize {
		currentDownloader = downloader.smallFileDownloader
	} else {
		currentDownloader = downloader.createDownloader(file.Size, chunkSize)
	}

	_, err := currentDownloader.Download(ctx, w, input)
	if err != nil {
		return fmt.Errorf("download error for file: %s, error: %w", file.Name, err)
	}

	return nil
}

func (downloader *Downloader) createDownloader(fileSize int64, chuckSize int64) *manager.Downloader {
	newDownloader := manager.NewDownloader(downloader, func(d *manager.Downloader) {
		d.Logger = logging.NewStandardLogger(os.Stdout)
	})

	parts := downloader.getDownloadParts(fileSize, chuckSize)

	newDownloader.PartSize = chuckSize
	newDownloader.Concurrency = parts
	newDownloader.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(files.Buffer512KB)
	newDownloader.PartBodyMaxRetries = maxRetries

	return newDownloader
}

func (downloader *Downloader) getDownloadParts(fileSize, chunkSize int64) int {
	parts := fileSize / chunkSize
	if fileSize%chunkSize != 0 {
		parts++
	}
	return int(parts)
}
