package downloader

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
				d.LogInterruptedDownloads = true
				d.PartBodyMaxRetries = 0

			}),
		}
	})
	return downloader
}

func (downloader *Downloader) DownloadFiles(ctx context.Context, data *files.FileCollection) (time.Duration, error) {
	start := time.Now()
	workers := downloader.cfg.GetDownloaders()

	go downloader.printer.StartProgressTicker(ctx, data, start, &downloader.activeFiles)

	for i := 0; i < workers; i++ {
		downloader.wg.Add(1)
		go func() {
			defer downloader.wg.Done()
			for fileData := range data.DownloadChan {
				err := downloader.downloadFile(ctx, fileData, data)
				if err != nil {
					log.Printf("Download error: %v", err)
					continue
				}
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
		result := fmt.Sprintf("Downloaded %d file(s) in %s. Total filesize: %s. ", data.Count(), elapsed.Truncate(time.Millisecond), utils.FormatBytes(bytes))
		result += fmt.Sprintf("Average download speed = %s/s\n", utils.FormatBytes(int64(averageSpeed)))
		fmt.Print(result)
	} else {
		fmt.Printf("Nothing to download. Exit...\n")
	}
	return elapsed, nil
}

func (downloader *Downloader) downloadFile(ctx context.Context, fileData *files.File, data *files.FileCollection) error {
	downloader.activeFiles.Add(1)
	defer downloader.activeFiles.Add(-1)
	defer data.MarkAsDownloaded(fileData)

	if fileData.IsSmallFile || fileData.IsArchive() {
		fileData.Data = files.NewBuffer()
		fileData.Data.Grow(int(fileData.Size))
		pw := NewProgressWriterAt(fileData.Data, fileData.Size, func(n int64) {
			data.UpdateProgress(n)
		})

		if err := downloader.download(ctx, fileData, pw); err != nil {
			return fmt.Errorf("download file %s error: %w", fileData.Name, err)
		}

		if numBytes := pw.(*progressWriterAt).BytesWritten(); numBytes != int(fileData.Size) {
			return fmt.Errorf("written bytes not equal fileData size")
		}

		if downloader.cfg.IsDecompress && fileData.IsArchive() && archives.IsSupportedArchive(fileData.Extension) {
			data.ArchivesChan <- fileData
		} else {
			data.DataChan <- fileData
		}
	} else {
		if err := utils.CreatePath(fileData.Path); err != nil {
			return fmt.Errorf("create folder %s error: %w", fileData.Path, err)
		}
		file, err := os.OpenFile(filepath.Join(fileData.Path, fileData.Name), os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return fmt.Errorf("create file %s error: %w", fileData.Name, err)
		}
		defer file.Close()
		pw := NewProgressWriterAt(file, fileData.Size, func(n int64) {
			data.UpdateProgress(n)
		})
		defer func(at *progressWriterAt) {
			err = at.Close()
			if err != nil {
				log.Println(err)
			}
		}(pw.(*progressWriterAt))
		if err = downloader.download(ctx, fileData, pw); err != nil {
			return fmt.Errorf("download file %s error: %w", fileData.Name, err)
		}

		if actualSize := pw.(*progressWriterAt).BytesWritten(); actualSize != int(fileData.Size) {
			return fmt.Errorf("written bytes not equal file size")
		}
	}
	return nil
}

func (downloader *Downloader) download(ctx context.Context, fileData *files.File, w io.WriterAt) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(downloader.cfg.BucketName),
		Key:    aws.String(fileData.Key),
	}

	chunkSize := downloader.cfg.GetChunkSize()

	var currentDownloader *manager.Downloader
	if fileData.Size <= chunkSize {
		currentDownloader = downloader.smallFileDownloader
		fileData.IsSmallFile = true
	} else {
		currentDownloader = downloader.createDownloader(fileData.Size, chunkSize)
	}

	_, err := currentDownloader.Download(ctx, w, input)
	if err != nil {
		return fmt.Errorf("download error for fileData: %s, error: %w", fileData.Name, err)
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
	newDownloader.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(files.MiB)
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
