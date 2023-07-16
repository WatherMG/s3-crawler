package downloader

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
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

type Downloader struct {
	cfg *configuration.Configuration
	*s3client.Client
	wg      *sync.WaitGroup
	printer printprogress.ProgressPrinter
}

var downloader *Downloader
var once sync.Once

func NewDownloader(client *s3client.Client, cfg *configuration.Configuration) *Downloader {
	once.Do(func() {
		downloader = &Downloader{
			Client:  client,
			cfg:     cfg,
			wg:      &sync.WaitGroup{},
			printer: printprogress.NewPrinter(cfg),
		}
	})
	return downloader
}

func (downloader *Downloader) DownloadFiles(ctx context.Context, data *files.FileCollection) error {
	activeFiles := &sync.Map{}
	start := time.Now()
	go downloader.printer.StartProgressTicker(ctx, data, activeFiles, start)

	for i := 0; i < downloader.cfg.GetDownloaders(); i++ {
		downloader.wg.Add(1)
		go func() {
			defer downloader.wg.Done()
			for file := range data.DownloadChan {
				if err := downloader.downloadFile(ctx, file, data, activeFiles); err != nil {
					log.Printf("Download error: %v", err)
					return
				}
			}
		}()
	}

	downloader.wg.Wait()

	elapsed := time.Since(start)
	_, _, _, bytes, _, averageSpeed, _ := data.GetStatistics(elapsed)
	// clear line
	fmt.Print("\u001B[2K\r")
	if data.Count() > 0 {
		result := fmt.Sprintf("Downloaded all files in %s. Total filesize: %s\n", elapsed, utils.FormatBytes(bytes))
		result += fmt.Sprintf("Average speed = %s\n", utils.FormatBytes(int64(averageSpeed)))
		fmt.Print(result)
	} else {
		fmt.Printf("Nothing to download. Exit...\n")
	}

	return nil
}

func (downloader *Downloader) downloadFile(ctx context.Context, file *files.File, data *files.FileCollection, activeFiles *sync.Map) error {
	activeFiles.Store(file.Name, true)
	defer activeFiles.Delete(file.Name)
	defer file.ReturnToPool()

	path := downloader.getFilePath(file)
	destinationFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	pw := &progressWriterAt{
		writer:   destinationFile,
		fileSize: file.Size,
		callback: func(n int64) {
			data.UpdateProgress(file, n)
		},
	}
	if err = downloader.download(ctx, file, pw); err != nil {
		return err
	}

	if downloader.cfg.IsDecompress && file.IsArchive() && archives.IsSupportedArchive(file.Name) {
		if err = downloader.decompressFile(destinationFile, path, file); err != nil {
			return err
		}
	}
	data.MarkAsDownloaded(file)

	return nil
}

func (downloader *Downloader) getFilePath(file *files.File) string {
	return filepath.Join(downloader.cfg.LocalPath, file.Name)
}

func (downloader *Downloader) decompressFile(destinationFile *os.File, path string, file *files.File) error {
	destinationFile.Close()
	var decompressedPath string
	if downloader.cfg.IsWithDirName {
		decompressedPath = filepath.Join(downloader.cfg.LocalPath, "decompressed", file.Name)
	} else {
		decompressedPath = filepath.Join(downloader.cfg.LocalPath, "decompressed")
	}
	if err := archives.DecompressFile(path, decompressedPath); err != nil {
		return err
	}
	if downloader.cfg.IsDeleteAfterDecompress {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func (downloader *Downloader) download(ctx context.Context, file *files.File, w io.WriterAt) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(downloader.cfg.BucketName),
		Key:    aws.String(file.Key),
	}
	newDownloader := downloader.createDownloader(file.Size)

	_, err := newDownloader.Download(ctx, w, input)
	if err != nil {
		return err
	}

	return nil
}

func (downloader *Downloader) createDownloader(fileSize int64) *manager.Downloader {
	newDownloader := manager.NewDownloader(downloader, func(d *manager.Downloader) {
		d.Logger = logging.NewStandardLogger(os.Stdout)
	})

	parts, partSize, bufferSize, maxRetries := downloader.getDownloadParts(fileSize)

	newDownloader.PartSize = partSize
	newDownloader.Concurrency = int(parts)
	if bufferSize != 0 {
		newDownloader.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(bufferSize)
	}
	newDownloader.PartBodyMaxRetries = maxRetries

	return newDownloader
}

func (downloader *Downloader) getDownloadParts(fileSize int64) (int64, int64, int, int) {
	var parts, partSize int64
	var bufferSize, maxRetries int
	chunkSize := downloader.cfg.GetChunkSize()
	if fileSize <= chunkSize {
		parts = 1
		partSize = fileSize
		bufferSize = files.Buffer32KB
		maxRetries = 1
	} else {
		parts = fileSize / chunkSize
		if fileSize%chunkSize != 0 {
			parts++
		}
		partSize = chunkSize
		bufferSize = files.MiB
		maxRetries = 5
	}
	return parts, partSize, bufferSize, maxRetries
}
