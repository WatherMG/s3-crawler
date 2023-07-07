package downloader

import (
	"context"
	"fmt"
	"log"
	"os"
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

const DownloadersModifier int = 1 << 8

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
			manager: manager.NewDownloader(client, func(d *manager.Downloader) {
				d.LogInterruptedDownloads = true
				// d.PartSize = files.ChunkSize >> 7
				d.PartBodyMaxRetries = 5
				// d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(files.ChunkSize >> 2)
				d.Concurrency = int(cfg.NumCPU) << 2
				d.Logger = logging.NewStandardLogger(os.Stdout)
			}),
			client: client,
			cfg:    cfg,
			wg:     &sync.WaitGroup{},
		}
	})
	return downloader
}

func (downloader *Downloader) DownloadFile(ctx context.Context, file *files.File) (int64, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(downloader.cfg.BucketName),
		Key:    aws.String(file.Key),
	}
	numBytes, err := downloader.manager.Download(ctx, file.Data, input)
	if err != nil {
		return 0, err
	}

	return numBytes, err
}

var bufPool = sync.Pool{
	New: func() interface{} {
		buffer := make([]byte, files.Buffer8KB)
		return &buffer
	},
}

func (downloader *Downloader) DownloadFiles(ctx context.Context, data *files.Objects) error {
	activeFiles := sync.Map{}
	start := time.Now()
	go downloader.printProgress(ctx, data, &activeFiles, start)

	numWorkers := int(downloader.cfg.NumCPU) * DownloadersModifier
	for i := 0; i < numWorkers; i++ {
		downloader.wg.Add(1)
		go func() {
			defer downloader.wg.Done()
			for file := range data.DownloadChan {
				if err := downloader.downloadFile(ctx, file, data, &activeFiles); err != nil {
					log.Printf("Download error: %v", err)
					/*fmt.Printf("Retry to download file: %s\n", file.Key) // TODO: отправлять на перезакачку файл, если ошибка, открывать заново канал (лучше использовать буфер).
					data.DownloadChan <- file*/
					return
				}
			}
		}()
	}

	downloader.wg.Wait()

	elapsed := time.Since(start)
	_, bytesInMiB, averageSpeed, _, _ := data.GetStats(elapsed)
	fmt.Printf("\n\nDownloaded all files in %s. Total filesize: %.3fMB\n", elapsed, bytesInMiB)
	fmt.Printf("Average speed = %.3fMBps\n", averageSpeed)

	return nil
}

func (downloader *Downloader) downloadFile(ctx context.Context, file *files.File, data *files.Objects, activeFiles *sync.Map) error {
	buffer := make([]byte, 0, file.Size)
	file.Data = manager.NewWriteAtBuffer(buffer)
	/*f, err := os.Create(downloader.cfg.LocalPath + "/" + file.Name)
	if err != nil {
		return err
	}
	defer f.Close()*/
	activeFiles.Store(file.Name, true)

	numBytes, err := downloader.DownloadFile(ctx, file)
	if err != nil {
		return fmt.Errorf("DownloadFile error:%s %w", file.Key, err)
	}

	data.SetProgress(numBytes)
	activeFiles.Delete(file.Name)

	// err = os.WriteFile(downloader.cfg.LocalPath+"/"+file.Name, file.Data.Bytes(), 0755)
	// if err != nil {
	// 	log.Fatalf("failed to write file to disk, %v", err)
	// }

	file.ReturnToPool()

	return nil
}

func (downloader *Downloader) printProgress(ctx context.Context, data *files.Objects, activeFiles *sync.Map, start time.Time) {
	delay := 1000 / time.Duration(downloader.cfg.NumCPU) * time.Millisecond
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
				dataCount, bytesInMiB, averageSpeed, progress, remainMBToDownload := data.GetStats(time.Since(start))
				fmt.Printf("\u001B[2K\rPart %d, Downloaded %.2f/%.2f MB (%.2f%%). ", downloader.client.GetPagesCount(), progress, bytesInMiB, progress/bytesInMiB*100)
				fmt.Printf("Total [%d] file(s). Remain [%d] file(s) (%.2f MB). Speed: [%.2f MB/s]", dataCount, remainDownloads, remainMBToDownload, averageSpeed)
			default:
				if data.DownloadChan != nil {
					for _, r := range `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` {
						fmt.Printf("\u001B[2K\r%c Waiting metadata to download from bucket. Current page %d.", r, downloader.client.GetPagesCount())
						time.Sleep(delay * 2)
					}
				}
			}
		}
	}
}
