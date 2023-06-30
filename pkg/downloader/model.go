package downloader

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/s3client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Downloader struct {
	cfg     *configuration.Configuration
	manager *manager.Downloader
	client  *s3client.Client
	wg      sync.WaitGroup
}

var downloader *Downloader
var once sync.Once

func NewDownloader(client *s3client.Client, cfg *configuration.Configuration) *Downloader {
	once.Do(func() {
		downloader = &Downloader{
			manager: manager.NewDownloader(client, func(d *manager.Downloader) {
				d.LogInterruptedDownloads = true
				d.PartSize = files.ChunkSize
			}),
			client: client,
			cfg:    cfg,
		}
	})

	return downloader
}

func (d *Downloader) DownloadFile(ctx context.Context, object *files.File) (int64, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(d.cfg.BucketName),
		Key:    aws.String(object.Key),
	}

	fileName := strings.ReplaceAll(object.Key, "/", "_")

	path := filepath.Join(d.cfg.LocalPath, fileName)
	if d.cfg.Prefix != "" {
		path = filepath.Join(d.cfg.LocalPath, d.cfg.Prefix+"_"+fileName)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	numBytes, err := d.manager.Download(ctx, file, input)

	return numBytes, err
}
