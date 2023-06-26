package downloader

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/s3client"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const FileSize = 0.125 * 1024 * 1024

type Downloader struct {
	cfg     *configuration.Configuration
	manager *manager.Downloader
	client  *s3client.Client
}

func NewDownloader(client *s3client.Client, cfg *configuration.Configuration) *Downloader {
	return &Downloader{
		manager: manager.NewDownloader(client, func(d *manager.Downloader) {
			d.Concurrency = cfg.CPUWorker
			d.PartSize = FileSize
		}),
		client: client,
		cfg:    cfg,
	}
}

func (d *Downloader) Download(ctx context.Context, key string) (int64, error) {
	// TODO: Добавить функционал выкачки данных в буфер с передачей в канал, для последующей записи на диск из буфера.
	fileName := strings.ReplaceAll(key, "/", "_")
	path := filepath.Join(d.cfg.LocalPath, fileName)
	if d.cfg.Prefix != "" {
		path = filepath.Join(d.cfg.LocalPath, d.cfg.Prefix+"_"+fileName)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	input := &s3.GetObjectInput{
		Bucket: &d.cfg.BucketName,
		Key:    &key,
	}

	numBytes, err := d.manager.Download(ctx, file, input)
	return numBytes, err
}
