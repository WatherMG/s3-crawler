package downloader

import (
	"context"
	"os"

	"s3-crawler/pkg/files"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (downloader *Downloader) DownloadFileCustom(ctx context.Context, file *files.File, f *os.File) (int64, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(downloader.cfg.BucketName),
		Key:    aws.String(file.Key),
	}

	concurrency := file.Size / files.ChunkSize
	if file.Size <= files.ChunkSize {
		concurrency = 0
	}
	if file.Size%files.ChunkSize != 0 {
		concurrency++
	}

	d := manager.NewDownloader(downloader.client, func(d *manager.Downloader) {
		d.LogInterruptedDownloads = true
		d.PartSize = file.Size / concurrency
		d.Concurrency = int(concurrency)
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(1 << 13)
	})

	numBytes, err := d.Download(ctx, f, input)

	return numBytes, err
}

/*func (downloader *Downloader) DownloadFileCustom2(ctx context.Context, file *files.File, startByte, endByte int64) (int64, error) {
	var input *s3.GetObjectInput

	// parts := file.Size / files.ChunkSize
	if file.Size <= files.ChunkSize {
		input = &s3.GetObjectInput{
			Bucket: aws.String(downloader.cfg.BucketName),
			Key:    aws.String(file.Key),
		}
	}
	if file.Size%files.ChunkSize != 0 {
		input = &s3.GetObjectInput{
			Bucket: aws.String(downloader.cfg.BucketName),
			Key:    aws.String(file.Key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
		}
	}

	resp, err := downloader.client.GetObject(ctx, input)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	n, err := io.CopyN(file.Data, resp.Body, file.Size)
	if err != nil {
		return 0, err
	}

	return n, err
}*/
