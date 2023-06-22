package s3

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/downloader"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client struct {
	s3Client *s3.Client
	cfg      *configuration.Configuration
}

func NewClient(ctx context.Context, cfg *configuration.Configuration) (*s3.Client, error) {
	config, err := config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           cfg.S3Connection.Endpoint,
					SigningRegion: cfg.S3Connection.Region,
				}, nil
			},
		)),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.S3Connection.AccessKeyID,
			cfg.S3Connection.SecretAccessKey,
			"",
		)),
		config.WithRegion(cfg.S3Connection.Region),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(config)
	return client, nil
}

func (c *Client) CheckBucket(ctx context.Context) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(c.cfg.BucketName),
	}
	_, err := c.s3Client.HeadBucket(ctx, input)
	return err
}

var mu sync.Mutex

func (c *Client) ListFiles(ctx context.Context) (downloader.Bucket, error) {
	start := time.Now()
	var files downloader.Bucket
	input := &s3.ListObjectsV2Input{
		Bucket:  &c.cfg.BucketName,
		Prefix:  &c.cfg.Prefix,
		MaxKeys: c.cfg.Pagination.MaxKeys,
	}
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)

	filesCh := make(chan *downloader.File, c.cfg.Pagination.MaxKeys)

	var wg sync.WaitGroup

	for i := 0; i < c.cfg.CPUWorker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range filesCh {
				if strings.HasSuffix(file.Key, c.cfg.Extension) && strings.Contains(file.Key, c.cfg.NameMask) {
					mu.Lock()
					files.Files = append(files.Files, file)
					mu.Unlock()
				}
			}
		}()
	}

	var pagesWG sync.WaitGroup
	pageNum := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("Error getting page: %v", err)
			continue
		}
		pageNum++
		log.Printf("Current page: %d", pageNum)
		for _, object := range page.Contents {
			pagesWG.Add(1)
			go func(object types.Object) {
				defer pagesWG.Done()
				file := &downloader.File{
					Key:  *object.Key,
					Size: object.Size,
				}
				filesCh <- file
			}(object)
		}
	}
	go func() {
		pagesWG.Wait()
		close(filesCh)
		wg.Wait()
	}()

	log.Printf("ListFiles, elapsed: %s", time.Since(start))
	return files, nil
}
