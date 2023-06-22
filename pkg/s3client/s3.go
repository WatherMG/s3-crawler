package s3client

import (
	"context"
	"fmt"
	"log"
	"strings"
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
	*s3.Client
	cfg        *configuration.Configuration
	Objects    chan types.Object
	input      *s3.ListObjectsV2Input
	pagesCount int32
}

func NewClient(ctx context.Context, cfg *configuration.Configuration) (*Client, error) {
	client := &Client{
		cfg:     cfg,
		Objects: make(chan types.Object, cfg.Pagination.MaxKeys),
		input: &s3.ListObjectsV2Input{
			Bucket:  &cfg.BucketName,
			Prefix:  &cfg.Prefix,
			MaxKeys: cfg.Pagination.MaxKeys,
		},
	}
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
	client.Client = s3.NewFromConfig(config)
	return client, nil
}

func (c *Client) CheckBucket(ctx context.Context) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(c.cfg.BucketName),
	}
	_, err := c.HeadBucket(ctx, input)
	return err
}

func (c *Client) ListFiles(ctx context.Context) ([]*downloader.File, error) {
	objects, err := c.ListObjects(ctx)
	if err != nil {
		return nil, err
	}
	files := make([]*downloader.File, len(objects), c.cfg.Pagination.MaxKeys*c.pagesCount)

	start := time.Now()

	for i, object := range objects {
		if strings.HasSuffix(*object.Key, c.cfg.Extension) && strings.Contains(*object.Key, c.cfg.NameMask) {
			file := &downloader.File{
				Key:  *object.Key,
				Size: object.Size,
			}
			files[i] = file
		}
	}

	fmt.Printf("Total files to download:%d\n", len(files))
	log.Printf("ListFiles, elapsed: %s", time.Since(start))
	return files, nil
}

func (c *Client) ListObjects(ctx context.Context) ([]types.Object, error) {
	start := time.Now()
	paginator := s3.NewListObjectsV2Paginator(c, c.input)
	objects := make([]types.Object, 0, c.cfg.Pagination.MaxKeys)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		objects = append(objects, page.Contents...)
		c.pagesCount++
	}
	log.Printf("ListObjects, elapsed: %s", time.Since(start))
	log.Printf("Files in bucket: %d", len(objects))
	return objects, nil
}
