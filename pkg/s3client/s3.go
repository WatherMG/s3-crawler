package s3client

import (
	"context"
	"log"
	"strings"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client struct {
	*s3.Client
	Cfg        *configuration.Configuration
	Objects    chan types.Object
	input      *s3.ListObjectsV2Input
	pagesCount int32
}

func NewClient(ctx context.Context, cfg *configuration.Configuration) (*Client, error) {
	client := &Client{
		Cfg:     cfg,
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
		Bucket: aws.String(c.Cfg.BucketName),
	}
	log.Printf("Bucket name: %s", *input.Bucket)
	_, err := c.HeadBucket(ctx, input)
	return err
}

func (c *Client) ListObjects(ctx context.Context) (*files.Files, error) {
	start := time.Now()
	paginator := s3.NewListObjectsV2Paginator(c, c.input)
	objects := make(chan types.Object)
	data := &files.Files{
		Objects:    make([]*files.Object, 0, c.Cfg.Pagination.MaxKeys),
		TotalBytes: 0,
	}

	go func() {
		for object := range objects {
			if strings.HasSuffix(*object.Key, c.Cfg.Extension) && strings.Contains(*object.Key, c.Cfg.NameMask) {
				file := &files.Object{
					Key:  *object.Key,
					Size: object.Size,
				}
				data.Objects = append(data.Objects, file)
				data.TotalBytes += file.Size
			}
		}
		close(objects)
	}()

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			objects <- object
		}
		c.pagesCount++
	}

	log.Printf("ListObjects, elapsed: %s", time.Since(start))
	log.Printf("Files in bucket: %d", len(data.Objects))
	return data, nil
}
