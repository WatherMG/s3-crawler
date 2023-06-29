package s3client

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/cacher"
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
	cfg        *configuration.Configuration
	Objects    chan types.Object
	input      *s3.ListObjectsV2Input
	PagesCount int32
	wg         sync.WaitGroup
}

func NewClient(ctx context.Context, cfg *configuration.Configuration) (*Client, error) {
	client := &Client{
		cfg:     cfg,
		Objects: make(chan types.Object, cfg.Pagination.MaxKeys),
		input: &s3.ListObjectsV2Input{
			Bucket:  aws.String(cfg.BucketName),
			Prefix:  aws.String(cfg.Prefix),
			MaxKeys: int32(cfg.Pagination.MaxKeys),
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
	log.Printf("Bucket name: %s", *input.Bucket)
	_, err := c.HeadBucket(ctx, input)
	return err
}

// ListObjects lists objects from an S3 bucket using the ListObjectsV2 API. It
// uses a paginator to retrieve the objects in pages and sends them to a
// channel. A separate goroutine receives the objects from the channel, checks
// if they meet certain criteria (based on their key), and sends them to another
// channel if they are not in the cache. The function also keeps track of the
// number of pages and logs some information.
func (c *Client) ListObjects(ctx context.Context, data *files.Objects, cache *cacher.FileCache) error {
	start := time.Now()
	paginator := s3.NewListObjectsV2Paginator(c, c.input)

	c.wg.Add(1)
	go func(data *files.Objects) {
		defer c.wg.Done()
	loop:
		for {
			select {
			case object, ok := <-c.Objects:
				if !ok {
					// closed channel
					break loop
				}
				if strings.HasSuffix(*object.Key, c.cfg.Extension) && strings.Contains(*object.Key, c.cfg.NameMask) {
					newFile := files.NewFile(object)
					cached := cache.HasFile(newFile.Key, newFile)
					if !cached {
						data.Objects <- newFile
					}
					cache.RemoveFile(newFile.Key)
				}
			case <-ctx.Done():
				return
			}
		}
	}(data)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for _, object := range page.Contents {
				c.Objects <- object
			}
		}()
		c.PagesCount++
		log.Printf("Page %d", c.PagesCount)
		log.Printf("Items: %d", page.KeyCount)
	}

	go func() {
		c.wg.Wait()
		close(c.Objects)
	}()

	log.Printf("ListObjects, elapsed: %s\n", time.Since(start))
	log.Printf("Files to download: %d\n", len(data.Objects))
	return nil
}
