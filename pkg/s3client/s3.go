package s3client

import (
	"context"
	"log"
	"runtime"
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
	cfg            *configuration.Configuration
	input          *s3.ListObjectsV2Input
	wg             *sync.WaitGroup
	objectsChan    chan types.Object
	PagesCount     int32
	goroutineCount int
}

func NewClient(ctx context.Context, cfg *configuration.Configuration) (*Client, error) {
	client := &Client{
		cfg: cfg,
		input: &s3.ListObjectsV2Input{
			Bucket:  aws.String(cfg.BucketName),
			Prefix:  aws.String(cfg.Prefix),
			MaxKeys: int32(cfg.Pagination.MaxKeys),
		},
		wg: &sync.WaitGroup{},
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
	c.objectsChan = make(chan types.Object, c.cfg.Pagination.MaxKeys)

	c.startObjectProcessor(ctx, data, cache)
	if err := c.processPages(ctx, paginator); err != nil {
		return err
	}

	c.waitForCompletion()

	log.Printf("ListObjects, elapsed: %s\n", time.Since(start))
	log.Printf("Files to download: %d\n", len(data.Objects))
	return nil
}

func (c *Client) startObjectProcessor(ctx context.Context, data *files.Objects, cache *cacher.FileCache) {
	for i := 0; i < runtime.NumCPU(); i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for object := range c.objectsChan {
				name := *object.Key
				if strings.HasSuffix(name, c.cfg.Extension) && strings.Contains(name, c.cfg.NameMask) {
					file := files.NewFile(object)
					cached := cache.HasFile(name, file)
					if !cached {
						data.Objects <- file
					} else {
						log.Printf("File %s did not pass the filter\n", name)
					}
					cache.RemoveFile(name)
				}
			}
		}()
	}
}

func (c *Client) processPages(ctx context.Context, paginator *s3.ListObjectsV2Paginator) error {
	var totalObjects int
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		c.sendObjectsToChannel(page.Contents)
		c.PagesCount++
		totalObjects += len(page.Contents)
		log.Printf("Page %d, %d objects recived from s3 and sent to channel\n", c.PagesCount, totalObjects)
	}
	return nil
}

func (c *Client) sendObjectsToChannel(objects []types.Object) {
	for _, object := range objects {
		c.objectsChan <- object
	}
}

func (c *Client) waitForCompletion() {
	close(c.objectsChan)
	c.wg.Wait()
}
