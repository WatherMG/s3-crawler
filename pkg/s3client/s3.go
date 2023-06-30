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

// Client represents an S3 client.
type Client struct {
	*s3.Client
	cfg         *configuration.Configuration // Configuration for the S3 client.
	input       *s3.ListObjectsV2Input       // Input for the ListObjectsV2 operation.
	wg          *sync.WaitGroup              // WaitGroup to wait for goroutines to finish.
	objectsChan chan types.Object            // Channel to send objects to be processed.
	PagesCount  int32                        // Number of pages processed by the paginator.
}

var s3Client *Client
var once sync.Once

// NewClient creates a new S3 client with the given context and configuration.
// It ensures that only one instance of the S3 client is created using the
// Singleton pattern.
func NewClient(ctx context.Context, cfg *configuration.Configuration) (*Client, error) {
	var err error

	once.Do(func() {
		s3Client = &Client{
			cfg: cfg,
			input: &s3.ListObjectsV2Input{
				Bucket:  aws.String(cfg.BucketName),    // The name of the bucket to list objects from.
				Prefix:  aws.String(cfg.Prefix),        // The prefix of the keys to list objects from.
				MaxKeys: int32(cfg.Pagination.MaxKeys), // The maximum number of keys to return in each page of results.
			},
			wg: &sync.WaitGroup{},
		}
		defaultConfig, err := config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{
						URL:           cfg.S3Connection.Endpoint, // The endpoint URL to use for the S3 service.
						SigningRegion: cfg.S3Connection.Region,   // The region to use for signing requests.
					}, nil
				},
			)),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				cfg.S3Connection.AccessKeyID,     // The access key ID to use for authentication.
				cfg.S3Connection.SecretAccessKey, // The secret access key to use for authentication.
				"",
			)),
			config.WithRegion(cfg.S3Connection.Region), // The region to use for the S3 service.
		)
		if err != nil {
			return
		}
		s3Client.Client = s3.NewFromConfig(defaultConfig)
	})

	return s3Client, err
}

// CheckBucket checks if the bucket specified in the configuration exists and is accessible.
func (c *Client) CheckBucket(ctx context.Context) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(c.cfg.BucketName),
	}
	log.Printf("Bucket name: %s", *input.Bucket)
	_, err := c.HeadBucket(ctx, input)

	return err
}

// ListObjects lists objects from the bucket specified in the configuration and
// sends them to be processed.
func (c *Client) ListObjects(ctx context.Context, data *files.Objects, cache *cacher.FileCache) error {
	start := time.Now()
	paginator := s3.NewListObjectsV2Paginator(c, c.input)
	c.objectsChan = make(chan types.Object, c.cfg.CPUWorker*2)

	c.startObjectProcessor(ctx, data, cache)

	if err := c.processPages(ctx, paginator); err != nil {
		return err
	}

	c.waitForCompletion()
	close(data.Objects)

	log.Printf("ListObjects: elapsed: %s\n", time.Since(start))
	log.Printf("ListObjects: Files not in cache, need download: %d\n", len(data.Objects))

	return nil
}

// startObjectProcessor starts workers that process items sent through the
// objectsChan channel. Workers check if an item is already in the cache and if
// not, they send it to be downloaded. They also remove downloaded files from the
// cache.
func (c *Client) startObjectProcessor(ctx context.Context, data *files.Objects, cache *cacher.FileCache) {
	for i := uint8(0); i < c.cfg.CPUWorker; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for object := range c.objectsChan {
				select {
				case <-ctx.Done():
					close(data.Objects)
					return
				default:
					name := *object.Key
					if strings.HasSuffix(name, c.cfg.Extension) && strings.Contains(name, c.cfg.NameMask) {
						file := files.NewFileFromObject(object)
						cached := cache.HasFile(file)
						if !cached {
							data.Objects <- file
						}
						cache.RemoveFile(name)
					}
				}
			}
		}()
	}
}

// processPages processes pages of results returned by the paginator and sends items to be processed.
func (c *Client) processPages(ctx context.Context, paginator *s3.ListObjectsV2Paginator) error {
	var totalObjects int

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		if err = c.sendObjectsToChannel(ctx, page.Contents); err != nil {
			return err
		}

		c.PagesCount++

		totalObjects += len(page.Contents)
		log.Printf("ProcessPages: Page %d. %d objects recived from s3 and sent to channel\n", c.PagesCount, totalObjects)
	}
	return nil
}

// sendObjectsToChannel sends items to be processed through the objectsChan channel.
func (c *Client) sendObjectsToChannel(ctx context.Context, objects []types.Object) error {
	for _, object := range objects {
		select {
		case c.objectsChan <- object:
		case <-ctx.Done():
			close(c.objectsChan)
			return ctx.Err()
		}
	}

	return nil
}

// waitForCompletion waits for all goroutines to finish processing items.
func (c *Client) waitForCompletion() {
	close(c.objectsChan)
	c.wg.Wait()
}
