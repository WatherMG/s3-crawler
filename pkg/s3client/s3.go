package s3client

import (
	"context"
	"errors"
	"fmt"
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
	cfg          *configuration.Configuration // Configuration for the S3 client.
	input        *s3.ListObjectsV2Input       // Input for the ListObjectsV2 operation.
	wg           *sync.WaitGroup              // WaitGroup to wait for goroutines to finish.
	pagesCount   uint32                       // Number of pages processed by the paginator.
	acceleration bool
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
			log.Fatal(err)
		}
		s3Client.Client = s3.NewFromConfig(defaultConfig)
	})

	return s3Client, err
}

// CheckBucket checks if the bucket specified in the configuration exists and is accessible.
func (client *Client) CheckBucket(ctx context.Context) error {
	if client.cfg.BucketName == "" {
		return errors.New("s3.CheckBucket: bucketName is required")
	}
	input := &s3.HeadBucketInput{
		Bucket: aws.String(client.cfg.BucketName),
	}

	_, err := client.HeadBucket(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get bucket, %w", err)
	}

	log.Printf("Bucket exist: %s", client.cfg.BucketName)

	resp, err := client.GetBucketAccelerateConfiguration(ctx, &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String(client.cfg.BucketName)})
	if err != nil {
		return fmt.Errorf("failed to get bucket accelerate configuration, %w", err)
	}

	if resp.Status == types.BucketAccelerateStatusEnabled {
		client.acceleration = true
		log.Println("Transfer Acceleration is enabled on the bucket.")
	} else {
		client.acceleration = false
		log.Println("Transfer Acceleration is not enabled on the bucket.")
	}
	return err
}

// ListObjects lists objects from the bucket specified in the configuration and
// sends them to be processed.
func (client *Client) ListObjects(ctx context.Context, data *files.Objects, cache *cacher.FileCache) error {
	start := time.Now()
	if err := client.processPages(ctx, data, cache); err != nil {
		return err
	}
	fmt.Printf("\u001B[2K\nListObjects: recive all objects in bucket. Need to download [%d] file(s). Elapsed: %s\n", data.Count(), time.Since(start))
	return nil
}

// processPages processes pages of results returned by the paginator and sends items to be processed.
func (client *Client) processPages(ctx context.Context, data *files.Objects, cache *cacher.FileCache) error {
	paginator := s3.NewListObjectsV2Paginator(client, client.input, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.StopOnDuplicateToken = true
	})

	pool := NewWorkerPool(int(client.cfg.Pagination.MaxKeys), func(object types.Object) {
		client.sendObjectsToChannel(object, data, cache)
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx, func(o *s3.Options) {
			o.UseAccelerate = client.acceleration
		})
		if err != nil {
			return fmt.Errorf("paginator error: %w", err)
		}

		for _, object := range page.Contents {
			pool.AddFileFromObject(object)
		}

		client.pagesCount++
	}
	pool.Wait()
	close(data.ProcessedChan)
	return nil
}

// sendObjectsToChannel sends items to be processed through the objectsChan channel.
func (client *Client) sendObjectsToChannel(object types.Object, data *files.Objects, cache *cacher.FileCache) {
	name := strings.ReplaceAll(*object.Key, "/", "_")
	if strings.HasSuffix(name, client.cfg.Extension) && strings.Contains(name, client.cfg.NameMask) && object.Size >= client.cfg.GetMinFileSize() {
		etag := strings.Trim(*object.ETag, "\"")
		downloaded := cache.HasFile(name, etag, object.Size)
		if !downloaded {
			file := files.NewFileFromObject(object)
			data.AddFile(file)
		}
		cache.RemoveFile(name)
	}
}

func (client *Client) GetPagesCount() uint32 {
	return client.pagesCount
}
