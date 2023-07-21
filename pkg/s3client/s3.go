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
	"s3-crawler/pkg/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	timeout     = 3 * time.Second
	maxAttempts = 3
)

// Client represents an S3 client.
type Client struct {
	*s3.Client
	cfg          *configuration.Configuration // Configuration for the S3 client.
	input        *s3.ListObjectsV2Input       // Input for the ListObjectsV2 operation.
	wg           sync.WaitGroup               // WaitGroup to wait for goroutines to finish.
	extensions   []string
	nameMask     string
	minSize      int64
	maxSize      int64
	maxPages     int
	pagesCount   int // Number of pages processed by the paginator.
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
			wg:         sync.WaitGroup{},
			minSize:    cfg.GetMinFileSize(),
			maxSize:    cfg.GetMaxFileSize(),
			extensions: strings.Split(cfg.Extension, ","),
			nameMask:   strings.ToLower(cfg.NameMask),
			maxPages:   int(cfg.Pagination.MaxPages),
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
			config.WithClientLogMode(aws.LogRetries),
			config.WithRetryMode(aws.RetryModeStandard),
			config.WithRetryMaxAttempts(0),
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
	err := client.doRequestWithRetry(ctx, func(reqCtx context.Context) error {
		input := &s3.HeadBucketInput{
			Bucket: aws.String(client.cfg.BucketName),
		}

		_, err := client.HeadBucket(reqCtx, input)
		return err
	})
	if err != nil {
		return err
	}
	log.Printf("Bucket exist: %s", client.cfg.BucketName)

	var resp *s3.GetBucketAccelerateConfigurationOutput
	err = client.doRequestWithRetry(ctx, func(reqCtx context.Context) error {
		var err error
		resp, err = client.GetBucketAccelerateConfiguration(reqCtx, &s3.GetBucketAccelerateConfigurationInput{
			Bucket: aws.String(client.cfg.BucketName),
		})
		return err
	})
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
func (client *Client) ListObjects(ctx context.Context, data *files.FileCollection, cache *cacher.FileCache) error {
	start := time.Now()
	if err := client.processPages(ctx, cache, data); err != nil {
		return err
	}

	fmt.Printf("Recive all objects in bucket. Need to download [%d] file(s). Elapsed: %s\n", data.Count(), time.Since(start))
	cache.Clear()
	return nil
}

// processPages processes pages of results returned by the paginator and sends items to be processed.
func (client *Client) processPages(ctx context.Context, cache *cacher.FileCache, data *files.FileCollection) error {
	paginator := s3.NewListObjectsV2Paginator(client, client.input, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.StopOnDuplicateToken = true
	})

	if client.maxPages == 0 {
		client.maxPages = -1
	}

	for paginator.HasMorePages() && client.maxPages != client.pagesCount {
		page, err := client.getPageWithRetry(ctx, paginator)
		if err != nil {
			return fmt.Errorf("paginator error: %w", err)
		}

		for _, object := range page.Contents {
			client.sendObjectsToMap(object, cache, data)
		}

		client.pagesCount++
		fmt.Printf("\rPage %d", client.pagesCount)
	}

	return nil
}

func (client *Client) getPageWithRetry(ctx context.Context, paginator *s3.ListObjectsV2Paginator) (page *s3.ListObjectsV2Output, err error) {
	err = client.doRequestWithRetry(ctx, func(reqCtx context.Context) error {
		page, err = paginator.NextPage(reqCtx, func(o *s3.Options) {
			o.UseAccelerate = client.acceleration
		})
		return err
	})
	return page, err
}

func (client *Client) doRequestWithRetry(ctx context.Context, f func(context.Context) error) error {
	var err error
	for i := 0; i < maxAttempts; i++ {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		err = f(reqCtx)
		cancel()
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			break
		}
		log.Printf("Request timed out, retrying... (attempt %d/%d)", i+1, maxAttempts)
		time.Sleep(1 * time.Second)
	}
	return err
}

// sendObjectsToMap verify items and sends it's in the progressMap.
func (client *Client) sendObjectsToMap(object types.Object, cache *cacher.FileCache, data *files.FileCollection) {
	if name, valid := client.isValidObject(object); valid {
		etag := strings.Trim(*object.ETag, "\"")
		downloaded := cache.HasFile(name, etag, object.Size)
		if !downloaded {
			file := files.NewFileFromObject(object, client.cfg.LocalPath)
			data.AddToProgress(file)
		}
		cache.RemoveFile(name)
	}
}

func (client *Client) isValidObject(object types.Object) (string, bool) {
	// Normalize the object key by replacing slashes with underscores and converting to lowercase
	name := strings.ToLower(strings.ReplaceAll(*object.Key, "/", "_"))

	// Check if the object has a valid file extension
	hasValidExt := utils.HasValidExtension(name, client.extensions)

	// Check if the object has a valid name
	hasValidName := utils.HasValidName(name, client.nameMask)

	// Check if the object has a valid size
	hasValidSize := utils.HasValidSize(object.Size, client.minSize, client.maxSize)

	return name, hasValidExt && hasValidName && hasValidSize
}

func (client *Client) GetPagesCount() int {
	return client.pagesCount
}
