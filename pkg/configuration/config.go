package configuration

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Configuration struct {
	S3Connection S3ConnectionConfig `json:"s3Connection"`
	BucketName   string             `json:"bucketName"`
	Prefix       string             `json:"prefix,omitempty"`
	Extension    string             `json:"extension,omitempty"`
	NameMask     string             `json:"nameMask,omitempty"`
	LocalPath    string             `json:"localPath"`
	Pagination   PaginationConfig   `json:"pagination"`
	CPUWorker    int                `json:"cpu_worker,omitempty"` // - отвечает за распределение нагрузки на ядра процессора
	Downloaders  int                `json:"downloaders,omitempty"`
	Compression  bool               `json:"compression,omitempty"`
}

type S3ConnectionConfig struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type PaginationConfig struct {
	MaxKeys int32 `json:"maxKeys,omitempty"`
}

func LoadConfig(filename string) (*Configuration, error) {
	start := time.Now()
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	cfg := &Configuration{}
	err = json.NewDecoder(file).Decode(cfg)
	log.Printf("Load config, elapsed: %s", time.Since(start))
	return cfg, err
}

func CheckBucket(client s3.HeadBucketAPIClient, bucketName string) error {
	start := time.Now()
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err := client.HeadBucket(context.TODO(), input)
	log.Printf("CheckBucket, elapsed: %s", time.Since(start))
	return err
}

type File struct {
	Key  string
	Size int64
}

var mu sync.Mutex

func ListFiles(client s3.ListObjectsV2APIClient, config *Configuration) ([]*File, error) {
	start := time.Now()
	var files []*File
	input := &s3.ListObjectsV2Input{
		Bucket:  &config.BucketName,
		Prefix:  &config.Prefix,
		MaxKeys: config.Pagination.MaxKeys,
	}
	paginator := s3.NewListObjectsV2Paginator(client, input)

	filesCh := make(chan *File, config.Pagination.MaxKeys)
	// objectCh := make(chan types.Object, config.Pagination.MaxKeys)

	var wg sync.WaitGroup

	for i := 0; i < config.CPUWorker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range filesCh {
				if strings.HasSuffix(file.Key, config.Extension) && strings.Contains(file.Key, config.NameMask) {
					mu.Lock()
					files = append(files, file)
					mu.Unlock()
				}
			}
		}()
	}

	var pagesWG sync.WaitGroup
	pageNum := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
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
				file := &File{
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
