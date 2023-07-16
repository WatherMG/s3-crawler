package configuration

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"runtime"
	"time"

	"s3-crawler/pkg/files"
	"s3-crawler/pkg/utils"
)

const (
	DownloadersModifier uint16 = 1 << 9
	ChunkSize                  = 8 * files.MiB
)

// Configuration holds settings for connecting to S3 and downloading files.
type Configuration struct {
	S3Connection            S3ConnectionConfig `json:"s3Connection"`
	BucketName              string             `json:"bucketName"`              // BucketName is the name of the S3 bucket.
	Prefix                  string             `json:"s3prefix,omitempty"`      // Prefix is the prefix for files in the S3 bucket.
	Extension               string             `json:"extensions,omitempty"`    // Extension is the file extension to filter by.
	NameMask                string             `json:"nameMask,omitempty"`      // NameMask is a mask for filtering file names.
	LocalPath               string             `json:"downloadPath"`            // LocalPath is the local path to download files to.
	MaxFileSize             uint64             `json:"maxFileSizeMB,omitempty"` // MaxFileSize is the maximum file size in MB.
	MinFileSize             uint64             `json:"minFileSizeMB,omitempty"` // MinFileSize is the minimum file size in MB.
	Pagination              PaginationConfig   `json:"pagination"`
	Downloaders             uint16             `json:"downloaders,omitempty"` // Downloaders is the maximum number of concurrent goroutines for downloading files.
	Progress                Progress           `json:"progress,omitempty"`
	NumCPU                  uint8              `json:"numCPU,omitempty"`                // NumCPU controls the distribution of load on processor cores.
	IsDecompress            bool               `json:"decompress,omitempty"`            // IsDecompress specifies whether to decompress downloaded files.
	IsWithDirName           bool               `json:"withDirName"`                     // IsWithDirName specifies whether to include directory names in downloaded file paths.
	IsDeleteAfterDecompress bool               `json:"deleteAfterDecompress,omitempty"` // IsDeleteAfterDecompress specifies whether to delete downloaded files after decompression.
	IsHashWithParts         bool               `json:"withParts"`                       // IsHashWithParts specifies whether to include parts in hash calculation.
}

// S3ConnectionConfig holds settings for connecting to S3.
type S3ConnectionConfig struct {
	Endpoint        string `json:"endpoint"`        // Endpoint is the S3 endpoint URL.
	Region          string `json:"region"`          // Region is the S3 region.
	AccessKeyID     string `json:"accessKeyId"`     // AccessKeyID is the S3 access key ID.
	SecretAccessKey string `json:"secretAccessKey"` // SecretAccessKey is the S3 secret access key.
}

// PaginationConfig holds settings for pagination.
type PaginationConfig struct {
	// ChunkSize is the size of the chunks used when calculating the hash of a local file and when downloading large files.
	// This size is also used by the AWS S3 CLI when uploading or syncing files, and determines the hash calculated on the S3 bucket.
	ChunkSize int64  `json:"chunkSize,omitempty"`
	MaxPages  uint16 `json:"maxPages,omitempty"` // MaxPages is the maximum number of pages to retrieve.
	MaxKeys   uint16 `json:"maxKeys,omitempty"`  // MaxKeys is the maximum number of keys per page.
}

// Progress holds settings for progress reporting.
type Progress struct {
	Delay           time.Duration `json:"delay,omitempty"`           // Delay is the delay between progress updates.
	BarSize         uint8         `json:"barSize,omitempty"`         // BarSize is the size of the progress bar.
	WithProgressBar bool          `json:"withProgressBar,omitempty"` // WithProgressBar specifies whether to display a progress bar.
}

func NewConfiguration() *Configuration {
	return &Configuration{
		LocalPath:   "/tmp/crawler",
		NumCPU:      0,
		Downloaders: 0,
		Pagination: PaginationConfig{
			MaxKeys: 1000,
		},
		Progress: Progress{
			Delay:   500,
			BarSize: 20,
		},
	}
}

func LoadConfig(filename string) (*Configuration, error) {
	start := time.Now()

	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	cfg := NewConfiguration()
	if err = json.Unmarshal(file, &cfg); err != nil {
		return nil, err
	}

	// Validate S3Connection fields
	if cfg.S3Connection.Endpoint == "" {
		return nil, errors.New("endpoint must be provided")
	}
	if cfg.S3Connection.Region == "" {
		return nil, errors.New("region must be provided")
	}
	if cfg.S3Connection.AccessKeyID == "" {
		return nil, errors.New("accessKeyID must be provided")
	}
	if cfg.S3Connection.SecretAccessKey == "" {
		return nil, errors.New("secretAccessKey must be provided")
	}

	if cfg.LocalPath == "/tmp/crawler/" {
		log.Printf("Use default local path: %s\n", cfg.LocalPath)
	}
	if cfg.Pagination.MaxKeys <= 0 {
		cfg.Pagination.MaxKeys = 1000
	}
	if cfg.NumCPU <= 0 {
		cfg.NumCPU = uint8(runtime.NumCPU())
		log.Printf("Invalid value of NumCPU provided, using default value: %d\n", cfg.NumCPU)
	}
	cfg.validateDownloaders()
	cfg.validateChunkSize()

	log.Printf("Load config, elapsed: %s", time.Since(start))

	return cfg, err
}

func (config *Configuration) GetMinFileSize() int64 {
	if config.MinFileSize > 0 {
		return int64(config.MinFileSize * files.MiB)
	}
	return 0
}

func (config *Configuration) GetMaxFileSize() int64 {
	if config.MaxFileSize > 0 {
		return int64(config.MaxFileSize * files.MiB)
	}
	return 0
}

func (config *Configuration) GetDownloaders() int {
	return int(config.Downloaders)
}

func (config *Configuration) validateDownloaders() {
	switch {
	case config.Downloaders == 0:
		config.Downloaders = uint16(config.NumCPU) * DownloadersModifier
		log.Printf("Downloaders value not provided, using default value: %d\n", config.Downloaders)
	case config.Downloaders > 10000:
		config.Downloaders = 10000
		log.Printf("Invalid value of Downloaders provided, using max value: %d\n", config.Downloaders)
	}
}
func (config *Configuration) validateChunkSize() {
	switch {
	case config.Pagination.ChunkSize == 0:
		config.Pagination.ChunkSize = ChunkSize
		log.Printf("ChunkSize value not provided, using default value: %s\n", utils.FormatBytes(config.Pagination.ChunkSize))
	case config.Pagination.ChunkSize < 0:
		config.Pagination.ChunkSize = ChunkSize
		log.Printf("Invalid value of ChunkSize provided, using default value: %s\n", utils.FormatBytes(config.Pagination.ChunkSize))
	}
}
func (config *Configuration) GetChunkSize() int64 {
	return config.Pagination.ChunkSize
}
