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
	maxDownloaders           = 9000
	maxGoroutines     uint16 = 512
	defaultGoroutines uint16 = 128
	midCPUThresh      uint8  = 16

	defaultDelay   = 1000 * time.Millisecond
	defaultBarSize = 20

	defaultMaxKeys = 1000
	ChunkSizeMB    = 8 * files.MiB

	defaultPath = "/tmp/crawler"
)

// Configuration holds settings for connecting to S3 and downloading files.
type Configuration struct {
	S3Connection    S3ConnectionConfig `json:"s3Connection"`
	BucketName      string             `json:"bucketName"`              // BucketName is the name of the S3 bucket.
	Prefix          string             `json:"s3prefix,omitempty"`      // Prefix is the prefix for files in the S3 bucket.
	Extension       string             `json:"extensions,omitempty"`    // Extension is the file extension to filter by.
	NameMask        string             `json:"nameMask,omitempty"`      // NameMask is a mask for filtering file names.
	LocalPath       string             `json:"downloadPath"`            // LocalPath is the local path to download files to.
	MaxFileSize     uint64             `json:"maxFileSizeMB,omitempty"` // MaxFileSize is the maximum file size in MB.
	MinFileSize     uint64             `json:"minFileSizeMB,omitempty"` // MinFileSize is the minimum file size in MB.
	Pagination      PaginationConfig   `json:"pagination"`
	Downloaders     uint16             `json:"downloaders,omitempty"`  // Downloaders is the maximum number of concurrent goroutines for downloading files.
	NumCPU          uint8              `json:"numCPU,omitempty"`       // NumCPU controls the distribution of load on processor cores.
	IsDecompress    bool               `json:"decompress,omitempty"`   // IsDecompress specifies whether to decompress downloaded files.
	IsWithDirName   bool               `json:"decompressWithDirName"`  // IsWithDirName specifies whether to include directory names in downloaded file paths.
	IsSaveArchives  bool               `json:"saveArchives,omitempty"` // IsSaveArchives specifies whether to delete downloaded files after decompression.
	IsHashWithParts bool               `json:"withParts"`              // IsHashWithParts specifies whether to include parts in hash calculation.
	IsFlattenName   bool               `json:"isFlattenName"`
	Progress        Progress           `json:"progress,omitempty"`
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
	ChunkSize int64  `json:"chunkSizeMB,omitempty"`
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
		LocalPath: defaultPath,
		Pagination: PaginationConfig{
			MaxKeys:   defaultMaxKeys,
			ChunkSize: ChunkSizeMB,
		},
		Progress: Progress{
			Delay:   defaultDelay,
			BarSize: defaultBarSize,
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

	if err = cfg.validateS3creds(); err != nil {
		return nil, err
	}

	if cfg.LocalPath == defaultPath {
		log.Printf("Using default local path: %s.\n", cfg.LocalPath)
	} else {
		log.Printf("Local path is provided. Using local path: %s.\n", cfg.LocalPath)
	}
	if cfg.Pagination.MaxKeys <= 0 {
		cfg.Pagination.MaxKeys = defaultMaxKeys
	}
	if cfg.NumCPU <= 0 {
		cfg.NumCPU = uint8(runtime.NumCPU())
		log.Printf("Invalid value of NumCPU provided, using default value: %d.\n", cfg.NumCPU)
	}
	cfg.validateDownloaders()
	cfg.validateChunkSize()

	log.Printf("Load config, elapsed: %s.\n", time.Since(start).Truncate(time.Millisecond))

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

func (config *Configuration) calcGoroutinesForCores(cores uint8) uint16 {
	switch {
	case cores == 1:
		return defaultGoroutines
	case cores >= 2 && cores <= midCPUThresh:
		return defaultGoroutines + (maxGoroutines >> 3)
	case cores > midCPUThresh:
		if config.IsDecompress {
			return defaultGoroutines + (maxGoroutines >> 3)
		}
		return maxGoroutines
	default:
		log.Printf("Invalid numCPU, setting to default goroutines: %d.\n", defaultGoroutines)
		return defaultGoroutines
	}
}

func (config *Configuration) GetDownloaders() int {
	return int(config.Downloaders)
}

func (config *Configuration) validateS3creds() error {
	// Validate S3Connection fields
	if config.S3Connection.Endpoint == "" {
		return errors.New("endpoint must be provided")
	}
	if config.S3Connection.Region == "" {
		return errors.New("region must be provided")
	}
	if config.S3Connection.AccessKeyID == "" {
		return errors.New("accessKeyID must be provided")
	}
	if config.S3Connection.SecretAccessKey == "" {
		return errors.New("secretAccessKey must be provided")
	}
	return nil
}

func (config *Configuration) validateDownloaders() {
	switch {
	case config.Downloaders == 0:
		config.Downloaders = config.calcGoroutinesForCores(config.NumCPU)
		log.Printf("Downloaders value not provided, using default value: %d.\n", config.Downloaders)
	case config.Downloaders > maxDownloaders:
		config.Downloaders = maxGoroutines
		log.Printf("Invalid value of Downloaders provided, using max value: %d.\n", config.Downloaders)
	}
}

func (config *Configuration) validateChunkSize() {
	chunkSize := config.Pagination.ChunkSize * files.MiB
	switch {
	case chunkSize == 0:
		config.Pagination.ChunkSize = ChunkSizeMB
		log.Printf("ChunkSizeMB value not provided, using default value: %s.\n", utils.FormatBytes(config.Pagination.ChunkSize))
	case chunkSize < 0:
		config.Pagination.ChunkSize = ChunkSizeMB
		log.Printf("Invalid value of ChunkSizeMB provided, using default value: %s.\n", utils.FormatBytes(config.Pagination.ChunkSize))
	default:
		config.Pagination.ChunkSize = chunkSize
		log.Printf("ChunkSizeMB value is provided, using value: %s.\n", utils.FormatBytes(config.Pagination.ChunkSize))
	}
}
func (config *Configuration) GetChunkSize() int64 {
	return config.Pagination.ChunkSize
}
