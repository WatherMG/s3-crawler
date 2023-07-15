package configuration

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"s3-crawler/pkg/files"
)

type Configuration struct {
	S3Connection            S3ConnectionConfig `json:"s3Connection"`
	BucketName              string             `json:"bucketName"`
	Prefix                  string             `json:"s3prefix,omitempty"`
	Extension               string             `json:"extensions,omitempty"`
	NameMask                string             `json:"nameMask,omitempty"`
	LocalPath               string             `json:"downloadPath"`
	MaxFileSize             int64              `json:"maxFileSizeMB,omitempty"`
	MinFileSize             int64              `json:"minFileSizeMB,omitempty"`
	Downloaders             int                `json:"downloaders,omitempty"` // - максимальное количество одновременно запущенных горутин для скачивания файлов
	Pagination              PaginationConfig   `json:"pagination"`
	Progress                Progress           `json:"progress,omitempty"`
	NumCPU                  uint8              `json:"numCPU,omitempty"` // - отвечает за распределение нагрузки на ядра процессора
	IsDecompress            bool               `json:"decompress,omitempty"`
	IsWithDirName           bool               `json:"withDirName"`
	IsDeleteAfterDecompress bool               `json:"deleteAfterDecompress,omitempty"`
	HashWithParts           bool               `json:"withParts"`
}

type S3ConnectionConfig struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type PaginationConfig struct {
	MaxPages uint32 `json:"maxPages,omitempty"`
	MaxKeys  uint16 `json:"maxKeys,omitempty"`
}

type Progress struct {
	BarSize         int           `json:"barSize,omitempty"`
	Delay           time.Duration `json:"delay,omitempty"`
	WithProgressBar bool          `json:"withProgressBar,omitempty"`
}

func LoadConfig(filename string) (*Configuration, error) {
	start := time.Now()

	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	cfg := &Configuration{}
	if err = json.Unmarshal(file, &cfg); err != nil {
		return nil, err
	}
	if cfg.LocalPath == "" {
		cfg.LocalPath = "/tmp/crawler/"
	}
	if cfg.Pagination.MaxKeys <= 0 {
		cfg.Pagination.MaxKeys = 1000
	}

	log.Printf("Load config, elapsed: %s", time.Since(start))

	return cfg, err
}

func (config *Configuration) GetMinFileSize() int64 {
	if config.MinFileSize > 0 {
		return config.MinFileSize * files.MiB
	}
	return 0
}

func (config *Configuration) GetMaxFileSize() int64 {
	if config.MaxFileSize > 0 {
		return config.MaxFileSize * files.MiB
	}
	return 0
}
