package config

import (
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	S3Connection S3ConnectionConfig `json:"s3Connection"`
	BucketName   string             `json:"bucketName"`
	Prefix       string             `json:"prefix,omitempty"`
	Extension    string             `json:"extension,omitempty"`
	NameMask     string             `json:"nameMask,omitempty"`
	Pagination   PaginationConfig   `json:"pagination"`
	Downloaders  int                `json:"downloaders,omitempty"`
	Workers      int                `json:"workers,omitempty"`
	Compression  bool               `json:"compression,omitempty"`
	LocalPath    string             `json:"localPath"`
}

type S3ConnectionConfig struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type PaginationConfig struct {
	MaxKeys int `json:"maxKeys,omitempty"`
}

func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	cfg := &Config{}
	err = json.NewDecoder(file).Decode(cfg)
	return cfg, err
}

func CreateS3Client(config *Config) (*s3.Client, error) {
	cfg, err := config
}
