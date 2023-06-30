package configuration

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

type Configuration struct {
	S3Connection S3ConnectionConfig `json:"s3Connection"`
	BucketName   string             `json:"bucketName"`
	Prefix       string             `json:"s3prefix,omitempty"`
	Extension    string             `json:"extension,omitempty"`
	NameMask     string             `json:"nameMask,omitempty"`
	LocalPath    string             `json:"localPath"`
	Pagination   PaginationConfig   `json:"pagination"`
	Downloaders  uint16             `json:"downloaders,omitempty"` // - максимальное количество одновременно запущенных горутин для скачивания файлов
	CPUWorker    uint8              `json:"cpuWorker,omitempty"`   // - отвечает за распределение нагрузки на ядра процессора
	Compression  bool               `json:"compression,omitempty"`
}

type S3ConnectionConfig struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type PaginationConfig struct {
	MaxKeys uint16 `json:"maxKeys,omitempty"`
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

	log.Printf("Load config, elapsed: %s", time.Since(start))

	return cfg, err
}
