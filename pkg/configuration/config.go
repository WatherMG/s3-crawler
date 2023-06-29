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
	Prefix       string             `json:"prefix,omitempty"`
	Extension    string             `json:"extension,omitempty"`
	NameMask     string             `json:"nameMask,omitempty"`
	LocalPath    string             `json:"localPath"`
	Pagination   PaginationConfig   `json:"pagination"`
	CPUWorker    int                `json:"cpu_worker,omitempty"`  // - отвечает за распределение нагрузки на ядра процессора
	Downloaders  int                `json:"downloaders,omitempty"` // - максимальное количество одновременно запущенных горутин для скачивания файлов
	Compression  bool               `json:"compression,omitempty"`
}

type S3ConnectionConfig struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type PaginationConfig struct {
	MaxKeys uint32 `json:"maxKeys,omitempty"`
}

func LoadConfig(filename string) (*Configuration, error) {
	start := time.Now()
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	cfg := &Configuration{}
	err = json.Unmarshal(file, &cfg)
	log.Printf("Load config, elapsed: %s", time.Since(start))
	return cfg, err
}
