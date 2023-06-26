package main

import (
	"context"
	"log"
	"runtime"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/downloader"
	"s3-crawler/pkg/s3client"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := configuration.LoadConfig("../config.json")
	if err != nil {
		log.Fatal(err)
	}
	runtime.GOMAXPROCS(cfg.CPUWorker)
	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}
	files, err := client.ListObjects(ctx)
	if err != nil {
		log.Fatal(err)
	}
	manager := downloader.NewDownloader(client, cfg)
	manager.DownloadFiles(ctx, files)
}
