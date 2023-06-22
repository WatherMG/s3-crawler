package main

import (
	"context"
	"fmt"
	"log"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/s3client"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := configuration.LoadConfig("../config.json")
	if err != nil {
		log.Fatal(err)
	}
	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}
	files, err := client.ListFiles(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(files))
	/*manager := downloader.NewDownloader(client.Client, cfg, len(files))
	if err = manager.Download(ctx, files); err != nil {
		log.Fatal(err)
	}*/
}
