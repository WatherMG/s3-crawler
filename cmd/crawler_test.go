package main

import (
	"context"
	"fmt"
	"log"
	"testing"

	"s3-crawler/pkg/cacher"
	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/s3client"
)

func TestListObjects(t *testing.T) {
	const fileCount = 253 // in bucket

	cfg, _ := configuration.LoadConfig("../config.json")
	client, _ := s3client.NewClient(context.Background(), cfg)
	data := files.NewObject(fileCount)
	cache := cacher.NewCache()

	for i := 0; i < 3; i++ {
		fmt.Println("----------------------------------------")

		err := client.ListObjects(context.Background(), data, cache)
		if err != nil {
			t.Errorf("ListObjects error: %v", err)
		}

		if len(data.DownloadChan) != fileCount {
			t.Errorf("Expected 253 files, got %d", len(data.DownloadChan))
		}

		for len(data.DownloadChan) != 0 {
			<-data.DownloadChan
		}
		log.Printf("Reset data. Len of objects in channel %d\n", len(data.DownloadChan))

		client.PagesCount = 0
	}
}
