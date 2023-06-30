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
	const fileCount = 12945 // in bucket
	cfg, _ := configuration.LoadConfig("../config.json")
	client, _ := s3client.NewClient(context.Background(), cfg)
	data := files.GetInstance(fileCount)
	cache := cacher.GetInstance()

	for i := 0; i < 3; i++ {
		fmt.Println("----------------------------------------")
		err := client.ListObjects(context.Background(), data, cache)
		if err != nil {
			t.Errorf("ListObjects error: %v", err)
		}
		if len(data.Objects) != fileCount {
			t.Errorf("Expected 253 files, got %d", len(data.Objects))
		}
		for len(data.Objects) != 0 {
			<-data.Objects
		}
		log.Printf("Reset data. Len of objects in channel %d\n", len(data.Objects))
		client.PagesCount = 0
	}
}
