package main

import (
	"context"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"s3-crawler/pkg/cacher"
	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/s3client"
)

func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // TODO: add timeout to config
	defer cancel()

	cfg, err := configuration.LoadConfig("../config.json") // TODO: add config path to config
	if err != nil {
		log.Fatal(err)
	}
	runtime.GOMAXPROCS(cfg.CPUWorker)
	if err = createPath(cfg.LocalPath); err != nil {
		log.Fatal(err)
	}

	cache := cacher.NewCache() // TODO: add chacher if true to config
	start := time.Now()
	if err = cache.LoadFromDir(cfg.LocalPath); err != nil {
		log.Fatal(err)
	}
	log.Printf("Cache loaded from %s\n", time.Since(start))
	log.Printf("Files in cache: %d\n", cache.Count)

	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}

	data := files.NewObjects(cfg.Pagination.MaxKeys * 15) // TODO reduce debug value for buffer

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = client.ListObjects(ctx, data, cache); err != nil {
			log.Fatal(err) // TODO: разобраться почему горутины не синхронизованы и кеш отрабатывает не корректно = рандом количество файлов.
		}
	}()

	/*manager := downloader.NewDownloader(client, cfg)
	wg.Add(1)
	go func(cache *cacher.FileCache) {
		defer wg.Done()
		manager.DownloadFiles(ctx, data)
	}(cache)*/

	wg.Wait()
}

func createPath(path string) error {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		log.Printf("MkdirAll error: %v", err)
	}
	return err
}
