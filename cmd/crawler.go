package main

import (
	"context"
	"fmt"
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

	cache := cacher.GetInstance() // TODO: add chacher if true to config
	start := time.Now()
	if err = cache.LoadFromDir(cfg); err != nil {
		log.Fatal(err)
	}
	log.Printf("Cache loaded from %s\n", time.Since(start))
	log.Printf("Total files in cache: %d\n", cache.TotalCount)

	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}

	data := files.GetInstance(cfg.Pagination.MaxKeys * 15) // TODO set the cfg.Downloaders for optimizations

	/*manager := downloader.NewDownloader(client, cfg)
	wg.Add(1)
	go func(cache *cacher.FileCache) {
		defer wg.Done()
		manager.DownloadFiles(ctx, data)
	}(cache)*/

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = client.ListObjects(ctx, data, cache); err != nil {
			log.Fatal(err) // TODO: разобраться почему горутины не синхронизованы и кеш отрабатывает не корректно = рандом количество файлов.
		}
	}()

	wg.Wait()
	memstat(data)
}

func createPath(path string) error {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		log.Printf("MkdirAll error: %v", err)
	}
	return err
}
func memstat(data *files.Objects) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	f, err := os.OpenFile("mem.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "------------------------------------\nMem for file Pool in listobjects\n")
	fmt.Fprintf(f, "With %d objects in bucket\n", len(data.Objects))

	fmt.Fprintf(f, "Alloc = %v MiB\n", m.Alloc/1024/1024)
	fmt.Fprintf(f, "TotalAlloc = %v MiB\n", m.TotalAlloc/1024/1024)
	fmt.Fprintf(f, "Sys = %v MiB\n", m.Sys/1024/1024)
	fmt.Fprintf(f, "NumGC = %v\n", m.NumGC)
}
