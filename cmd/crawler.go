package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"s3-crawler/pkg/cacher"
	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/downloader"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/s3client"
)

func main() {
	f, err := os.Create("mem.prof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	runTime := time.Now()

	fi, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer fi.Close()

	// Начинаем трассировку
	err = trace.Start(fi)
	if err != nil {
		panic(err)
	}
	defer trace.Stop()

	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute) // TODO: add timeout to config
	defer cancel()

	cfg, err := configuration.LoadConfig("../config.json") // TODO: add config path to config
	if err != nil {
		log.Fatal(err)
	}

	if err = createPath(cfg.LocalPath); err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(cfg.LocalPath)

	cache := cacher.NewCache()
	if err = cache.LoadFromDir(cfg); err != nil {
		log.Fatal(err)
	}

	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	manager := downloader.NewDownloader(client, cfg)

	runtime.GOMAXPROCS(int(cfg.NumCPU))

	data := files.NewObject(cfg.Pagination.MaxKeys * 10)

	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = manager.DownloadFiles(ctx, data); err != nil {
			log.Println(err)
		}
	}()

	if err = client.ListObjects(ctx, data, cache); err != nil {
		log.Fatal(err)
	}

	cache.Clear()

	wg.Wait()
	memstat(data)
	fmt.Printf("Programm running total %s", time.Since(runTime))
	runtime.GC() // get up-to-date statistics
	pprof.WriteHeapProfile(f)
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
	fmt.Fprintf(f, "With %d objects in bucket\n", len(data.DownloadChan))

	fmt.Fprintf(f, "Alloc = %v MiB\n", m.Alloc/files.MiB)
	fmt.Fprintf(f, "TotalAlloc = %v MiB\n", m.TotalAlloc/files.MiB)
	fmt.Fprintf(f, "Sys = %v MiB\n", m.Sys/files.MiB)
	fmt.Fprintf(f, "NumGC = %v\n", m.NumGC)
}

/*func main() {
	// Load configuration
	cfg, err := configuration.LoadConfig("../config.json")
	if err != nil {
		log.Fatal(err)
	}

	// Set GOMAXPROCS
	runtime.GOMAXPROCS(int(cfg.NumCPU))

	// Create local path if it doesn't exist
	if err = createPath(cfg.LocalPath); err != nil {
		log.Fatal(err)
	}

	// Create a new cache
	cache := cacher.NewCache()

	// Load cache from local directory
	start := time.Now()
	if err = cache.LoadFromDir(cfg); err != nil {
		log.Fatal(err)
	}
	log.Printf("Cache loaded from %s\n", time.Since(start))
	log.Printf("Total files in cache: %d\n", cache.totalCount)

	// Create a new S3 client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Check if the bucket exists
	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}

	// Create a new DownloadChan structure
	data := files.NewObject(uint32(cfg.Downloaders))

	// Start downloading files
	manager := downloader.NewDownloader(client, cfg)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.DownloadFiles(ctx, data)
	}()

	// List objects in the bucket
	if err = client.ListObjects(ctx, data, cache); err != nil {
		log.Fatal(err)
	}

	// Clear the cache
	cache.Clear()
	log.Printf("Total files in cache: %d\n", cache.totalCount)

	wg.Wait()
}
*/
