package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"

	"s3-crawler/pkg/archives"
	"s3-crawler/pkg/cacher"
	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/downloader"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/s3client"
	"s3-crawler/pkg/utils"
)

var confPath = flag.String("config", "../config1.json", "path to the configuration file")

func main() {
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
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

	cpuf, err := os.Create("cpu.prof")
	if err != nil {
		log.Println(err)
	}
	pprof.StartCPUProfile(cpuf)
	defer pprof.StopCPUProfile()

	// var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute) // TODO: add timeout to config
	defer cancel()

	cfg, err := configuration.LoadConfig(*confPath)
	if err != nil {
		log.Fatal(err)
	}

	if err = utils.CreatePath(cfg.LocalPath); err != nil {
		log.Fatal(err)
	}

	cache := cacher.NewCache()
	if err = cache.LoadFromDir(cfg); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(cfg.LocalPath)

	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	manager := downloader.NewDownloader(client, cfg)

	runtime.GOMAXPROCS(int(cfg.NumCPU))

	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}

	data := files.NewFileCollection()
	st := time.Now()
	if err = client.ListObjects(ctx, data, cache); err != nil {
		log.Fatal(err)
	}
	el := time.Since(st)

	s := time.Now()
	var e time.Duration
	go func() {
		var wg sync.WaitGroup
		for filePath := range data.ArchivesChan {
			wg.Add(1)
			go func(path string) {
				defer wg.Done()
				if err := archives.DecompressFile(path, cfg); err != nil {
					log.Println(err)
					return
				}
			}(filePath)
		}
		wg.Wait()
		e = time.Since(s)
	}()

	start := time.Now()
	if err = manager.DownloadFiles(ctx, data); err != nil {
		log.Println(err)
	}
	downloadTime := time.Since(start)

	memstat(data, cfg, manager, downloadTime, e, el)
	fmt.Printf("Programm running total %s\n", time.Since(runTime))
	runtime.GC() // get up-to-date statistics
	pprof.WriteHeapProfile(f)
}

func memstat(data *files.FileCollection, cfg *configuration.Configuration, manager *downloader.Downloader, elapsed time.Duration, e time.Duration, el time.Duration) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	f, err := os.OpenFile("mem.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Fprintf(f, "----------------------------------------------------------------\n")
	fmt.Fprintf(f, "Bucket:%s with %d file(s). with cores %d, downloaders %d, withDecompress %t\n", cfg.BucketName, data.Count(), cfg.NumCPU, cfg.GetDownloaders(), cfg.IsDecompress)

	fmt.Fprintf(f, "Alloc = %v MiB\n", m.Alloc/files.MiB)
	fmt.Fprintf(f, "TotalAlloc = %v MiB\n", m.TotalAlloc/files.MiB)
	fmt.Fprintf(f, "Sys = %v MiB\n", m.Sys/files.MiB)
	fmt.Fprintf(f, "NumGC = %v\n", m.NumGC)
	fmt.Fprintf(f, "Frees = %v\n", utils.FormatBytes(int64(m.Frees)))
	fmt.Fprintf(f, "HeapAlloc = %v\n", utils.FormatBytes(int64(m.HeapAlloc)))
	fmt.Fprintf(f, "HeapIdle = %v\n", utils.FormatBytes(int64(m.HeapIdle)))
	fmt.Fprintf(f, "HeapInuse = %v\n", utils.FormatBytes(int64(m.HeapInuse)))
	fmt.Fprintf(f, "HeapObjects = %v\n", utils.FormatBytes(int64(m.HeapObjects)))
	fmt.Fprintf(f, "HeapReleased = %v\n", utils.FormatBytes(int64(m.HeapReleased)))
	fmt.Fprintf(f, "HeapSys = %v\n", utils.FormatBytes(int64(m.HeapSys)))
	fmt.Fprintf(f, "Mallocs = %v\n", utils.FormatBytes(int64(m.Mallocs)))
	fmt.Fprintf(f, "StackSys = %v\n", utils.FormatBytes(int64(m.StackSys)))
	fmt.Fprintf(f, "Time to get objects = %s\n", el)
	fmt.Fprintf(f, "Time to download = %s\n", elapsed)
	fmt.Fprintf(f, "AVG time to download 1 file = %s\n", elapsed/time.Duration(data.Count()))
	fmt.Fprintf(f, "Time to decompress = %s\n", e)
}
