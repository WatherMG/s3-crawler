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
	"time"

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

	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	manager := downloader.NewDownloader(client, cfg)

	runtime.GOMAXPROCS(int(cfg.NumCPU))

	if err = client.CheckBucket(ctx); err != nil {
		log.Fatal(err)
	}
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	data, err := client.ListObjects(ctx, cache)
	if err != nil {
		log.Fatal(err)
	}
	// }()
	cache.Clear()
	datas3 := files.NewFileCollection(cfg.Pagination.MaxKeys)

	if err = manager.DownloadFiles(ctx, data, datas3); err != nil {
		log.Println(err)
	}

	// wg.Wait()
	// fmt.Scanln()
	memstat(datas3)
	fmt.Printf("Programm running total %s", time.Since(runTime))
	runtime.GC() // get up-to-date statistics
	pprof.WriteHeapProfile(f)
}

func memstat(data *files.FileCollection) {
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
