package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"time"

	"s3-crawler/pkg/archives"
	"s3-crawler/pkg/cacher"
	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/downloader"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/profiler"
	"s3-crawler/pkg/s3client"
	"s3-crawler/pkg/utils"
)

var confPath = flag.String("config", "config1.json", "Path to the configuration file")
var isProfilingEnabled = flag.Bool("profiling", false, "Enable profiling")

func main() {
	flag.Parse()
	profilerCleanUpFunc := profiler.SetupProfiling(*isProfilingEnabled)
	defer profilerCleanUpFunc()

	runTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute) // TODO: add timeout to config
	defer cancel()

	cfg, err := configuration.LoadConfig(*confPath)
	if err != nil {
		log.Fatal(err)
	}
	runtime.GOMAXPROCS(int(cfg.NumCPU))
	workers := cfg.GetDownloaders()

	client, err := s3client.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	cache := cacher.NewCache(ctx, cfg)
	if err = cache.LoadFromDir(cfg); err != nil {
		log.Fatal(err)
	}

	data := files.NewFileCollection(workers)
	if err = client.ListObjects(ctx, data, cache); err != nil {
		log.Fatal(err)
	}
	data.CreateChannels()

	var wg sync.WaitGroup
	var wgWrite sync.WaitGroup
	maxWriters := int(cfg.NumCPU)
	availableWriters := make(chan struct{}, maxWriters)
	for i := 0; i < cap(availableWriters); i++ {
		availableWriters <- struct{}{}
	}

	wgWrite.Add(maxWriters)
	startWrite := time.Now()
	for i := 0; i < maxWriters; i++ {
		go func(data *files.FileCollection) {
			defer wgWrite.Done()
			for file := range data.DataChan {
				<-availableWriters
				if err := utils.SaveDataToFile(file); err != nil {
					log.Printf("Save file error: %v\n", err)
				}
				availableWriters <- struct{}{}
			}
		}(data)
	}

	wg.Add(workers)
	startDecompress := time.Now()
	for i := 0; i < workers; i++ {
		go func(data *files.FileCollection) {
			defer wg.Done()
			for file := range data.ArchivesChan {
				if err := archives.ProcessFile(file, data); err != nil {
					log.Printf("Decompress error: %v\n", err)
				}
			}
		}(data)
	}

	manager := downloader.NewDownloader(client, cfg)
	downloadTime, err := manager.DownloadFiles(ctx, data)
	if err != nil {
		log.Println(err)
	}

	wg.Wait()
	close(data.DataChan)
	wgWrite.Wait()
	close(availableWriters)

	if *isProfilingEnabled {
		profiler.WriteMemStat(data, cfg, downloadTime)
	}
	fmt.Print("\u001B[2K\r")
	if data.ArchivesCount() > 0 {
		utils.TimeTrack(startDecompress, "Decompressing files")
	}
	if data.Count() > 0 {
		utils.TimeTrack(startWrite, "Write files to disk")
	}
	fmt.Printf("Programm running total %s\n", time.Since(runTime).Truncate(time.Millisecond))
	fmt.Scanln("Press ENTER to exit...")
}
