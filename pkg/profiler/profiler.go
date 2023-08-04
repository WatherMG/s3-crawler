package profiler

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/utils"
)

const (
	profilingDir  = "./"
	traceFileName = "trace.out"
	memFileName   = "mem.pprof"
	cpuFileName   = "cpu.pprof"
)

func SetupProfiling(isProfilingEnabled bool) (cleanupFunc func()) {
	if isProfilingEnabled {
		if err := utils.CreatePath(profilingDir); err != nil {
			log.Println(err)
		}
		traceFile, _ := os.Create(filepath.Join(profilingDir, traceFileName))
		memFile, _ := os.Create(filepath.Join(profilingDir, memFileName))
		cpuFile, _ := os.Create(filepath.Join(profilingDir, cpuFileName))

		trace.Start(traceFile)
		pprof.StartCPUProfile(cpuFile)

		cleanupFunc = func() {
			trace.Stop()
			pprof.StopCPUProfile()
			traceFile.Close()
			cpuFile.Close()
			runtime.GC() // get up-to-date statistics
			pprof.WriteHeapProfile(memFile)
			memFile.Close()
		}
	} else {
		cleanupFunc = func() {}
	}
	return
}

func WriteMemStat(data *files.FileCollection, cfg *configuration.Configuration, elapsed time.Duration) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	f, err := os.OpenFile("mem.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Fprintf(f, "----------------------------------------------------------------\n")
	fmt.Fprintf(f, "Bucket:%s with %d file(s). with cores %d, downloaders %d, withDecompress %t\n", cfg.BucketName, data.Count(), cfg.NumCPU, cfg.GetDownloaders(), cfg.IsDecompress)
	fmt.Fprintf(f, "Alloc = %s\n", utils.FormatBytes(int64(m.Alloc)))
	fmt.Fprintf(f, "TotalAlloc = %s\n", utils.FormatBytes(int64(m.TotalAlloc)))
	fmt.Fprintf(f, "NumGC = %d\n", m.NumGC)
	fmt.Fprintf(f, "Time to download = %s\n", elapsed)
	fmt.Fprintf(f, "AVG time to download 1 file = %s\n", elapsed/time.Duration(data.Count()+1))
}
