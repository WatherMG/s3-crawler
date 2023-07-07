package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

var size = 1024 * 1024 * 1
var maxSizeTemp = int(size)
var bufTemp = &sync.Pool{New: func() interface{} { return make([]byte, maxSizeTemp) }}

func BenchmarkGenerateFiles(b *testing.B) {
	orig := runtime.GOMAXPROCS(0)
	runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(orig)
	// Пробуем разные значения горутин на одно ядро процессора
	goroutinesPerCore := []int{1, 2, 4, 8, 16, 24, 32, 64, 128, 256, 512, 1024}
	for _, g := range goroutinesPerCore {
		b.Run(fmt.Sprintf("GoroutinesPerCore_%d", g), func(b *testing.B) {
			// Устанавливаем GOMAXPROCS и размер workerPool
			b.ReportAllocs()
			workerPool = make(chan struct{}, runtime.NumCPU()*g)

			buf := bufTemp.Get().([]byte)
			defer bufTemp.Put(buf)

			// Запускаем бенчмарк
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				fileType := []string{"html", "json", "txt"}[rand.Intn(3)]
				path := "directory"
				os.MkdirAll(path, 0755)
				content := generateContent(fileType, maxSizeTemp, buf)
				createFile(fmt.Sprintf("%s/%s.%s", path, "fileName"+strconv.Itoa(n), fileType), content)
			}
		})
	}
}

func remove() {
	os.RemoveAll("directory")
}

func BenchmarkCreateFile(b *testing.B) {
	b.ReportAllocs()
	content := []byte("example content")
	for i := 0; i < b.N; i++ {
		createFile("test.txt", content)
	}
}
