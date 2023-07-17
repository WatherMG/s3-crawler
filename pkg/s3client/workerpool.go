package s3client

import (
	"sync"

	"s3-crawler/pkg/files"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type WorkerPool struct {
	objects chan types.Object
	files   chan *files.File
	wg      sync.WaitGroup
	mu      sync.Mutex
}

type ObjectProcessor func(object types.Object)
type FileProcessor func(file *files.File)

func NewWorkerPool(maxWorkers int, processor ObjectProcessor) *WorkerPool {
	pool := &WorkerPool{
		objects: make(chan types.Object, maxWorkers),
	}
	pool.wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			defer pool.wg.Done()
			for object := range pool.objects {
				pool.mu.Lock()
				processor(object)
				pool.mu.Unlock()
			}
		}()
	}
	return pool
}

func NewWorkerPoolFile(maxWorkers int, processor FileProcessor) *WorkerPool {
	pool := &WorkerPool{
		files: make(chan *files.File, maxWorkers),
	}
	pool.wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			defer pool.wg.Done()
			for file := range pool.files {
				processor(file)
			}
		}()
	}
	return pool
}

func (pool *WorkerPool) Add(file *files.File) {
	pool.files <- file
}

func (pool *WorkerPool) AddFileFromObject(object types.Object) {
	pool.objects <- object
}

func (pool *WorkerPool) WaitObjects() {
	close(pool.objects)
	pool.wg.Wait()
}
func (pool *WorkerPool) WaitFiles() {
	close(pool.files)
	pool.wg.Wait()
}
