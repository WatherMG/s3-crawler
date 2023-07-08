package s3client

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type WorkerPool struct {
	work chan types.Object
	wg   sync.WaitGroup
}

type ObjectProcessor func(object types.Object)

func NewWorkerPool(maxWorkers int, processor ObjectProcessor) *WorkerPool {
	pool := &WorkerPool{
		work: make(chan types.Object),
	}
	pool.wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			defer pool.wg.Done()
			for file := range pool.work {
				processor(file)
			}
		}()
	}
	return pool
}

func (pool *WorkerPool) AddFileFromObject(file types.Object) {
	pool.work <- file
}

func (pool *WorkerPool) Wait() {
	close(pool.work)
	pool.wg.Wait()
}
