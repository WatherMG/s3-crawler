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
		work: make(chan types.Object, maxWorkers),
	}
	pool.wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			defer pool.wg.Done()
			for object := range pool.work {
				processor(object)
			}
		}()
	}
	return pool
}

func (pool *WorkerPool) AddFileFromObject(object types.Object) {
	pool.work <- object
}

func (pool *WorkerPool) Wait() {
	close(pool.work)
	pool.wg.Wait()
}
