/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobolt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type signal struct{}

type workItem func(stopper <-chan signal)

type worker func(pool *workerPool)

type workerPool struct {
	minWorkers   int32
	maxWorkers   int32
	keepAlive    time.Duration
	workers      int32
	workQueue    chan workItem
	stopper      chan signal
	stoppedEvent sync.WaitGroup
}

func newWorkerPool(minWorkers, maxWorkers int32, keepAlive time.Duration) *workerPool {
	if minWorkers < 0 {
		panic(fmt.Sprintf("%v is an invalid value for minWorkers", minWorkers))
	}

	if maxWorkers == 0 {
		panic(fmt.Sprintf("%v is an invalid value for maxWorkers", maxWorkers))
	}

	if maxWorkers < minWorkers {
		panic(fmt.Sprintf("maxWorkers[%v] is expected to be larger than or equal to minWorkers[%v]", maxWorkers, minWorkers))
	}

	poolInstance := &workerPool{
		minWorkers:   minWorkers,
		maxWorkers:   maxWorkers,
		keepAlive:    keepAlive,
		workers:      0,
		workQueue:    make(chan workItem),
		stopper:      make(chan signal),
		stoppedEvent: sync.WaitGroup{},
	}

	for i := int32(0); i < minWorkers; i++ {
		poolInstance.launchWorker()
	}

	return poolInstance
}

func (pool *workerPool) launchWorker() {
	pool.stoppedEvent.Add(1)

	var started = make(chan int, 1)
	go func(pool *workerPool) {
		defer atomic.AddInt32(&pool.workers, -1)
		defer pool.stoppedEvent.Done()

		atomic.AddInt32(&pool.workers, 1)
		started <- 1

		workerEntryPoint(pool)
	}(pool)

	<-started
}

func workerEntryPoint(pool *workerPool) {
	for {
		select {
		case work := <-pool.workQueue:
			work(pool.stopper)
		case <-pool.stopper:
			return
		case <-time.After(pool.keepAlive):
			return
		}
	}
}

func (pool *workerPool) submit(work workItem) error {
	for {
		select {
		case pool.workQueue <- work:
			return nil
		case <-pool.stopper:
			return newGenericError("unable to submit job to a closed worker pool")
		default:
			if atomic.LoadInt32(&pool.workers) < pool.maxWorkers {
				pool.launchWorker()
			} else {
				return newGenericError("worker pool reached its maximum capacity")
			}
		}
	}
}

func (pool *workerPool) isClosed() bool {
	select {
	case _, ok := <-pool.stopper:
		return !ok
	default:
		return false
	}
}

func (pool *workerPool) close() {
	close(pool.stopper)
	pool.stoppedEvent.Wait()
	close(pool.workQueue)
}
