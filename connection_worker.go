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
	"sync/atomic"
	"time"
)

type job func()

type workerConnection struct {
	pool     *workerPool
	delegate Connection
	sending  int32
	recving  int32
}

func (w *workerConnection) queueSendJob(item job) error {
	if atomic.CompareAndSwapInt32(&w.sending, 0, 1) {
		defer atomic.StoreInt32(&w.sending, 0)

		var done = make(chan bool, 1)
		defer close(done)

		if err := w.pool.submit(func(stopper <-chan signal) {
			item()
			done <- true
		}); err != nil {
			return err
		}

		<-done

		return nil
	}

	return newGenericError("a connection is not thread-safe and thus should not be used concurrently")
}

func (w *workerConnection) queueRecvJob(item job) error {
	if atomic.CompareAndSwapInt32(&w.recving, 0, 1) {
		defer atomic.StoreInt32(&w.recving, 0)

		var done = make(chan bool, 1)
		defer close(done)

		if err := w.pool.submit(func(stopper <-chan signal) {
			item()
			done <- true
		}); err != nil {
			return err
		}

		<-done

		return nil
	}

	return newGenericError("a connection is not thread-safe and thus should not be used concurrently")
}

func (w *workerConnection) Id() (string, error) {
	var id string
	var err error

	if otherErr := w.queueRecvJob(func() {
		id, err = w.delegate.Id()
	}); otherErr != nil {
		err = otherErr
	}

	return id, err
}

func (w *workerConnection) RemoteAddress() (string, error) {
	var remoteAddress string
	var err error

	if otherErr := w.queueRecvJob(func() {
		remoteAddress, err = w.delegate.RemoteAddress()
	}); otherErr != nil {
		err = otherErr
	}

	return remoteAddress, err
}

func (w *workerConnection) Server() (string, error) {
	var server string
	var err error

	if otherErr := w.queueRecvJob(func() {
		server, err = w.delegate.Server()
	}); otherErr != nil {
		err = otherErr
	}

	return server, err
}

func (w *workerConnection) Begin(bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.Begin(bookmarks, txTimeout, txMetadata)
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) Commit() (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.Commit()
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) Rollback() (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.Rollback()
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) Run(cypher string, args map[string]interface{}, bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.Run(cypher, args, bookmarks, txTimeout, txMetadata)
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) PullAll() (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.PullAll()
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) DiscardAll() (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.DiscardAll()
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) Reset() (RequestHandle, error) {
	var handle RequestHandle
	var err error

	if otherErr := w.queueSendJob(func() {
		handle, err = w.delegate.Reset()
	}); otherErr != nil {
		err = otherErr
	}

	return handle, err
}

func (w *workerConnection) Flush() error {
	var err error

	if otherErr := w.queueSendJob(func() {
		err = w.delegate.Flush()
	}); otherErr != nil {
		err = otherErr
	}

	return err
}

func (w *workerConnection) Fetch(request RequestHandle) (FetchType, error) {
	var fetched FetchType
	var err error

	if otherErr := w.queueRecvJob(func() {
		fetched, err = w.delegate.Fetch(request)
	}); otherErr != nil {
		err = otherErr
	}

	return fetched, err
}

func (w *workerConnection) FetchSummary(request RequestHandle) (int, error) {
	var fetched int
	var err error

	if otherErr := w.queueRecvJob(func() {
		fetched, err = w.delegate.FetchSummary(request)
	}); otherErr != nil {
		err = otherErr
	}

	return fetched, err
}

func (w *workerConnection) LastBookmark() (string, error) {
	var bookmark string
	var err error

	if otherErr := w.queueRecvJob(func() {
		bookmark, err = w.delegate.LastBookmark()
	}); otherErr != nil {
		err = otherErr
	}

	return bookmark, err
}

func (w *workerConnection) Fields() ([]string, error) {
	var fields []string
	var err error

	if otherErr := w.queueRecvJob(func() {
		fields, err = w.delegate.Fields()
	}); otherErr != nil {
		err = otherErr
	}

	return fields, err
}

func (w *workerConnection) Metadata() (map[string]interface{}, error) {
	var metadata map[string]interface{}
	var err error

	if otherErr := w.queueRecvJob(func() {
		metadata, err = w.delegate.Metadata()
	}); otherErr != nil {
		err = otherErr
	}

	return metadata, err
}

func (w *workerConnection) Data() ([]interface{}, error) {
	var data []interface{}
	var err error

	if otherErr := w.queueRecvJob(func() {
		data, err = w.delegate.Data()
	}); otherErr != nil {
		err = otherErr
	}

	return data, err
}

func (w *workerConnection) Close() error {
	var err error

	if otherErr := w.queueSendJob(func() {
		err = w.delegate.Close()
	}); otherErr != nil {
		err = otherErr
	}

	return err
}
