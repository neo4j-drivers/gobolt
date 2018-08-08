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

package seabolt

/*
#include <stdlib.h>

#include "bolt/lifecycle.h"
#include "bolt/pooling.h"
#include "bolt/mem.h"
*/
import "C"
import (
	"net/url"
	"sync/atomic"
	"unsafe"
	"sync"
	"reflect"
	"errors"
)

// Connector represents an initialised seabolt connector
type Connector interface {
	GetPool() (Pool, error)
	Close() error
}

// RequestHandle identifies an individual request sent to server
type RequestHandle int64

// FetchType identifies the type of the result fetched via Fetch() call
type FetchType int

const (
	// RECORD tells that fetched data is record
	RECORD FetchType = 1
	// METADATA tells that fetched data is metadata
	METADATA = 0
	// ERROR tells that fetch was not successful
	ERROR = -1
)

var initCounter int32

// Config holds the available configurations options applicable to the connector
type Config struct {
	Encryption    bool
	Debug         bool
	MaxPoolSize   int
	ValueHandlers []ValueHandler
}

type neo4jConnector struct {
	sync.Mutex

	uri       *url.URL
	authToken map[string]interface{}
	config    Config

	address *C.struct_BoltAddress
	pool    *neo4jPool

	valueSystem *boltValueSystem
}

func (conn *neo4jConnector) Close() error {
	if conn.pool != nil {
		conn.pool.Close()
		conn.pool = nil
	}

	C.BoltAddress_destroy(conn.address)
	shutdownLibrary()
	return nil
}

func (conn *neo4jConnector) GetPool() (Pool, error) {
	if conn.pool == nil {
		conn.Lock()
		defer conn.Unlock()

		if conn.pool == nil {
			userAgent := C.CString("Go Driver/1.0")
			defer C.free(unsafe.Pointer(userAgent))

			authTokenBoltValue := conn.valueSystem.valueToConnector(conn.authToken)
			defer C.BoltValue_destroy(authTokenBoltValue)

			socketType := C.BOLT_SOCKET
			if conn.config.Encryption {
				socketType = C.BOLT_SECURE_SOCKET
			}

			cInstance := C.BoltConnectionPool_create(uint32(socketType), conn.address, userAgent, authTokenBoltValue, C.uint32_t(conn.config.MaxPoolSize))
			conn.pool = &neo4jPool{connector: conn, cInstance: cInstance, cAgent: C.CString("Go Connector")}
		}
	}

	return conn.pool, nil
}

// GetAllocationStats returns statistics about seabolt (C) allocations
func GetAllocationStats() (int64, int64, int64) {
	current := C.BoltMem_current_allocation()
	peak := C.BoltMem_peak_allocation()
	events := C.BoltMem_allocation_events()

	return int64(current), int64(peak), int64(events)
}

// NewConnector returns a new connector instance with given parameters
func NewConnector(uri *url.URL, authToken map[string]interface{}, config *Config) (connector Connector, err error) {
	if uri == nil {
		return nil, errors.New("provided uri should not be nil")
	}

	hostname, port := C.CString(uri.Hostname()), C.CString(uri.Port())
	defer C.free(unsafe.Pointer(hostname))
	defer C.free(unsafe.Pointer(port))
	address := C.BoltAddress_create(hostname, port)

	if config == nil {
		config = &Config{
			Debug:       true,
			Encryption:  true,
			MaxPoolSize: 100,
		}
	}

	startupLibrary(config.Debug)
	conn := &neo4jConnector{
		uri:         uri,
		authToken:   authToken,
		config:      *config,
		address:     address,
		valueSystem: createValueSystem(config.ValueHandlers)}
	return conn, nil
}

func createValueSystem(valueHandlers []ValueHandler) *boltValueSystem {
	valueHandlersBySignature := make(map[int8]ValueHandler, len(valueHandlers))
	valueHandlersByType := make(map[reflect.Type]ValueHandler, len(valueHandlers))
	for _, handler := range valueHandlers {
		for _, readSignature := range handler.ReadableStructs() {
			valueHandlersBySignature[readSignature] = handler
		}

		for _, writeType := range handler.WritableTypes() {
			valueHandlersByType[writeType] = handler
		}
	}

	return &boltValueSystem{valueHandlers: valueHandlers, valueHandlersBySignature: valueHandlersBySignature, valueHandlersByType: valueHandlersByType}
}

func startupLibrary(debug bool) {
	if atomic.AddInt32(&initCounter, 1) == 1 {
		logTarget := C.stdout
		if !debug {
			logTarget = nil
		}

		C.Bolt_startup(logTarget)
	}
}

func shutdownLibrary() {
	if atomic.AddInt32(&initCounter, -1) == 0 {
		C.Bolt_shutdown()
	}
}
