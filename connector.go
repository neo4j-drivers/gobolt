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

/*
#include <stdlib.h>

#include "bolt/lifecycle.h"
#include "bolt/connector.h"
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

type AccessMode int

const (
	AccessModeWrite AccessMode = 0
	AccessModeRead  AccessMode = 1
)

// Connector represents an initialised seabolt connector
type Connector interface {
	Acquire(mode AccessMode) (Connection, error)
	Close() error
}

// RequestHandle identifies an individual request sent to server
type RequestHandle int64

// FetchType identifies the type of the result fetched via Fetch() call
type FetchType int

const (
	// FetchTypeRecord tells that fetched data is record
	FetchTypeRecord FetchType = 1
	// FetchTypeMetadata tells that fetched data is metadata
	FetchTypeMetadata = 0
	// FetchTypeError tells that fetch was not successful
	FetchTypeError = -1
)

var initCounter int32

type neo4jConnector struct {
	sync.Mutex

	key int

	uri       *url.URL
	authToken map[string]interface{}
	config    Config

	cAddress  *C.struct_BoltAddress
	cInstance *C.struct_BoltConnector
	cLogger   *C.struct_BoltLog
	cResolver *C.struct_BoltAddressResolver

	valueSystem *boltValueSystem
}

func (conn *neo4jConnector) Close() error {
	if conn.cInstance != nil {
		C.BoltConnector_destroy(conn.cInstance)
		conn.cInstance = nil
	}

	if conn.cLogger != nil {
		unregisterLogging(conn.key)
		C.BoltLog_destroy(conn.cLogger)
		conn.cLogger = nil
	}

	if conn.cResolver != nil {
		unregisterResolver(conn.key)
		C.BoltAddressResolver_destroy(conn.cResolver)
		conn.cResolver = nil
	}

	if conn.cAddress != nil {
		C.BoltAddress_destroy(conn.cAddress)
		conn.cAddress = nil
	}

	shutdownLibrary()

	return nil
}

func (conn *neo4jConnector) Acquire(mode AccessMode) (Connection, error) {
	var cMode uint32 = C.BOLT_ACCESS_MODE_WRITE
	if mode == AccessModeRead {
		cMode = C.BOLT_ACCESS_MODE_READ
	}

	cResult := C.BoltConnector_acquire(conn.cInstance, cMode)
	if cResult.connection == nil {
		return nil, newConnectionErrorWithCode(cResult.connection_status, cResult.connection_error, "unable to acquire connection from connector")
	}

	return &neo4jConnection{connector: conn, cInstance: cResult.connection, valueSystem: conn.valueSystem}, nil
}

func (conn *neo4jConnector) release(connection *neo4jConnection) error {
	C.BoltConnector_release(conn.cInstance, connection.cInstance)
	return nil
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

	if config == nil {
		config = &Config{
			Encryption:  true,
			MaxPoolSize: 100,
		}
	}

	valueSystem := createValueSystem(config.ValueHandlers)

	var mode uint32 = C.BOLT_DIRECT
	if uri.Scheme == "bolt+routing" {
		mode = C.BOLT_ROUTING
	}

	var transport uint32 = C.BOLT_SOCKET
	if config.Encryption {
		transport = C.BOLT_SECURE_SOCKET
	}

	userAgentStr := C.CString("Go Driver/1.7")
	routingContextValue := valueSystem.valueToConnector(uri.Query())
	hostnameStr, portStr := C.CString(uri.Hostname()), C.CString(uri.Port())
	address := C.BoltAddress_create(hostnameStr, portStr)
	authTokenValue := valueSystem.valueToConnector(authToken)

	key := startupLibrary()

	cLogger := registerLogging(key, config.Log)
	cResolver := registerResolver(key, config.AddressResolver)
	cConfig := C.struct_BoltConfig{
		mode:             mode,
		transport:        transport,
		user_agent:       userAgentStr,
		routing_context:  routingContextValue,
		address_resolver: cResolver,
		log:              cLogger,
		max_pool_size:    C.uint(config.MaxPoolSize),
	}

	cInstance := C.BoltConnector_create(address, authTokenValue, &cConfig)
	conn := &neo4jConnector{
		key:         key,
		uri:         uri,
		authToken:   authToken,
		config:      *config,
		cAddress:    address,
		valueSystem: createValueSystem(config.ValueHandlers),
		cInstance:   cInstance,
		cLogger:     cLogger,
	}

	// do cleanup
	C.free(unsafe.Pointer(userAgentStr))
	C.free(unsafe.Pointer(hostnameStr))
	C.free(unsafe.Pointer(portStr))
	C.BoltValue_destroy(routingContextValue)
	C.BoltValue_destroy(authTokenValue)

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

func startupLibrary() int {
	counter := atomic.AddInt32(&initCounter, 1)
	if counter == 1 {
		C.Bolt_startup()
	}
	return int(counter)
}

func shutdownLibrary() {
	if atomic.AddInt32(&initCounter, -1) == 0 {
		C.Bolt_shutdown()
	}
}
