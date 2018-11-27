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

#include "bolt/bolt.h"
*/
import "C"
import (
	"errors"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// AccessMode is used by the routing driver to decide if a transaction should be routed to a write server
// or a read server in a cluster. When running a transaction, a write transaction requires a server that
// supports writes. A read transaction, on the other hand, requires a server that supports read operations.
// This classification is key for routing driver to route transactions to a cluster correctly.
type AccessMode int

const (
	// AccessModeWrite makes the driver return a session towards a write server
	AccessModeWrite AccessMode = 0
	// AccessModeRead makes the driver return a session towards a follower or a read-replica
	AccessModeRead AccessMode = 1
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

	cAddress  *C.BoltAddress
	cInstance *C.BoltConnector
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

	cStatus := C.BoltStatus_create()
	defer C.BoltStatus_destroy(cStatus)
	cConnection := C.BoltConnector_acquire(conn.cInstance, C.BoltAccessMode(cMode), cStatus)
	if cConnection == nil {
		state := C.BoltStatus_get_state(cStatus)
		code := C.BoltStatus_get_error(cStatus)
		codeText := C.GoString(C.BoltError_get_string(code))
		context := C.GoString(C.BoltStatus_get_error_context(cStatus))

		return nil, newConnectorError(int(state), int(code), codeText, context, "unable to acquire connection from connector")
	}

	return &neo4jConnection{connector: conn, cInstance: cConnection, valueSystem: conn.valueSystem}, nil
}

func (conn *neo4jConnector) release(connection *neo4jConnection) error {
	C.BoltConnector_release(conn.cInstance, connection.cInstance)
	return nil
}

// GetAllocationStats returns statistics about seabolt (C) allocations
func GetAllocationStats() (int64, int64, int64) {
	current := C.BoltStat_memory_allocation_current()
	peak := C.BoltStat_memory_allocation_peak()
	events := C.BoltStat_memory_allocation_events()

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

	cTrust := C.BoltTrust_create()
	C.BoltTrust_set_certs(cTrust, nil, 0)
	C.BoltTrust_set_skip_verify(cTrust, 0)
	C.BoltTrust_set_skip_verify_hostname(cTrust, 0)

	certsBuf, err := pemEncodeCerts(config.TLSCertificates)
	if err != nil {
		return nil, err
	}

	if certsBuf != nil {
		certsBytes := certsBuf.String()
		C.BoltTrust_set_certs(cTrust, C.CString(certsBytes), C.uint64_t(certsBuf.Len()))
	}

	if config.TLSSkipVerify {
		C.BoltTrust_set_skip_verify(cTrust, 1)
	}

	if config.TLSSkipVerifyHostname {
		C.BoltTrust_set_skip_verify_hostname(cTrust, 1)
	}

	cSocketOpts := C.BoltSocketOptions_create()
	C.BoltSocketOptions_set_connect_timeout(cSocketOpts, C.int(config.SockConnectTimeout/time.Millisecond))
	C.BoltSocketOptions_set_keep_alive(cSocketOpts, 1)
	if !config.SockKeepalive {
		C.BoltSocketOptions_set_keep_alive(cSocketOpts, 0)
	}

	valueSystem := createValueSystem(config)

	var mode uint32 = C.BOLT_MODE_DIRECT
	if uri.Scheme == "bolt+routing" {
		mode = C.BOLT_MODE_ROUTING
	}

	var transport uint32 = C.BOLT_TRANSPORT_PLAINTEXT
	if config.Encryption {
		transport = C.BOLT_TRANSPORT_ENCRYPTED
	}

	userAgentStr := C.CString("Go Driver/1.7")
	routingContextValue, err := extractRoutingContext(uri, valueSystem)
	if err != nil {
		return nil, valueSystem.genericErrorFactory("unable to extract routing context: %v", err)
	}
	hostnameStr, portStr := C.CString(uri.Hostname()), C.CString(uri.Port())
	address := C.BoltAddress_create(hostnameStr, portStr)
	authTokenValue, err := valueSystem.valueToConnector(authToken)
	if err != nil {
		return nil, valueSystem.genericErrorFactory("unable to convert authentication token to connector value: %v", err)
	}

	key := startupLibrary()

	cLogger := registerLogging(key, config.Log)
	cResolver := registerResolver(key, config.AddressResolver)

	cConfig := C.BoltConfig_create()
	C.BoltConfig_set_mode(cConfig, C.BoltMode(mode))
	C.BoltConfig_set_transport(cConfig, C.BoltTransport(transport))
	C.BoltConfig_set_trust(cConfig, cTrust)
	C.BoltConfig_set_user_agent(cConfig, userAgentStr)
	C.BoltConfig_set_routing_context(cConfig, routingContextValue)
	C.BoltConfig_set_address_resolver(cConfig, cResolver)
	C.BoltConfig_set_log(cConfig, cLogger)
	C.BoltConfig_set_max_pool_size(cConfig, C.int(config.MaxPoolSize))
	C.BoltConfig_set_max_connection_life_time(cConfig, C.int(config.MaxConnLifetime/time.Millisecond))
	C.BoltConfig_set_max_connection_acquisition_time(cConfig, C.int(config.ConnAcquisitionTimeout/time.Millisecond))
	C.BoltConfig_set_socket_options(cConfig, cSocketOpts)

	cInstance := C.BoltConnector_create(address, authTokenValue, cConfig)
	conn := &neo4jConnector{
		key:         key,
		uri:         uri,
		authToken:   authToken,
		config:      *config,
		cAddress:    address,
		valueSystem: valueSystem,
		cInstance:   cInstance,
		cLogger:     cLogger,
	}

	// do cleanup
	C.free(unsafe.Pointer(userAgentStr))
	C.free(unsafe.Pointer(hostnameStr))
	C.free(unsafe.Pointer(portStr))
	C.BoltValue_destroy(routingContextValue)
	C.BoltValue_destroy(authTokenValue)
	C.BoltTrust_destroy(cTrust)
	C.BoltSocketOptions_destroy(cSocketOpts)
	C.BoltConfig_destroy(cConfig)

	return conn, nil
}

func createValueSystem(config *Config) *boltValueSystem {
	valueHandlersBySignature := make(map[int16]ValueHandler, len(config.ValueHandlers))
	valueHandlersByType := make(map[reflect.Type]ValueHandler, len(config.ValueHandlers))
	for _, handler := range config.ValueHandlers {
		for _, readSignature := range handler.ReadableStructs() {
			valueHandlersBySignature[readSignature] = handler
		}

		for _, writeType := range handler.WritableTypes() {
			valueHandlersByType[writeType] = handler
		}
	}

	databaseErrorFactory := newDatabaseError
	connectorErrorFactory := newConnectorError
	genericErrorFactory := newGenericError
	if config.DatabaseErrorFactory != nil {
		databaseErrorFactory = config.DatabaseErrorFactory
	}
	if config.ConnectorErrorFactory != nil {
		connectorErrorFactory = config.ConnectorErrorFactory
	}
	if config.GenericErrorFactory != nil {
		genericErrorFactory = config.GenericErrorFactory
	}

	return &boltValueSystem{
		valueHandlers:            config.ValueHandlers,
		valueHandlersBySignature: valueHandlersBySignature,
		valueHandlersByType:      valueHandlersByType,
		connectorErrorFactory:    connectorErrorFactory,
		databaseErrorFactory:     databaseErrorFactory,
		genericErrorFactory:      genericErrorFactory,
	}
}

func extractRoutingContext(source *url.URL, valueSystem *boltValueSystem) (*C.struct_BoltValue, error) {
	var err error
	var values url.Values
	var result map[string]string

	if values, err = url.ParseQuery(source.RawQuery); err != nil {
		return nil, valueSystem.genericErrorFactory("unable to parse routing context '%s'", source.RawQuery)
	}

	if len(values) == 0 {
		return nil, nil
	}

	result = make(map[string]string, len(values))
	for key, value := range values {
		if len(value) > 1 {
			return nil, valueSystem.genericErrorFactory("duplicate value specified for '%s' as routing context", key)
		}

		result[key] = value[0]
	}

	return valueSystem.valueToConnector(result)
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
