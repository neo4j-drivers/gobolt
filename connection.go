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
#cgo pkg-config: seabolt17

#include <stdlib.h>

#include "bolt/bolt.h"
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"
)

// Connection represents an active seabolt connection
type Connection interface {
	Id() string
	RemoteAddress() string
	Server() string

	Begin(bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error)
	Commit() (RequestHandle, error)
	Rollback() (RequestHandle, error)
	Run(cypher string, args map[string]interface{}, bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error)
	PullAll() (RequestHandle, error)
	DiscardAll() (RequestHandle, error)
	Reset() (RequestHandle, error)
	Flush() error
	Fetch(request RequestHandle) (FetchType, error)  // return type ?
	FetchSummary(request RequestHandle) (int, error) // return type ?

	LastBookmark() string
	Fields() ([]string, error)
	Metadata() (map[string]interface{}, error)
	Data() ([]interface{}, error)

	Close() error
}

type neo4jConnection struct {
	connector   *neo4jConnector
	cInstance   *C.struct_BoltConnection
	valueSystem *boltValueSystem
}

func (connection *neo4jConnection) Id() string {
	return C.GoString(C.BoltConnection_id(connection.cInstance))
}

func (connection *neo4jConnection) RemoteAddress() string {
	connectedAddress := C.BoltConnection_remote_endpoint(connection.cInstance)
	if connectedAddress == nil {
		return "UNKNOWN"
	}

	return fmt.Sprintf("%s:%s", C.GoString(C.BoltAddress_host(connectedAddress)), C.GoString(C.BoltAddress_port(connectedAddress)))
}

func (connection *neo4jConnection) Server() string {
	server := C.BoltConnection_server(connection.cInstance)
	if server == nil {
		return "UNKNOWN"
	}

	return C.GoString(server)
}

func (connection *neo4jConnection) Begin(bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error) {
	var res C.int

	res = C.BoltConnection_clear_begin(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to clear begin message")
	}

	if len(bookmarks) > 0 {
		bookmarksValue := connection.valueSystem.valueToConnector(bookmarks)
		res := C.BoltConnection_set_begin_bookmarks(connection.cInstance, bookmarksValue)
		C.BoltValue_destroy(bookmarksValue)
		if res != C.BOLT_SUCCESS {
			return -1, newError(connection, "unable to set bookmarks for begin message")
		}
	}

	if txTimeout > 0 {
		timeOut := C.int64_t(txTimeout / time.Millisecond)
		res := C.BoltConnection_set_begin_tx_timeout(connection.cInstance, timeOut)
		if res != C.BOLT_SUCCESS {
			return -1, newError(connection, "unable to set tx timeout for begin message")
		}
	}

	if len(txMetadata) > 0 {
		metadataValue := connection.valueSystem.valueToConnector(txMetadata)
		res := C.BoltConnection_set_begin_tx_metadata(connection.cInstance, metadataValue)
		C.BoltValue_destroy(metadataValue)
		if res != C.BOLT_SUCCESS {
			return -1, newError(connection, "unable to set tx metadata for begin message")
		}
	}

	res = C.BoltConnection_load_begin_request(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate begin message")
	}

	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) Commit() (RequestHandle, error) {
	res := C.BoltConnection_load_commit_request(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate commit message")
	}

	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) Rollback() (RequestHandle, error) {
	res := C.BoltConnection_load_rollback_request(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate rollback message")
	}

	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) Run(cypher string, params map[string]interface{}, bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error) {
	var res C.int

	res = C.BoltConnection_clear_run(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to clear run message")
	}

	cypherStr := C.CString(cypher)
	res = C.BoltConnection_set_run_cypher(connection.cInstance, cypherStr, C.size_t(len(cypher)), C.int32_t(len(params)))
	C.free(unsafe.Pointer(cypherStr))
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to set cypher statement")
	}

	var index C.int32_t
	for paramName, paramValue := range params {
		paramNameLen := C.size_t(len(paramName))
		paramNameStr := C.CString(paramName)

		boltValue := C.BoltConnection_set_run_cypher_parameter(connection.cInstance, index, paramNameStr, paramNameLen)
		C.free(unsafe.Pointer(paramNameStr))
		if boltValue == nil {
			return -1, newError(connection, "unable to retrieve reference to cypher statement parameter value")
		}

		connection.valueSystem.valueAsConnector(boltValue, paramValue)

		index++
	}

	if len(bookmarks) > 0 {
		bookmarksValue := connection.valueSystem.valueToConnector(bookmarks)
		res := C.BoltConnection_set_run_bookmarks(connection.cInstance, bookmarksValue)
		C.BoltValue_destroy(bookmarksValue)
		if res != C.BOLT_SUCCESS {
			return -1, newError(connection, "unable to set bookmarks for run message")
		}
	}

	if txTimeout > 0 {
		timeOut := C.int64_t(txTimeout / time.Millisecond)
		res := C.BoltConnection_set_run_tx_timeout(connection.cInstance, timeOut)
		if res != C.BOLT_SUCCESS {
			return -1, newError(connection, "unable to set tx timeout for run message")
		}
	}

	if len(txMetadata) > 0 {
		metadataValue := connection.valueSystem.valueToConnector(txMetadata)
		res := C.BoltConnection_set_run_tx_metadata(connection.cInstance, metadataValue)
		C.BoltValue_destroy(metadataValue)
		if res != C.BOLT_SUCCESS {
			return -1, newError(connection, "unable to set tx metadata for run message")
		}
	}

	res = C.BoltConnection_load_run_request(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate run message")
	}

	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) PullAll() (RequestHandle, error) {
	res := C.BoltConnection_load_pull_request(connection.cInstance, -1)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate pullall message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) DiscardAll() (RequestHandle, error) {
	res := C.BoltConnection_load_discard_request(connection.cInstance, -1)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate discardall message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) assertReadyState() error {
	cStatus := C.BoltConnection_status(connection.cInstance)

	if C.BoltStatus_get_state(cStatus) != C.BOLT_CONNECTION_STATE_READY {
		return newError(connection, "unexpected connection state")
	}

	return nil
}

func (connection *neo4jConnection) Flush() error {
	res := C.BoltConnection_send(connection.cInstance)
	if res < 0 {
		return newError(connection, "unable to flush")
	}

	return connection.assertReadyState()
}

func (connection *neo4jConnection) Fetch(request RequestHandle) (FetchType, error) {
	res := C.BoltConnection_fetch(connection.cInstance, C.BoltRequest(request))

	if err := connection.assertReadyState(); err != nil {
		return FetchTypeError, err
	}

	return FetchType(res), nil
}

func (connection *neo4jConnection) FetchSummary(request RequestHandle) (int, error) {
	res := C.BoltConnection_fetch_summary(connection.cInstance, C.BoltRequest(request))
	if res < 0 {
		return -1, newError(connection, "unable to fetch summary")
	}

	err := connection.assertReadyState()
	if err != nil {
		return -1, err
	}

	return int(res), nil
}

func (connection *neo4jConnection) LastBookmark() string {
	bookmark := C.BoltConnection_last_bookmark(connection.cInstance)
	if bookmark != nil {
		return C.GoString(bookmark)
	}
	return ""
}

func (connection *neo4jConnection) Fields() ([]string, error) {
	fields, err := connection.valueSystem.valueAsGo(C.BoltConnection_field_names(connection.cInstance))
	if err != nil {
		return nil, err
	}

	if fields != nil {
		fieldsAsList := fields.([]interface{})
		fieldsAsStr := make([]string, len(fieldsAsList))
		for i := range fieldsAsList {
			fieldsAsStr[i] = fieldsAsList[i].(string)
		}
		return fieldsAsStr, nil
	}

	return nil, newGenericError("field names not available")
}

func (connection *neo4jConnection) Metadata() (map[string]interface{}, error) {
	metadata, err := connection.valueSystem.valueAsGo(C.BoltConnection_metadata(connection.cInstance))
	if err != nil {
		return nil, err
	}

	if metadataAsGenericMap, ok := metadata.(map[string]interface{}); ok {
		return metadataAsGenericMap, nil
	}

	return nil, newGenericError("metadata is not of expected type")
}

func (connection *neo4jConnection) Data() ([]interface{}, error) {
	fields, err := connection.valueSystem.valueAsGo(C.BoltConnection_field_values(connection.cInstance))
	if err != nil {
		return nil, err
	}

	return fields.([]interface{}), nil
}

func (connection *neo4jConnection) Reset() (RequestHandle, error) {
	res := C.BoltConnection_load_reset_request(connection.cInstance)
	if res != C.BOLT_SUCCESS {
		return -1, newError(connection, "unable to generate reset message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) Close() error {
	err := connection.connector.release(connection)
	if err != nil {
		return err
	}
	return nil
}
