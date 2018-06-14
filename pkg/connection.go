package neo4j

/*
#cgo CFLAGS: -ID:/Projects.GitHub/seabolt/seabolt/include -I/home/ali/Projects/neo4j/seabolt/seabolt/include
#cgo LDFLAGS: -LD:/Projects.GitHub/seabolt/build/lib -L/home/ali/Projects/neo4j/seabolt/build/lib -lseabolt

#include <stdlib.h>

#include "bolt/connections.h"
*/
import "C"
import (
	"errors"
	"unsafe"
)

type Connection interface {
	Run(cypher string, args *map[string]interface{}) (RequestHandle, error)
	PullAll() (RequestHandle, error)
	DiscardAll() (RequestHandle, error)
	Flush() error
	Fetch(request RequestHandle) (FetchType, error)  // return type ?
	FetchSummary(request RequestHandle) (int, error) // return type ?
	Metadata() (map[string]interface{}, error)
	Data() ([]interface{}, error)

	Reset() error
	Close() error
}

type neo4jConnection struct {
	pool      *neo4jPool
	cInstance *C.struct_BoltConnection
}

func (connection *neo4jConnection) Run(cypher string, params *map[string]interface{}) (RequestHandle, error) {
	stmt := C.CString(cypher)
	defer C.free(unsafe.Pointer(stmt))

	var actualParams map[string]interface{}
	if params == nil {
		actualParams = map[string]interface{}(nil)
	} else {
		actualParams = *params
	}

	res := C.BoltConnection_cypher(connection.cInstance, stmt, C.size_t(len(cypher)), C.int32_t(len(actualParams)))
	if res < 0 {
		return -1, errors.New("unable to set cypher statement")
	}

	i := 0
	for k, v := range actualParams {
		index := C.int32_t(i)
		key := C.CString(k)

		boltValue := C.BoltConnection_cypher_parameter(connection.cInstance, index, key, C.size_t(len(k)))
		if boltValue == nil {
			return -1, errors.New("unable to get cypher statement parameter value to set")
		}

		valueAsConnector(boltValue, v)

		i += 1
	}

	res = C.BoltConnection_load_run_request(connection.cInstance)
	if res < 0 {
		return -1, errors.New("unable to generate RUN message")
	}

	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) PullAll() (RequestHandle, error) {
	res := C.BoltConnection_load_pull_request(connection.cInstance, -1)
	if res < 0 {
		return -1, errors.New("unable to generate PULLALL message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) DiscardAll() (RequestHandle, error) {
	res := C.BoltConnection_load_discard_request(connection.cInstance, -1)
	if res < 0 {
		return -1, errors.New("unable to generate DISCARDALL message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.cInstance)), nil
}

func (connection *neo4jConnection) assertReadyState() error {
	if connection.cInstance.status != C.BOLT_READY {
		return errors.New("expected connection to be in READY state, where it is " + string(connection.cInstance.status))
	}

	return nil
}

func (connection *neo4jConnection) Flush() error {
	res := C.BoltConnection_send(connection.cInstance)
	if res < 0 {
		return errors.New("unable to send pending messages")
	}

	return connection.assertReadyState()
}

func (connection *neo4jConnection) Fetch(request RequestHandle) (FetchType, error) {
	res := C.BoltConnection_fetch(connection.cInstance, C.bolt_request_t(request))
	if res < 0 {
		return -1, errors.New("unable to fetch from connection")
	}

	err := connection.assertReadyState()
	if err != nil {
		return -1, err
	}

	return FetchType(res), nil
}

func (connection *neo4jConnection) FetchSummary(request RequestHandle) (int, error) {
	res := C.BoltConnection_fetch_summary(connection.cInstance, C.bolt_request_t(request))
	if res < 0 {
		return -1, errors.New("unable to fetch summary from connection")
	}

	err := connection.assertReadyState()
	if err != nil {
		return -1, err
	}

	return int(res), nil
}

func (connection *neo4jConnection) Metadata() (map[string]interface{}, error) {
	metadata := make(map[string]interface{}, 1)

	fields, err := valueAsGo(C.BoltConnection_metadata_fields(connection.cInstance))
	if err != nil {
		return nil, err
	}

	if fields != nil {
		fieldsAsList := fields.([]interface{})
		fieldsAsStr := make([]string, len(fieldsAsList))
		for i := range fieldsAsList {
			fieldsAsStr[i] = fieldsAsList[i].(string)
		}
		metadata["fields"] = fieldsAsStr
	}

	return metadata, nil
}

func (connection *neo4jConnection) Data() ([]interface{}, error) {
	fields, err := valueAsGo(C.BoltConnection_record_fields(connection.cInstance))
	if err != nil {
		return nil, err
	}

	return fields.([]interface{}), nil
}

func (connection *neo4jConnection) Reset() error {
	res := C.BoltConnection_reset(connection.cInstance)
	if res < 0 {
		return errors.New("unable to reset connection")
	}

	err := connection.assertReadyState()
	if err != nil {
		return err
	}

	return nil
}

func (connection *neo4jConnection) Close() error {
	err := connection.pool.release(connection)
	if err != nil {
		return err
	}
	return nil
}
