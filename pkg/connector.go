package neo4j

/*
#cgo CFLAGS: -ID:/Projects.GitHub/seabolt/seabolt/include -I/home/ali/Projects/neo4j/seabolt/seabolt/include
#cgo LDFLAGS: -LD:/Projects.GitHub/seabolt/build/lib -L/home/ali/Projects/neo4j/seabolt/build/lib -lseabolt

#include <memory.h>
#include <stdlib.h>

#include "bolt/addressing.h"
#include "bolt/connections.h"
#include "bolt/lifecycle.h"
#include "bolt/pooling.h"
#include "bolt/mem.h"

FILE *get_stdout() {
	return stdout;
}

*/
import "C"
import (
	"sync/atomic"
	"errors"
	"net/url"
	"unsafe"
)

type Connector interface {
	GetPool() (Pool, error)
	Close() error
}

type Pool interface {
	Acquire() (Connection, error)
	Close() error
}

type Connection interface {
	Run(cypher string, args map[string]interface{})  (RequestHandle, error)
	PullAll() (RequestHandle, error)
	DiscardAll() (RequestHandle, error)
	Flush() error
	Fetch(request RequestHandle) (FetchType, error) // return type ?
	FetchSummary(request RequestHandle) (int, error) // return type ?
	Summary() (map[string]interface{}, error)
	Data() (interface{}, error)

	Reset() error
	Close() error
}

type RequestHandle int64

type FetchType int
const (
	RECORD FetchType = 1
	METADATA = 0
)

var initCounter int32 = 0

type Config struct{
	Encryption bool
	Debug bool
}

type internalConnector struct {
	uri       *url.URL
	authToken map[string]interface{}
	config    Config

	address *C.struct_BoltAddress
	pool    *internalPool
}

type internalPool struct {
	poolInstance *C.struct_BoltConnectionPool
}

type internalConnection struct {
	pool               *internalPool
	connectionInstance *C.struct_BoltConnection
}

func (conn *internalConnector) Close() error {
	C.BoltAddress_destroy(conn.address)
	shutdownLibrary()
	return nil
}

func (conn *internalConnector) GetPool() (Pool, error) {
	if conn.pool == nil {
		user, password, userAgent := C.CString(conn.authToken["principal"].(string)), C.CString(conn.authToken["credentials"].(string)), C.CString("Go Driver/1.0")
		defer C.free(unsafe.Pointer(user))
		defer C.free(unsafe.Pointer(password))
		defer C.free(unsafe.Pointer(userAgent))

		userProfile := C.struct_BoltUserProfile{
			auth_scheme: C.BOLT_AUTH_BASIC,
			user:        user,
			password:    password,
			user_agent:   userAgent,
		}

		socketType := C.BOLT_SOCKET
		if conn.config.Encryption {
			socketType = C.BOLT_SECURE_SOCKET
		}

		poolInstance := C.BoltConnectionPool_create(uint32(socketType), conn.address, &userProfile, 100)

		return &internalPool{poolInstance: poolInstance}, nil
	}
	return conn.pool, nil
}

func GetAllocationStats() (int64, int64, int64) {
	current := C.BoltMem_current_allocation()
	peak := C.BoltMem_peak_allocation()
	events := C.BoltMem_allocation_events()

	return int64(current), int64(peak), int64(events)
}

func (pool *internalPool) Acquire() (Connection, error) {
	conn := C.BoltConnectionPool_acquire(pool.poolInstance, nil)
	if conn == nil {
		return nil, errors.New("unable to acquire connection from the pool")
	}
	return &internalConnection{connectionInstance: conn, pool: pool}, nil
}

func (pool *internalPool) release(connection *internalConnection) error {
	res := C.BoltConnectionPool_release(pool.poolInstance, connection.connectionInstance)
	if res < 0 {
		return errors.New("connection instance is not part of the pool")
	}
	return nil
}

func (pool *internalPool) Close() error {
	C.BoltConnectionPool_destroy(pool.poolInstance)
	return nil
}

func (connection *internalConnection) Run(cypher string, params map[string]interface{}) (RequestHandle, error) {
	stmt := C.CString(cypher)
	defer C.free(unsafe.Pointer(stmt))

	res := C.BoltConnection_cypher(connection.connectionInstance, stmt, C.size_t(len(cypher)), C.int32_t(len(params)))
	if res < 0 {
		return -1, errors.New("unable to set cypher statement")
	}

	i := 0
	for k, v := range params {
		index := C.int32_t(i)
		key := C.CString(k)

		boltValue := C.BoltConnection_cypher_parameter(connection.connectionInstance, index, key, C.size_t(len(k)))
		if boltValue == nil {
			return -1, errors.New("unable to get cypher statement parameter value to set")
		}

		valueAsConnector(boltValue, v)

		i += 1
	}

	res = C.BoltConnection_load_run_request(connection.connectionInstance)
	if res < 0 {
		return -1, errors.New("unable to generate RUN message")
	}

	return RequestHandle(C.BoltConnection_last_request(connection.connectionInstance)), nil
}

func (connection *internalConnection) PullAll() (RequestHandle, error)  {
	res := C.BoltConnection_load_pull_request(connection.connectionInstance, -1)
	if res < 0 {
		return -1, errors.New("unable to generate PULLALL message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.connectionInstance)), nil
}

func (connection *internalConnection) DiscardAll() (RequestHandle, error)  {
	res := C.BoltConnection_load_discard_request(connection.connectionInstance, -1)
	if res < 0 {
		return -1, errors.New("unable to generate DISCARDALL message")
	}
	return RequestHandle(C.BoltConnection_last_request(connection.connectionInstance)), nil
}

func (connection *internalConnection) Flush() error  {
	res := C.BoltConnection_send(connection.connectionInstance)
	if res < 0 {
		return errors.New("unable to send pending messages")
	}
	return nil
}

func (connection *internalConnection) Fetch(request RequestHandle) (FetchType, error)  {
	res := C.BoltConnection_fetch(connection.connectionInstance, C.bolt_request_t(request))
	if res < 0 {
		return -1, errors.New("unable to fetch from connection")
	}

	return FetchType(res), nil
}

func (connection *internalConnection) FetchSummary(request RequestHandle) (int, error)  {
	res := C.BoltConnection_fetch_summary(connection.connectionInstance, C.bolt_request_t(request))
	if res < 0 {
		return -1, errors.New("unable to fetch summary from connection")
	}

	return int(res), nil
}

func (connection *internalConnection) Summary() (map[string]interface{}, error)  {
	metadata := make(map[string]interface{}, 1)

	fieldsCount := int(C.BoltConnection_result_n_fields(connection.connectionInstance))
	if fieldsCount < 0 {
		return nil, errors.New("unable to get number of fields")
	}
	fields := make([]interface{}, fieldsCount)
	for i := 0; i < fieldsCount; i++ {
		fieldNameLength := C.BoltConnection_result_field_name_size(connection.connectionInstance, C.int32_t(i))
		if fieldNameLength < 0 {
			return nil, errors.New("unable to field name length at index " + string(i))
		}
		fieldNameC := C.BoltConnection_result_field_name(connection.connectionInstance, C.int32_t(i))
		if fieldNameC == nil {
			return nil, errors.New("unable to field name at index " + string(i))
		}

		fields[i] = C.GoStringN(fieldNameC, fieldNameLength)
	}

	metadata["fields"] = fields

	return metadata, nil
}

func (connection *internalConnection) Data() (interface{}, error)  {
	size := int(C.BoltConnection_record_size(connection.connectionInstance))
	data := make([]interface{}, size)
	for i := 0; i < size; i++ {
		field := C.BoltConnection_record_field(connection.connectionInstance, C.int32_t(i))
		fieldAsGo, err := valueAsGo(field)
		if err != nil {
			return nil, err
		}

		data[i] = fieldAsGo
	}
	return data, nil
}

func (connection *internalConnection) Reset() error {
	return nil
}

func (connection *internalConnection) Close() error {
	err := connection.pool.release(connection)
	if err != nil {
		return err
	}
	return nil
}

func NewConnector(uri string, authToken map[string]interface{}, config Config) (connector Connector, err error) {
	parsedUrl, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	hostname, port := C.CString(parsedUrl.Hostname()), C.CString(parsedUrl.Port())
	defer C.free(unsafe.Pointer(hostname))
	defer C.free(unsafe.Pointer(port))
	address := C.BoltAddress_create(hostname, port)

	startupLibrary(config.Debug)
	conn := &internalConnector{uri: parsedUrl, authToken: authToken, config: config, address: address}
	return conn, nil
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
