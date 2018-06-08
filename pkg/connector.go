package neo4j

/*
#cgo CFLAGS: -ID:/Projects.GitHub/seabolt/seabolt/include -I/home/ali/Projects/neo4j/seabolt/seabolt/include
#cgo LDFLAGS: -LD:/Projects.GitHub/seabolt/build/lib -L/home/ali/Projects/neo4j/seabolt/build/lib -lseabolt

#include <stdlib.h>

#include "bolt/lifecycle.h"
#include "bolt/pooling.h"
#include "bolt/mem.h"
*/
import "C"
import (
	"sync/atomic"
	"net/url"
	"unsafe"
)

type Connector interface {
	GetPool() (Pool, error)
	Close() error
}

type RequestHandle int64

type FetchType int
const (
	RECORD FetchType = 1
	METADATA = 0
	ERROR = -1
)

var initCounter int32 = 0

type Config struct{
	Encryption bool
	Debug bool
}

type neo4jConnector struct {
	uri       *url.URL
	authToken map[string]interface{}
	config    Config

	address *C.struct_BoltAddress
	pool    *neo4jPool
}

func (conn *neo4jConnector) Close() error {
	C.BoltAddress_destroy(conn.address)
	shutdownLibrary()
	return nil
}

func (conn *neo4jConnector) GetPool() (Pool, error) {
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

		cInstance := C.BoltConnectionPool_create(uint32(socketType), conn.address, &userProfile, 100)

		return &neo4jPool{cInstance: cInstance}, nil
	}
	return conn.pool, nil
}

func GetAllocationStats() (int64, int64, int64) {
	current := C.BoltMem_current_allocation()
	peak := C.BoltMem_peak_allocation()
	events := C.BoltMem_allocation_events()

	return int64(current), int64(peak), int64(events)
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
	conn := &neo4jConnector{uri: parsedUrl, authToken: authToken, config: config, address: address}
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
