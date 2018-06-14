package neo4j

/*
#cgo CFLAGS: -ID:/Projects.GitHub/seabolt/seabolt/include -I/home/ali/Projects/neo4j/seabolt/seabolt/include
#cgo LDFLAGS: -LD:/Projects.GitHub/seabolt/build/lib -L/home/ali/Projects/neo4j/seabolt/build/lib -lseabolt

#include "bolt/pooling.h"
*/
import "C"
import "errors"

type Pool interface {
	Acquire() (Connection, error)
	Close() error
}

type neo4jPool struct {
	cInstance *C.struct_BoltConnectionPool
}

func (pool *neo4jPool) Acquire() (Connection, error) {
	cInstance := C.BoltConnectionPool_acquire(pool.cInstance, nil)
	if cInstance == nil {
		return nil, errors.New("unable to acquire connection from the pool")
	}
	return &neo4jConnection{cInstance: cInstance, pool: pool}, nil
}

func (pool *neo4jPool) Close() error {
	C.BoltConnectionPool_destroy(pool.cInstance)
	return nil
}

func (pool *neo4jPool) release(connection *neo4jConnection) error {
	res := C.BoltConnectionPool_release(pool.cInstance, connection.cInstance)
	if res < 0 {
		return errors.New("connection instance is not part of the pool")
	}
	return nil
}
