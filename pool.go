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
#include "bolt/pooling.h"
*/
import "C"
import "errors"

// Pool represents an instance of a pool of connections
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
		return nil, newConnectFailure()
	}
	return &neo4jConnection{cInstance: cInstance, pool: pool}, nil
}

func (pool *neo4jPool) Close() error {
	if pool.cInstance != nil {
		C.BoltConnectionPool_destroy(pool.cInstance)
		pool.cInstance = nil
	}

	return nil
}

func (pool *neo4jPool) release(connection *neo4jConnection) error {
	res := C.BoltConnectionPool_release(pool.cInstance, connection.cInstance)
	if res < 0 {
		return errors.New("connection instance is not part of the pool")
	}
	return nil
}
