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
#include "bolt/address-resolver.h"

extern void go_seabolt_server_address_resolver_cb(int state, struct BoltAddress *address, struct BoltAddressSet *resolved);
*/
import "C"
import (
	"sync"
	"unsafe"
)

type ServerAddress interface {
	Host() string
	Port() string
}

type ServerAddressResolver interface {
	Resolve(address ServerAddress) []ServerAddress
}

type internalServerAddress struct {
	host string
	port string
}

func (address *internalServerAddress) Host() string {
	return address.host
}

func (address *internalServerAddress) Port() string {
	return address.port
}

//export go_seabolt_server_address_resolver_cb
func go_seabolt_server_address_resolver_cb(state C.int, address *C.struct_BoltAddress, resolved *C.struct_BoltAddressSet) {
	resolver := lookupResolver(state)
	if resolver != nil {
		resolvedAddresses :=
			resolver.Resolve(&internalServerAddress{host: C.GoString(address.host), port: C.GoString(address.port)})

		for _, addr := range resolvedAddresses {
			cHost := C.CString(addr.Host())
			cPort := C.CString(addr.Port())
			cAddress := C.BoltAddress_create(cHost, cPort)

			C.BoltAddressSet_add(resolved, *cAddress)

			C.BoltAddress_destroy(cAddress)
			C.free(unsafe.Pointer(cHost))
			C.free(unsafe.Pointer(cPort))
		}
	}
}

var mapResolver sync.Map

func registerResolver(key int, resolver ServerAddressResolver) *C.struct_BoltAddressResolver {
	if resolver != nil {
		mapResolver.Store(key, resolver)
	}

	boltResolver := C.BoltAddressResolver_create()
	boltResolver.state = C.int(key)
	boltResolver.resolver = C.address_resolver_func(C.go_seabolt_server_address_resolver_cb)
	return boltResolver
}

func lookupResolver(key C.int) ServerAddressResolver {
	if resolver, ok := mapResolver.Load(int(key)); ok {
		return resolver.(ServerAddressResolver)
	}

	return nil
}

func unregisterResolver(key int) {
	mapResolver.Delete(key)
}
