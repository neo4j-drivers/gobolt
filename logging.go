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
#include "bolt/logging.h"

extern void go_seabolt_log_error_cb(int state, char* message);
extern void go_seabolt_log_warning_cb(int state, char* message);
extern void go_seabolt_log_info_cb(int state, char* message);
extern void go_seabolt_log_debug_cb(int state, char* message);
*/
import "C"
import "sync"

type Logging interface {
	ErrorEnabled() bool
	WarningEnabled() bool
	InfoEnabled() bool
	DebugEnabled() bool

	Errorf(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Debugf(message string, args ...interface{})
}

//export go_seabolt_log_error_cb
func go_seabolt_log_error_cb(state C.int, message *C.char) {
	logging := lookupLogging(state)
	if logging != nil && logging.ErrorEnabled() {
		logging.Errorf(C.GoString(message))
	}
}

//export go_seabolt_log_warning_cb
func go_seabolt_log_warning_cb(state C.int, message *C.char) {
	logging := lookupLogging(state)
	if logging != nil && logging.WarningEnabled() {
		logging.Warningf(C.GoString(message))
	}
}

//export go_seabolt_log_info_cb
func go_seabolt_log_info_cb(state C.int, message *C.char) {
	logging := lookupLogging(state)
	if logging != nil && logging.InfoEnabled() {
		logging.Infof(C.GoString(message))
	}
}

//export go_seabolt_log_debug_cb
func go_seabolt_log_debug_cb(state C.int, message *C.char) {
	logging := lookupLogging(state)
	if logging != nil && logging.DebugEnabled() {
		logging.Debugf(C.GoString(message))
	}
}

var mapLogging sync.Map

func registerLogging(key int, logging Logging) *C.struct_BoltLog {
	if logging != nil {
		mapLogging.Store(key, logging)
	}

	boltLog := C.BoltLog_create()
	boltLog.state = C.int(key)

	boltLog.error_enabled = 0
	if logging != nil && logging.ErrorEnabled() {
		boltLog.error_enabled = 1
	}
	boltLog.error_logger = C.log_func(C.go_seabolt_log_error_cb)

	boltLog.warning_enabled = 0
	if logging != nil && logging.WarningEnabled() {
		boltLog.warning_enabled = 1
	}
	boltLog.warning_logger = C.log_func(C.go_seabolt_log_warning_cb)

	boltLog.info_enabled = 0
	if logging != nil && logging.InfoEnabled() {
		boltLog.info_enabled = 1
	}
	boltLog.info_logger = C.log_func(C.go_seabolt_log_info_cb)

	boltLog.debug_enabled = 0
	if logging != nil && logging.DebugEnabled() {
		boltLog.debug_enabled = 1
	}
	boltLog.debug_logger = C.log_func(C.go_seabolt_log_debug_cb)

	return boltLog
}

func lookupLogging(key C.int) Logging {
	if logging, ok := mapLogging.Load(int(key)); ok {
		return logging.(Logging)
	}

	return nil
}

func unregisterLogging(key int) {
	mapLogging.Delete(key)
}
