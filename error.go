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
#cgo pkg-config: seabolt

#include "bolt/connections.h"
*/
import "C"
import (
	"fmt"
	"strings"
)

type DatabaseError struct {
	classification string
	code           string
	message        string
}

type ConnectorError struct {
	state int
	error int
}

type SessionExpiredError struct {
	message string
}

func (failure *DatabaseError) Code() string {
	return failure.code
}

func (failure *DatabaseError) Message() string {
	return failure.message
}

func (failure *DatabaseError) Error() string {
	return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

func (failure *SessionExpiredError) Error() string {
	return fmt.Sprintf("session expired: %s", failure.message)
}

// TODO: add some text description to the error message based on the state and error codes possibly from connector side
func (failure *ConnectorError) Error() string {
	return fmt.Sprintf("expected connection to be in READY state, where it is %d [error is %d]", failure.state, failure.error)
}

func newDatabaseError(details map[string]interface{}) error {
	var ok bool
	var codeInt, messageInt interface{}

	if codeInt, ok = details["code"]; !ok {
		return fmt.Errorf("expected 'code' key to be present in map '%g'", details)
	}

	if messageInt, ok = details["message"]; !ok {
		return fmt.Errorf("expected 'message' key to be present in map '%g'", details)
	}

	code := codeInt.(string)
	message := messageInt.(string)
	classification := ""
	if codeParts := strings.Split(code, "."); len(codeParts) >= 2 {
		classification = codeParts[1]
	}

	return &DatabaseError{code: code, message: message, classification: classification}
}

func newConnectFailure() error {
	return newConnectionErrorWithCode(int(C.BOLT_DEFUNCT), int(C.BOLT_CONNECTION_REFUSED))
}

func newConnectionError(connection *neo4jConnection) error {
	return newConnectionErrorWithCode(int(connection.cInstance.status), int(connection.cInstance.error))
}

func newConnectionErrorWithCode(state int, error int) error {
	return &ConnectorError{state: state, error: error}
}

func newSessionExpiredException(message string) error {
	return &SessionExpiredError{message: message}
}

func IsDatabaseError(err error) bool {
	_, ok := err.(*DatabaseError)
	return ok
}

func IsConnectorError(err error) bool {
	_, ok := err.(*ConnectorError)
	return ok
}

func IsSessionExpired(err error) bool {
	_, ok := err.(*SessionExpiredError)
	return ok
}

func IsTransientError(err error) bool {
	if dbErr, ok := err.(*DatabaseError); ok {
		if dbErr.classification == "TransientError" {
			switch dbErr.code {
			case "Neo.TransientError.Transaction.Terminated":
				fallthrough
			case "Neo.TransientError.Transaction.LockClientStopped":
				return false
			}

			return true
		}
	}

	return false
}

func IsServiceUnavailable(err error) bool {
	if IsDatabaseError(err) {
		// TODO: Add specific failure codes while adding routing driver support
		return false
	} else if IsConnectorError(err) {
		switch err.(*ConnectorError).error {
		case C.BOLT_END_OF_TRANSMISSION:
			fallthrough
		case C.BOLT_TLS_ERROR:
			fallthrough
		case C.BOLT_NETWORK_UNREACHABLE:
			fallthrough
		case C.BOLT_CONNECTION_REFUSED:
			fallthrough
		case C.BOLT_INTERRUPTED:
			fallthrough
		case C.BOLT_CONNECTION_RESET:
			fallthrough
		case C.BOLT_TIMED_OUT:
			return true
		}
	}

	return false
}
