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
#include "bolt/connections.h"
*/
import "C"
import (
	"fmt"
	"strings"
)

// DatabaseError represents errors returned from the server a FAILURE messages
type DatabaseError struct {
	classification string
	code           string
	message        string
}

// ConnectorError represents errors that occur on the connector/client side, like network errors, etc.
type ConnectorError struct {
	state       uint32
	code        uint32
	description string
}

// GenericError represents errors which originates from the connector wrapper itself
type GenericError struct {
	message string
}

// Classification returns classification of the error returned from the database
func (failure *DatabaseError) Classification() string {
	return failure.classification
}

// Code returns code of the error returned from the database
func (failure *DatabaseError) Code() string {
	return failure.code
}

// Message returns message of the error returned from the database
func (failure *DatabaseError) Message() string {
	return failure.message
}

// Error returns textual representation of the error returned from the database
func (failure *DatabaseError) Error() string {
	return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

// State returns the state of the related connection
func (failure *ConnectorError) State() uint32 {
	return failure.state
}

// Code returns the error code set on the related connection
func (failure *ConnectorError) Code() uint32 {
	return failure.code
}

// Description returns any additional description set
func (failure *ConnectorError) Description() string {
	return failure.description
}

// TODO: add some text description to the error message based on the state and error codes possibly from connector side
// Error returns textual representation of the connector level error
func (failure *ConnectorError) Error() string {
	return fmt.Sprintf("expected connection to be in READY state, where it is %d [error is %d]", failure.state, failure.code)
}

// Error returns textual representation of the generic error
func (failure *GenericError) Error() string {
	return failure.message
}

func newConnectionError(connection *neo4jConnection, description string) error {
	if connection.cInstance.error == C.BOLT_SERVER_FAILURE {
		status, err := connection.valueSystem.valueAsDictionary(C.BoltConnection_failure(connection.cInstance))
		if err != nil {
			return newGenericError("unable to construct database error: %s", err.Error())
		}

		return NewDatabaseError(status)
	}

	return newConnectionErrorWithCode(connection.cInstance.status, connection.cInstance.error, description)
}

func newGenericError(format string, args ...interface{}) error {
	return &GenericError{message: fmt.Sprintf(format, args...)}
}

// NewDatabaseError creates a new DatabaseError with provided details map consisting of
// `code` and `message` keys
func NewDatabaseError(details map[string]interface{}) error {
	var ok bool
	var codeInt, messageInt interface{}

	if codeInt, ok = details["code"]; !ok {
		return newGenericError("expected 'code' key to be present in map '%v'", details)
	}

	if messageInt, ok = details["message"]; !ok {
		return newGenericError("expected 'message' key to be present in map '%v'", details)
	}

	code := codeInt.(string)
	message := messageInt.(string)
	classification := ""
	if codeParts := strings.Split(code, "."); len(codeParts) >= 2 {
		classification = codeParts[1]
	}

	return &DatabaseError{code: code, message: message, classification: classification}
}

func newConnectionErrorWithCode(state uint32, code uint32, description string) error {
	return &ConnectorError{state: state, code: code, description: description}
}

// IsDatabaseError checkes whether given err is a DatabaseError
func IsDatabaseError(err error) bool {
	_, ok := err.(*DatabaseError)
	return ok
}

// IsConnectorError checkes whether given err is a ConnectorError
func IsConnectorError(err error) bool {
	_, ok := err.(*ConnectorError)
	return ok
}

// IsTransientError checks whether given err is a transient error
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

// IsWriteError checks whether given err can be classified as a write error
func IsWriteError(err error) bool {
	if dbErr, ok := err.(*DatabaseError); ok {
		switch dbErr.code {
		case "Neo.ClientError.Cluster.NotALeader":
			fallthrough
		case "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase":
			return true
		}
	}

	return false
}

// IsServiceUnavailable checkes whether given err represents a service unavailable status
func IsServiceUnavailable(err error) bool {
	if IsConnectorError(err) {
		switch err.(*ConnectorError).code {
		case C.BOLT_INTERRUPTED:
			fallthrough
		case C.BOLT_CONNECTION_RESET:
			fallthrough
		case C.BOLT_NO_VALID_ADDRESS:
			fallthrough
		case C.BOLT_TIMED_OUT:
			fallthrough
		case C.BOLT_CONNECTION_REFUSED:
			fallthrough
		case C.BOLT_NETWORK_UNREACHABLE:
			fallthrough
		case C.BOLT_TLS_ERROR:
			fallthrough
		case C.BOLT_END_OF_TRANSMISSION:
			fallthrough
		case C.BOLT_POOL_FULL:
			fallthrough
		case C.BOLT_ADDRESS_NOT_RESOLVED:
			fallthrough
		case C.BOLT_ROUTING_UNABLE_TO_RETRIEVE_ROUTING_TABLE:
			fallthrough
		case C.BOLT_ROUTING_UNABLE_TO_REFRESH_ROUTING_TABLE:
			fallthrough
		case C.BOLT_ROUTING_NO_SERVERS_TO_SELECT:
			return true
		}
	}

	return false
}
