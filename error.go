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
type DatabaseError interface {
	// Classification returns classification of the error returned from the database
	Classification() string
	// Code returns code of the error returned from the database
	Code() string
	// Message returns message of the error returned from the database
	Message() string
	// Error returns textual representation of the error returned from the database
	Error() string
}

// ConnectorError represents errors that occur on the connector/client side, like network errors, etc.
type ConnectorError interface {
	// State returns the state of the related connection
	State() int
	// Code returns the error code set on the related connection
	Code() int
	// Description returns any additional description set
	Description() string
	// Error returns textual representation of the connector level error
	Error() string
}

// GenericError represents errors which originates from the connector wrapper itself
type GenericError interface {
	// Error returns textual representation of the generic error
	Error() string
}

type defaultDatabaseError struct {
	classification string
	code           string
	message        string
}

type defaultConnectorError struct {
	state       int
	code        int
	description string
}

type defaultGenericError struct {
	message string
}

func (failure *defaultDatabaseError) Classification() string {
	return failure.classification
}

func (failure *defaultDatabaseError) Code() string {
	return failure.code
}

func (failure *defaultDatabaseError) Message() string {
	return failure.message
}

func (failure *defaultDatabaseError) Error() string {
	return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

func (failure *defaultConnectorError) State() int {
	return failure.state
}

func (failure *defaultConnectorError) Code() int {
	return failure.code
}

func (failure *defaultConnectorError) Description() string {
	return failure.description
}

func (failure *defaultConnectorError) Error() string {
	return fmt.Sprintf("expected connection to be in READY state, where it is %d [error is %d]", failure.state, failure.code)
}

func (failure *defaultGenericError) Error() string {
	return failure.message
}

func newError(connection *neo4jConnection, description string) error {
	if connection.cInstance.error == C.BOLT_SERVER_FAILURE {
		failure, err := connection.valueSystem.valueAsDictionary(C.BoltConnection_failure(connection.cInstance))
		if err != nil {
			return connection.valueSystem.genericErrorFactory("unable to construct database error: %s", err.Error())
		}

		var ok bool
		var codeInt, messageInt interface{}
		var code, message string

		if codeInt, ok = failure["code"]; !ok {
			return connection.valueSystem.genericErrorFactory("expected 'code' key to be present in map '%v'", failure)
		}
		if code, ok = codeInt.(string); !ok {
			return connection.valueSystem.genericErrorFactory("expected 'code' value to be of type 'string': '%v'", codeInt)
		}

		if messageInt, ok = failure["message"]; !ok {
			return connection.valueSystem.genericErrorFactory("expected 'message' key to be present in map '%v'", failure)
		}
		if message, ok = messageInt.(string); !ok {
			return connection.valueSystem.genericErrorFactory("expected 'message' value to be of type 'string': '%v'", messageInt)
		}

		classification := ""
		if codeParts := strings.Split(code, "."); len(codeParts) >= 2 {
			classification = codeParts[1]
		}

		return connection.valueSystem.databaseErrorFactory(classification, code, message)
	}

	return connection.valueSystem.connectorErrorFactory(int(connection.cInstance.status), int(connection.cInstance.error), description)
}

func newGenericError(format string, args ...interface{}) GenericError {
	return &defaultGenericError{message: fmt.Sprintf(format, args...)}
}

func newDatabaseError(classification, code, message string) DatabaseError {
	return &defaultDatabaseError{code: code, message: message, classification: classification}
}

func newConnectorError(state int, code int, description string) ConnectorError {
	return &defaultConnectorError{state: state, code: code, description: description}
}

// IsDatabaseError checkes whether given err is a DatabaseError
func IsDatabaseError(err error) bool {
	_, ok := err.(DatabaseError)
	return ok
}

// IsConnectorError checkes whether given err is a ConnectorError
func IsConnectorError(err error) bool {
	_, ok := err.(ConnectorError)
	return ok
}

// IsTransientError checks whether given err is a transient error
func IsTransientError(err error) bool {
	if dbErr, ok := err.(DatabaseError); ok {
		if dbErr.Classification() == "TransientError" {
			switch dbErr.Code() {
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
	if dbErr, ok := err.(DatabaseError); ok {
		switch dbErr.Code() {
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
		switch err.(ConnectorError).Code() {
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
