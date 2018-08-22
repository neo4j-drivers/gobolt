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

func (failure *ConnectorError) State() uint32 {
	return failure.state
}

func (failure *ConnectorError) Code() uint32 {
	return failure.code
}

func (failure *ConnectorError) Description() string {
	return failure.description
}

// TODO: add some text description to the error message based on the state and error codes possibly from connector side
// Error returns textual representation of the connector level error
func (failure *ConnectorError) Error() string {
	return fmt.Sprintf("expected connection to be in READY state, where it is %d [error is %d]", failure.state, failure.code)
}

func NewDatabaseError(details map[string]interface{}) error {
	var ok bool
	var codeInt, messageInt interface{}

	if codeInt, ok = details["code"]; !ok {
		return fmt.Errorf("expected 'code' key to be present in map '%v'", details)
	}

	if messageInt, ok = details["message"]; !ok {
		return fmt.Errorf("expected 'message' key to be present in map '%v'", details)
	}

	code := codeInt.(string)
	message := messageInt.(string)
	classification := ""
	if codeParts := strings.Split(code, "."); len(codeParts) >= 2 {
		classification = codeParts[1]
	}

	return &DatabaseError{code: code, message: message, classification: classification}
}

func newConnectionError(connection *neo4jConnection, description string) error {
	return newConnectionErrorWithCode(connection.cInstance.status, connection.cInstance.error, description)
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

// IsTransientError checkes whether given err is a transient error
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

// IsServiceUnavailable checkes whether given err represents a service unavailable status
func IsServiceUnavailable(err error) bool {
	if IsDatabaseError(err) {
		// TODO: Add specific failure codes while adding routing driver support
		return false
	} else if IsConnectorError(err) {
		switch err.(*ConnectorError).code {
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
		case C.BOLT_POOL_FULL:
			fallthrough
		case C.BOLT_TIMED_OUT:
			return true
		}
	}

	return false
}
