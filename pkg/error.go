package neo4j

/*
#cgo CFLAGS: -ID:/Projects.GitHub/seabolt/seabolt/include -I/home/ali/Projects/neo4j/seabolt/seabolt/include
#cgo LDFLAGS: -LD:/Projects.GitHub/seabolt/build/lib -L/home/ali/Projects/neo4j/seabolt/build/lib -lseabolt

#include "bolt/connections.h"
*/
import "C"
import (
    "fmt"
)

type DatabaseFailure struct {
    code    string
    message string
}

type ConnectorFailure struct {
    state int
    error int
}

func (failure *DatabaseFailure) Code() string {
    return failure.code
}

func (failure *DatabaseFailure) Message() string {
    return failure.message
}

func (failure *DatabaseFailure) Error() string {
    return fmt.Sprintf("database returned error [%s]: %s", failure.code, failure.message)
}

func (failure *ConnectorFailure) Error() string {
    return fmt.Sprintf("expected connection to be in READY state, where it is %d [error is %d]", failure.state, failure.error)
}

func newDatabaseFailure(details map[string]interface{}) error {
    var ok bool
    var code, message interface{}

    if code, ok = details["code"]; !ok {
        return fmt.Errorf("expected 'code' key to be present in map '%g'", details)
    }

    if message, ok = details["message"]; !ok {
        return fmt.Errorf("expected 'message' key to be present in map '%g'", details)
    }

    return &DatabaseFailure{code: code.(string), message: message.(string)}
}

func newConnectionFailure(connection *neo4jConnection) error {
    return &ConnectorFailure{state: int(connection.cInstance.status), error: int(connection.cInstance.error)}
}

func IsDatabaseFailure(err error) bool {
    _, ok := err.(*DatabaseFailure)
    return ok
}

func IsConnectorFailure(err error) bool {
    _, ok := err.(*ConnectorFailure)
    return ok
}

func IsServiceUnavailable(err error) bool {
    if IsDatabaseFailure(err) {
        return false
    } else if IsConnectorFailure(err) {
        switch err.(*ConnectorFailure).error {
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
