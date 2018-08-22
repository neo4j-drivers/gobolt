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

package main

import (
	"log"
	"io"
	"github.com/neo4j-drivers/gobolt"
)

type logLevel int

const (
	// LogLevelError is the level that error messages are written
	logLevelError logLevel = 1
	// LogLevelWarning is the level that warning messages are written
	logLevelWarning = 2
	// LogLevelInfo is the level that info messages are written
	logLevelInfo = 3
	// LogLevelDebug is the level that debug messages are written
	logLevelDebug = 4
)

type internalLogger struct {
	level         logLevel
	errorLogger   *log.Logger
	warningLogger *log.Logger
	infoLogger    *log.Logger
	debugLogger   *log.Logger
}

func simpleLogger(level logLevel, writer io.Writer) gobolt.Logging {
	return &internalLogger{
		level:         level,
		errorLogger:   log.New(writer, "ERROR  : ", log.Ldate|log.Ltime|log.Lmicroseconds),
		warningLogger: log.New(writer, "WARNING: ", log.Ldate|log.Ltime|log.Lmicroseconds),
		infoLogger:    log.New(writer, "INFO   : ", log.Ldate|log.Ltime|log.Lmicroseconds),
		debugLogger:   log.New(writer, "DEBUG  : ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}

func (logger *internalLogger) ErrorEnabled() bool {
	return logLevelError <= logger.level
}

func (logger *internalLogger) WarningEnabled() bool {
	return logLevelWarning <= logger.level
}

func (logger *internalLogger) InfoEnabled() bool {
	return logLevelInfo <= logger.level
}

func (logger *internalLogger) DebugEnabled() bool {
	return logLevelDebug <= logger.level
}

func (logger *internalLogger) Errorf(message string, args ...interface{}) {
	logger.errorLogger.Printf(message, args...)
}

func (logger *internalLogger) Warningf(message string, args ...interface{}) {
	logger.warningLogger.Printf(message, args...)
}

func (logger *internalLogger) Infof(message string, args ...interface{}) {
	logger.infoLogger.Printf(message, args...)
}

func (logger *internalLogger) Debugf(message string, args ...interface{}) {
	logger.debugLogger.Printf(message, args...)
}
