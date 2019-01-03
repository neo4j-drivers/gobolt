/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
	"errors"
	"flag"
	"fmt"
	"github.com/neo4j-drivers/gobolt"
	"net/url"
	"os"
	"strings"
)

var (
	uri      string
	username string
	password string
	secure   bool
	query    string
	mode     string
	debug    bool
)

func executeQuery() {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		panic(err)
	}

	logger := simpleLogger(logLevelDebug, os.Stderr)

	connector, err := gobolt.NewConnector(parsedURI, map[string]interface{}{
		"scheme":      "basic",
		"principal":   username,
		"credentials": password,
	}, &gobolt.Config{Encryption: secure, TLSSkipVerify: true, TLSSkipVerifyHostname: true, MaxPoolSize: 10, Log: logger})
	if err != nil {
		panic(err)
	}
	defer connector.Close()

	accessMode := gobolt.AccessModeWrite
	if strings.ToLower(mode) == "read" {
		accessMode = gobolt.AccessModeRead
	}

	conn, err := connector.Acquire(accessMode)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	runMsg, err := conn.Run(query, nil, nil, 0, nil)
	if err != nil {
		panic(err)
	}

	pullAllMsg, err := conn.PullAll()
	if err != nil {
		panic(err)
	}

	err = conn.Flush()
	if err != nil {
		panic(err)
	}

	records, err := conn.FetchSummary(runMsg)
	if records != 0 {
		panic(errors.New("unexpected summary fetch return"))
	}

	fields, err := conn.Fields()
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(fields); i++ {
		if i > 0 {
			fmt.Print("\t")
		}

		fmt.Print(fields[i])
	}
	fmt.Println()

	for {
		fetch, err := conn.Fetch(pullAllMsg)
		if err != nil {
			panic(err)
		}
		if fetch <= 0 {
			break
		}

		data, err := conn.Data()
		if err != nil {
			panic(err)
		}

		for i := 0; i < len(data); i++ {
			if i > 0 {
				fmt.Print("\t")
			}

			fmt.Print(data[i])
		}

		fmt.Println()
	}
}

func main() {
	flag.Parse()
	executeQuery()

	current, peak, events := gobolt.GetAllocationStats()

	fmt.Fprintf(os.Stderr, "=====================================\n")
	fmt.Fprintf(os.Stderr, "current allocation	: %d bytes\n", current)
	fmt.Fprintf(os.Stderr, "peak allocation		: %d bytes\n", peak)
	fmt.Fprintf(os.Stderr, "allocation events	: %d\n", events)
	fmt.Fprintf(os.Stderr, "=====================================\n")
}

func init() {
	flag.BoolVar(&secure, "secure", true, "whether to use TLS encryption")
	flag.StringVar(&uri, "uri", "bolt://localhost:7687", "bolt uri to establish connection against")
	flag.StringVar(&username, "username", "neo4j", "bolt user name")

	flag.StringVar(&password, "password", "neo4j", "bolt password")
	flag.StringVar(&query, "query", "UNWIND RANGE(1,1000) AS N RETURN N", "cypher query to run")
	flag.StringVar(&mode, "mode", "write", "access mode for routing mode (read or write)")
	flag.BoolVar(&debug, "debug", true, "whether to use debug logging")
}
