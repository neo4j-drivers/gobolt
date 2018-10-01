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

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
)

// Config holds the available configurations options applicable to the connector
type Config struct {
	Encryption            bool
	TLSCertificates       []*x509.Certificate
	TLSSkipVerify         bool
	TLSSkipVerifyHostname bool
	MaxPoolSize           int
	Log                   Logging
	AddressResolver       UrlAddressResolver
	ValueHandlers         []ValueHandler
}

func pemEncodeCerts(certs []*x509.Certificate) (*bytes.Buffer, error) {
	if len(certs) == 0 {
		return nil, nil
	}

	var buf = &bytes.Buffer{}
	for _, cert := range certs {
		if err := pem.Encode(buf, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
			return nil, err
		}
	}
	return buf, nil
}
