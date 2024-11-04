// Copyright © 2023 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

import "errors"

var (
	// ErrNilSDKPosition occurs when trying to parse a nil [opencdc.Position].
	// It's just a sentinel error for the [parsePosition] function.
	ErrNilSDKPosition = errors.New("nil sdk position")

	// errNoElements occurs when trying to read elements
	// but Neo4j returns nothing.
	errNoElements = errors.New("no elements")

	// errConvertRawNode occurs when trying to convert a raw element to [dbtype.Node]
	// and this process fails.
	errConvertRawNode = errors.New("cannot convert raw element to dbtype node")

	// errConvertRawRelationship occurs when trying to convert a raw element to [dbtype.Relationship]
	// and this process fails.
	errConvertRawRelationship = errors.New("cannot convert raw element to dbtype relationship")

	// neo4jNoMoreRecordsErrorMessage is a message
	// that Neo4j returns when it cannot find records.
	neo4jNoMoreRecordsErrorMessage = "Result contains no more records"
)
