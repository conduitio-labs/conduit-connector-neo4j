// Copyright © 2022 Meroxa, Inc. & Yalantis
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

// Package neo4j implements Neo4j connector for Conduit.
// It provides both, a source and a destination Neo4j connector.
package neo4j

import (
	"github.com/conduitio-labs/conduit-connector-neo4j/destination"
	"github.com/conduitio-labs/conduit-connector-neo4j/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        source.NewSource,
	NewDestination:   destination.NewDestination,
}
