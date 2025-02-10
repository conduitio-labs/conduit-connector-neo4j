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

//go:generate paramgen -output=paramgen.go Config

package source

import "github.com/conduitio-labs/conduit-connector-neo4j/config"

// Config holds configurable values specific to source.
type Config struct {
	config.Config

	// The name of a property that is used for ordering
	// nodes or relationships when capturing a snapshot.
	OrderingProperty string `json:"orderingProperty" validate:"required"`
	// The list of property names that are used for constructing a record key.
	KeyProperties []string `json:"keyProperties"`
	// The size of an element batch.
	BatchSize int `json:"batchSize" validate:"gt=0,lt=100001" default:"1000"`
	// Determines whether or not the connector will take a snapshot
	// of all nodes or relationships before starting polling mode.
	Snapshot bool `json:"snapshot" default:"true"`
}

// Init initializes the Config with desired values.
func (c Config) Init() Config {
	// if the keyProperties is empty,
	// we'll use the orderingProperty as a record key
	if len(c.KeyProperties) == 0 {
		c.KeyProperties = []string{c.OrderingProperty}
	}

	return c
}
