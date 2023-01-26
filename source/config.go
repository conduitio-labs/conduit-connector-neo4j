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

package source

import "github.com/conduitio-labs/conduit-connector-neo4j/config"

const (
	// ConfigKeyBatchSize is a config name for a batch size.
	ConfigKeyBatchSize = "batchSize"
	// ConfigKeySnapshot is a config name for a snapshot field.
	ConfigKeySnapshot = "snapshot"
	// ConfigKeyOrderingProperty is a config name for a orderingProperty field.
	ConfigKeyOrderingProperty = "orderingProperty"
)

// Config holds configurable values specific to source.
type Config struct {
	config.Config

	// The name of a property that is used for ordering
	// nodes or relationships when capturing a snapshot.
	OrderingProperty string `json:"orderingProperty" validate:"required"`
	// The size of an element batch.
	BatchSize int `json:"batchSize" validate:"gt=0,lt=100001" default:"1000"`
	// Determines whether or not the connector will take a snapshot
	// of all nodes or relationships before starting CDC mode.
	Snapshot bool `json:"snapshot" default:"true"`
}
