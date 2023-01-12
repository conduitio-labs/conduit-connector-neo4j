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

// Package destination implements the destination logic of the Neo4j connector.
package destination

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Writer is a writer interface needed for the [Destination].
type Writer interface {
	Write(ctx context.Context, record sdk.Record) error
}

// Destination Neo4j Connector persists records to a Neo4j.
type Destination struct {
	sdk.UnimplementedDestination

	config Config
	writer Writer
	driver neo4j.DriverWithContext
}

// NewDestination creates a new instance of the [Destination].
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named [sdk.Parameter] that describe how to configure the [Destination].
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

// Configure parses and initializes the [Destination] config.
func (d *Destination) Configure(ctx context.Context, raw map[string]string) error {
	if err := sdk.Util.ParseConfig(raw, &d.config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	driver, err := neo4j.NewDriverWithContext(d.config.URI, d.config.Auth.AuthToken())
	if err != nil {
		return fmt.Errorf("create neo4j driver: %w", err)
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		return fmt.Errorf("ping neo4j instance: %w", err)
	}

	d.driver = driver

	return nil
}

// Write writes a record into a [Destination].
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		if err := d.writer.Write(ctx, record); err != nil {
			return i, fmt.Errorf("write record: %w", err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.driver != nil {
		if err := d.driver.Close(ctx); err != nil {
			return fmt.Errorf("close neo4j driver: %w", err)
		}
	}

	return nil
}
