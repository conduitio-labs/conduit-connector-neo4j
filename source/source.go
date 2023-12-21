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

// Package source implements the source logic of the Neo4j connector.
package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-neo4j/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// errNoIterator occurs when the [Combined] has no any underlying iterators.
var errNoIterator = errors.New("no iterator")

// Iterator defines an Iterator interface needed for the [Source].
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (sdk.Record, error)
}

// Source Neo4j Connector reads records from a Neo4j.
type Source struct {
	sdk.UnimplementedSource

	config          Config
	driver          neo4j.DriverWithContext
	snapshot        Iterator
	pollingSnapshot Iterator
}

// New creates a new instance of the [Source].
func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named [sdk.Parameter] that describe how to configure the [Source].
func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

// Configure parses and initializes the [Source] config.
func (s *Source) Configure(_ context.Context, raw map[string]string) error {
	if err := sdk.Util.ParseConfig(raw, &s.config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	// if the keyProperties is empty,
	// we'll use the orderingProperty as a record key
	if len(s.config.KeyProperties) == 0 {
		s.config.KeyProperties = []string{s.config.OrderingProperty}
	}

	return nil
}

// Open makes sure everything is prepared to read records.
func (s *Source) Open(ctx context.Context, sdkPosition sdk.Position) error {
	driver, err := neo4j.NewDriverWithContext(s.config.URI, s.config.Auth.AuthToken())
	if err != nil {
		return fmt.Errorf("create neo4j driver: %w", err)
	}

	if err = driver.VerifyConnectivity(ctx); err != nil {
		return fmt.Errorf("ping neo4j instance: %w", err)
	}

	s.driver = driver

	position, err := iterator.ParsePosition(sdkPosition)
	if err != nil && !errors.Is(err, iterator.ErrNilSDKPosition) {
		return fmt.Errorf("parse position: %w", err)
	}

	s.pollingSnapshot, err = iterator.NewPollingSnapshot(ctx, iterator.SnapshotParams{
		Driver:           driver,
		OrderingProperty: s.config.OrderingProperty,
		KeyProperties:    s.config.KeyProperties,
		EntityType:       s.config.EntityType,
		EntityLabels:     s.config.EntityLabels,
		BatchSize:        s.config.BatchSize,
		DatabaseName:     s.config.Database,
		Position:         position,
	})
	if err != nil {
		return fmt.Errorf("init polling snapshot iterator: %w", err)
	}

	if s.config.Snapshot && (position == nil || position.Mode == iterator.ModeSnapshot) {
		s.snapshot, err = iterator.NewSnapshot(ctx, iterator.SnapshotParams{
			Driver:           driver,
			OrderingProperty: s.config.OrderingProperty,
			KeyProperties:    s.config.KeyProperties,
			EntityType:       s.config.EntityType,
			EntityLabels:     s.config.EntityLabels,
			BatchSize:        s.config.BatchSize,
			DatabaseName:     s.config.Database,
			Position:         position,
		})
		if err != nil {
			return fmt.Errorf("init snapshot iterator: %w", err)
		}
	}

	return nil
}

// Read returns a new [sdk.Record].
// It can return the error [sdk.ErrBackoffRetry] to signal to the SDK
// it should call Read again with a backoff retry.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	switch {
	case s.snapshot != nil:
		record, err := read(ctx, s.snapshot)
		if err != nil {
			if !errors.Is(err, sdk.ErrBackoffRetry) {
				return sdk.Record{}, err
			}

			s.snapshot = nil

			return read(ctx, s.pollingSnapshot)
		}

		return record, nil

	case s.pollingSnapshot != nil:
		return read(ctx, s.pollingSnapshot)

	default:
		return sdk.Record{}, errNoIterator
	}
}

// Ack just logs a provided position.
func (s *Source) Ack(ctx context.Context, sdkPosition sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(sdkPosition)).Msg("got ack")

	return nil
}

// Teardown closes connections, stops iterators and prepares for a graceful shutdown.
func (s *Source) Teardown(ctx context.Context) error {
	if s.driver != nil {
		if err := s.driver.Close(ctx); err != nil {
			return fmt.Errorf("close neo4j driver: %w", err)
		}
	}

	return nil
}

// read is a helper function that accepts an [Iterator] and do a common read logic.
func read(ctx context.Context, iterator Iterator) (sdk.Record, error) {
	hasNext, err := iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get next record: %w", err)
	}

	return record, nil
}
