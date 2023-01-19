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

// Package writer implements a writer logic for the Neo4j Destination.
package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	// all Cypher queries used by the [Writer] are listed below in the format of Go fmt.
	createNodeQueryTemplate = "CREATE (node:%s {%s})"
	updateNodeQueryTemplate = "MATCH (node:%s {%s}) SET %s"
	deleteNodeQueryTemplate = "MATCH (node:%s {%s}) DELETE node"

	// some helper symbols for Cypher queries.
	setKeyPrefix      = "node."
	setAssignSign     = "="
	matchAssignSign   = ":"
	interpolationSign = "$"
)

// ErrEmptyRawData occurs when trying to structurize empty [sdk.RawData].
var ErrEmptyRawData = errors.New("raw data is empty")

// Writer implements a writer logic for the Neo4j Destination.
type Writer struct {
	driver       neo4j.DriverWithContext
	databaseName string
	entityType   config.EntityType
	entityLabels string
}

// Params holds incoming params for the [Writer].
type Params struct {
	Driver       neo4j.DriverWithContext
	DatabaseName string
	EntityType   config.EntityType
	EntityLabels []string
}

// New creates a new instance of the [Writer].
func New(params Params) *Writer {
	return &Writer{
		driver:       params.Driver,
		databaseName: params.DatabaseName,
		entityType:   params.EntityType,
		// join entity labels here to not do this each time constructing queries
		entityLabels: strings.Join(params.EntityLabels, ":"),
	}
}

// Write writes a record to the destination.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	err := sdk.Util.Destination.Route(ctx, record,
		w.handleCreate,
		w.handleUpdate,
		w.handleDelete,
		w.handleCreate,
	)
	if err != nil {
		return fmt.Errorf("route record: %w", err)
	}

	return nil
}

func (w *Writer) handleCreate(ctx context.Context, record sdk.Record) error {
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: w.databaseName,
	})
	defer session.Close(ctx)

	properties, err := w.structurizeRawData(record.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record payload: %w", err)
	}

	// construct a CREATE query
	cypherMatchProperties, err := w.cypherMatchProperties(properties)
	if err != nil {
		return fmt.Errorf("create cypher match properties: %w", err)
	}

	query := fmt.Sprintf(createNodeQueryTemplate, w.entityLabels, cypherMatchProperties)

	// execute the CREATE query
	if err := w.executeWriteQuery(ctx, session, query, properties); err != nil {
		return fmt.Errorf("execute write query: %w", err)
	}

	return nil
}

func (w *Writer) handleUpdate(ctx context.Context, record sdk.Record) error {
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: w.databaseName,
	})
	defer session.Close(ctx)

	key, err := w.structurizeRawData(record.Key.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record key: %w", err)
	}

	properties, err := w.structurizeRawData(record.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record payload: %w", err)
	}

	// add keys to the properties map because we need them
	// for interpolation within the executeWriteQuery method
	// and to avoid creating a third map
	for keyName, keyValue := range key {
		if _, ok := properties[keyName]; !ok {
			properties[keyName] = keyValue
		}
	}

	// construct a MATCH SET query
	cypherMatchProperties, err := w.cypherMatchProperties(key)
	if err != nil {
		return fmt.Errorf("create cypher match properties: %w", err)
	}

	cypherSetProperties, err := w.cypherSetProperties(properties, key)
	if err != nil {
		return fmt.Errorf("create cypher set properties: %w", err)
	}

	query := fmt.Sprintf(updateNodeQueryTemplate, w.entityLabels, cypherMatchProperties, cypherSetProperties)

	// execute the MATCH SET query
	if err := w.executeWriteQuery(ctx, session, query, properties); err != nil {
		return fmt.Errorf("execute write query: %w", err)
	}

	return nil
}

func (w *Writer) handleDelete(ctx context.Context, record sdk.Record) error {
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: w.databaseName,
	})
	defer session.Close(ctx)

	key, err := w.structurizeRawData(record.Key.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record key: %w", err)
	}

	// construct a MATCH DELETE query
	cypherMatchProperties, err := w.cypherMatchProperties(key)
	if err != nil {
		return fmt.Errorf("create cypher match properties: %w", err)
	}

	query := fmt.Sprintf(deleteNodeQueryTemplate, w.entityLabels, cypherMatchProperties)

	// execute the MATCH DELETE query
	if err := w.executeWriteQuery(ctx, session, query, key); err != nil {
		return fmt.Errorf("execute write query: %w", err)
	}

	return nil
}

// structurizeRawData tries to unmarshal the [sdk.RawData]
// and if the process fails or the [sdk.RawData] is empty the method returns an error.
func (w *Writer) structurizeRawData(rawData sdk.RawData) (map[string]any, error) {
	if rawData == nil || len(rawData.Bytes()) == 0 {
		return nil, ErrEmptyRawData
	}

	var structurizedData map[string]any
	if err := json.Unmarshal(rawData, &structurizedData); err != nil {
		return nil, fmt.Errorf("unmarshal raw data: %w", err)
	}

	return structurizedData, nil
}

// executeWriteQuery is a helper method that wraps the [neo4j.ExecuteWrite] function
// and the underlying anonymous function.
func (w *Writer) executeWriteQuery(
	ctx context.Context,
	session neo4j.SessionWithContext,
	query string,
	properties map[string]any,
) error {
	_, err := neo4j.ExecuteWrite(ctx, session, func(tx neo4j.ManagedTransaction) (neo4j.ResultSummary, error) {
		result, txErr := tx.Run(ctx, query, properties)
		if txErr != nil {
			return nil, fmt.Errorf("run tx: %w", txErr)
		}

		summary, err := result.Consume(ctx)
		if err != nil {
			return nil, fmt.Errorf("consume result: %w", err)
		}

		return summary, nil
	})
	if err != nil {
		return fmt.Errorf("execute write: %w", err)
	}

	return nil
}

// cypherMatchProperties constructs a set of properties
// according to the Cypher MATCH syntax, e.g.: "{prop: $prop}".
func (w *Writer) cypherMatchProperties(properties map[string]any) (string, error) {
	var sb strings.Builder
	for propertyName := range properties {
		_, err := sb.WriteString(
			propertyName + matchAssignSign + interpolationSign + propertyName + ", ",
		)
		if err != nil {
			return "", fmt.Errorf("write string: %w", err)
		}
	}

	return strings.TrimRight(sb.String(), ", "), nil
}

// cypherSetProperties constructs a set of properties
// according to the Cypher SET syntax, e.g.: "prefix.prop = $prop".
func (w *Writer) cypherSetProperties(properties map[string]any, key map[string]any) (string, error) {
	var sb strings.Builder
	for propertyName := range properties {
		if _, ok := key[propertyName]; ok {
			continue
		}

		_, err := sb.WriteString(
			setKeyPrefix + propertyName + setAssignSign + interpolationSign + propertyName + ", ",
		)
		if err != nil {
			return "", fmt.Errorf("write string: %w", err)
		}
	}

	return strings.TrimRight(sb.String(), ", "), nil
}
