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

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	"github.com/conduitio-labs/conduit-connector-neo4j/schema"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

const (
	// all Cypher queries used by the [Snapshot] are listed below in the format of Go fmt.
	getNodeMaxPropertyQueryTemplate         = "MATCH (obj:%s) RETURN obj.%s as %s ORDER BY obj.%s DESC LIMIT 1"
	getRelationshipMaxPropertyQueryTemplate = "MATCH ()-[obj:%s]-() RETURN obj.%s as %s ORDER BY obj.%s DESC LIMIT 1"
	getNodesQueryTemplate                   = "MATCH (obj:%s) WHERE %s RETURN obj LIMIT %d"
	getRelationshipsQueryTemplate           = "MATCH (src)-[obj:%s]->(trgt) WHERE %s RETURN obj, src, trgt LIMIT %d"
	opmvLTEWhereClause                      = "obj.%s <= $opmv"
	opvGTWhereClause                        = "obj.%s > $opv"

	// some helpers for Cypher queries.
	orderingPropertyMaxValueFieldName = "opmv"
	orderingPropertyValueFieldName    = "opv"
	objPlaceholder                    = "obj"
	srcPlaceholder                    = "src"
	trgtPlaceholder                   = "trgt"

	// relationship payload-specific fields.
	sourceNodeField = "sourceNode"
	targetNodeField = "targetNode"

	// metadataEntityLabelsField is a name of a metadata field that holds entity labels.
	metadataEntityLabelsField = "neo4j.entityLabels"
)

// Snapshot implements a snapshot logic for the connector.
type Snapshot struct {
	driver                   neo4j.DriverWithContext
	orderingProperty         string
	keyProperties            []string
	orderingPropertyMaxValue any
	entityType               config.EntityType
	entityLabels             string
	batchSize                int
	databaseName             string
	position                 *Position
	// records stores fetched and parsed Neo4j records,
	// this channel works as a queue from which the Next method takes records.
	records chan map[string]any
	// polling defines if the snapshot is used to detect insertions
	// by polling for new documents.
	polling bool
}

// SnapshotParams is incoming params for the [NewSnapshot] function.
type SnapshotParams struct {
	Driver           neo4j.DriverWithContext
	OrderingProperty string
	KeyProperties    []string
	EntityType       config.EntityType
	EntityLabels     []string
	BatchSize        int
	DatabaseName     string
	Position         *Position
}

// NewSnapshot creates a new instance of the [Snapshot].
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	var (
		orderingPropertyMaxValue any
		// join entity labels here to not do this for each individual element
		entityLabels = strings.Join(params.EntityLabels, ":")
	)

	switch position := params.Position; {
	case position != nil && position.MaxElement != nil:
		orderingPropertyMaxValue = position.MaxElement

	default:
		var err error
		orderingPropertyMaxValue, err = getMaxPropertyValue(
			ctx, params.Driver,
			params.DatabaseName, entityLabels, params.OrderingProperty,
			params.EntityType,
		)
		if err != nil && !errors.Is(err, errNoElements) {
			return nil, fmt.Errorf("get ordering property max value: %w", err)
		}
	}

	return &Snapshot{
		driver:                   params.Driver,
		keyProperties:            params.KeyProperties,
		orderingProperty:         params.OrderingProperty,
		orderingPropertyMaxValue: orderingPropertyMaxValue,
		entityType:               params.EntityType,
		entityLabels:             entityLabels,
		batchSize:                params.BatchSize,
		databaseName:             params.DatabaseName,
		position:                 params.Position,
		records:                  make(chan map[string]any, params.BatchSize),
	}, nil
}

// NewPollingSnapshot creates a new instance of the [Snapshot] iterator prepared for polling.
func NewPollingSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	// join entity labels here to not do this for each individual element
	entityLabels := strings.Join(params.EntityLabels, ":")

	if params.Position == nil {
		orderingPropertyMaxValue, err := getMaxPropertyValue(ctx, params.Driver,
			params.DatabaseName, entityLabels, params.OrderingProperty,
			params.EntityType)
		if err != nil && !errors.Is(err, errNoElements) {
			return nil, fmt.Errorf("get ordering property max value: %w", err)
		}

		params.Position = &Position{
			Mode:               ModeSnapshotPolling,
			LastProcessedValue: orderingPropertyMaxValue,
		}
	}

	return &Snapshot{
		driver:           params.Driver,
		keyProperties:    params.KeyProperties,
		orderingProperty: params.OrderingProperty,
		entityType:       params.EntityType,
		entityLabels:     entityLabels,
		batchSize:        params.BatchSize,
		databaseName:     params.DatabaseName,
		position:         params.Position,
		records:          make(chan map[string]any, params.BatchSize),
		polling:          true,
	}, nil
}

// HasNext checks whether the snapshot iterator has records to return or not.
func (s *Snapshot) HasNext(ctx context.Context) (bool, error) {
	if len(s.records) > 0 {
		return true, nil
	}

	if err := s.loadBatch(ctx); err != nil {
		return false, fmt.Errorf("load batch: %w", err)
	}

	return len(s.records) > 0, nil
}

// Next returns the next available record.
func (s *Snapshot) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err() //nolint:wrapcheck // there's no much to wrap here

	case record := <-s.records:
		// if the snapshot is polling new items,
		// we mark its position as CDC to identify it during pauses correctly
		mode := ModeSnapshot
		if s.polling {
			mode = ModeSnapshotPolling
		}

		// construct the position
		position := &Position{
			Mode:               mode,
			LastProcessedValue: record[s.orderingProperty],
			MaxElement:         s.orderingPropertyMaxValue,
		}

		sdkPosition, err := position.MarshalSDKPosition()
		if err != nil {
			return sdk.Record{}, fmt.Errorf("marshal sdk position: %w", err)
		}

		s.position = position

		// construct the key
		key := make(sdk.StructuredData)
		for _, keyProperty := range s.keyProperties {
			keyPropertyValue, ok := record[keyProperty]
			if !ok {
				return sdk.Record{}, fmt.Errorf("payload doesn't contain %q property", keyProperty)
			}

			key[keyProperty] = keyPropertyValue
		}

		// construct the metadata
		metadata := sdk.Metadata{metadataEntityLabelsField: s.entityLabels}
		metadata.SetCreatedAt(time.Now())

		if s.polling {
			return sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, sdk.StructuredData(record)), nil
		}

		return sdk.Util.Source.NewRecordSnapshot(sdkPosition, metadata, key, sdk.StructuredData(record)), nil
	}
}

// loadBatch finds a batch of elements in a Neo4j database,
// based on labels and ordering property.
func (s *Snapshot) loadBatch(ctx context.Context) error {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: s.databaseName,
	})
	defer session.Close(ctx)

	var (
		whereClause string
		params      = make(map[string]any)
	)

	// if the ordering property max value isn't nil,
	// we'll use it to get elements with ordering property less than or equal to the max value
	if s.orderingPropertyMaxValue != nil {
		whereClause += fmt.Sprintf(opmvLTEWhereClause, s.orderingProperty)
		params[orderingPropertyMaxValueFieldName] = s.orderingPropertyMaxValue
	}

	if s.orderingPropertyMaxValue != nil && s.position != nil && s.position.LastProcessedValue != nil {
		whereClause += " AND "
	}

	// if the position and its last processed value are not nil,
	// we'll use the value to construct the where clause so we only get elements
	// that have ordering field greater than the position's last processed value
	if s.position != nil && s.position.LastProcessedValue != nil {
		whereClause += fmt.Sprintf(opvGTWhereClause, s.orderingProperty)
		params[orderingPropertyValueFieldName] = s.position.LastProcessedValue
	}

	getQueryTemplate := getNodesQueryTemplate
	if s.entityType == config.EntityTypeRelationship {
		getQueryTemplate = getRelationshipsQueryTemplate
	}

	query := fmt.Sprintf(getQueryTemplate, s.entityLabels, whereClause, s.batchSize)

	_, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (neo4j.ResultWithContext, error) {
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("run tx: %w", err)
		}

		// collect and send records here,
		// because once the function exits the result won't contain any records
		if err = s.processNeo4jResult(ctx, result); err != nil {
			return nil, fmt.Errorf("process neo4j result: %w", err)
		}

		return result, nil
	})
	if err != nil {
		return fmt.Errorf("execute read: %w", err)
	}

	return nil
}

// processNeo4jResult parses the result records and sends them to the records channel.
func (s *Snapshot) processNeo4jResult(ctx context.Context, result neo4j.ResultWithContext) error {
	var record *db.Record
	for result.NextRecord(ctx, &record) {
		elementRaw, ok := record.Get(objPlaceholder)
		if !ok {
			return fmt.Errorf("record doesn't contain %q key", objPlaceholder)
		}

		var props map[string]any
		switch element := elementRaw.(type) {
		case dbtype.Node:
			props = element.Props
		case dbtype.Relationship:
			props = element.Props

			srcNodeRaw, ok := record.Get(srcPlaceholder)
			if !ok {
				return fmt.Errorf("record doesn't contain %q key", srcPlaceholder)
			}

			srcNode, ok := srcNodeRaw.(dbtype.Node)
			if !ok {
				return errConvertRawNode
			}

			trgtNodeRaw, ok := record.Get(trgtPlaceholder)
			if !ok {
				return fmt.Errorf("record doesn't contain %q key", trgtPlaceholder)
			}

			trgtNode, ok := trgtNodeRaw.(dbtype.Node)
			if !ok {
				return errConvertRawRelationship
			}

			props[sourceNodeField] = schema.Node{Labels: srcNode.Labels, Key: srcNode.Props}
			props[targetNodeField] = schema.Node{Labels: trgtNode.Labels, Key: trgtNode.Props}
		}

		s.records <- props
	}

	return nil
}

// getMaxPropertyValue returns the maximum property value that can be found among Neo4j entities.
func getMaxPropertyValue(
	ctx context.Context,
	driver neo4j.DriverWithContext,
	database, labels, property string,
	entityType config.EntityType,
) (any, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: database,
	})
	defer session.Close(ctx)

	maxPropertyQueryTemplate := getNodeMaxPropertyQueryTemplate
	if entityType == config.EntityTypeRelationship {
		maxPropertyQueryTemplate = getRelationshipMaxPropertyQueryTemplate
	}

	query := fmt.Sprintf(maxPropertyQueryTemplate, labels, property, property, property)

	propertyValue, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("run tx: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			var usageError *neo4j.UsageError
			if errors.As(err, &usageError) && usageError.Message == neo4jNoMoreRecordsErrorMessage {
				return nil, errNoElements
			}

			return nil, fmt.Errorf("extract single from result: %w", err)
		}

		propertyValue, ok := record.Get(property)
		if !ok {
			return nil, fmt.Errorf("record doesn't contain %q property", property)
		}

		return propertyValue, nil
	})
	if err != nil {
		return nil, fmt.Errorf("execute read: %w", err)
	}

	return propertyValue, nil
}
