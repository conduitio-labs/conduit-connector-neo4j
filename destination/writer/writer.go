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
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	"github.com/conduitio-labs/conduit-connector-neo4j/schema"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/mitchellh/mapstructure"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	// all Cypher queries used by the [Writer] are listed below in the format of Go fmt.
	createNodeQueryTemplate         = "CREATE (obj:%s {%s})"
	updateNodeQueryTemplate         = "MATCH (obj:%s {%s}) SET %s"
	deleteNodeQueryTemplate         = "MATCH (obj:%s {%s}) DELETE obj"
	createRelationshipQueryTemplate = "MATCH (src:%s {%s}) MATCH (trgt:%s {%s}) CREATE (src)-[obj:%s {%s}]->(trgt)"
	updateRelationshipQueryTemplate = "MATCH ()-[obj:%s {%s}]->() SET %s"
	deleteRelationshipQueryTemplate = "MATCH ()-[obj:%s {%s}]->() DELETE obj"

	// some helper symbols for Cypher queries.
	setKeyPrefix              = "obj."
	setAssignSign             = "="
	matchAssignSign           = ":"
	interpolationSign         = "$"
	interpolationSourcePrefix = "src_"
	interpolationTargetPrefix = "trgt_"

	// relationship payload-specific fields.
	sourceNodeField = "sourceNode"
	targetNodeField = "targetNode"
)

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
func (w *Writer) Write(ctx context.Context, record opencdc.Record) error {
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

func (w *Writer) handleCreate(ctx context.Context, record opencdc.Record) error {
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: w.databaseName,
	})
	defer session.Close(ctx)

	switch w.entityType {
	case config.EntityTypeNode:
		return w.createNode(ctx, session, record)

	case config.EntityTypeRelationship:
		return w.createRelationship(ctx, session, record)

	default:
		// this shouldn't happen as we validate the config this value comes from
		return ErrUnsupportedEntityType
	}
}

func (w *Writer) handleUpdate(ctx context.Context, record opencdc.Record) error {
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

	// delete reserved sourceNode and targetNode fields
	// from the properties map, as we don't need them for updates
	delete(properties, sourceNodeField)
	delete(properties, targetNodeField)

	// add keys to the properties map because we need them
	// for interpolation within the executeWriteQuery method
	// and to avoid creating a third map
	for name, value := range key {
		if _, ok := properties[name]; !ok {
			properties[name] = value
		}
	}

	// construct a MATCH SET query
	cypherMatchProperties, err := w.cypherMatchProperties(key, "")
	if err != nil {
		return fmt.Errorf("create cypher match properties: %w", err)
	}

	cypherSetProperties, err := w.cypherSetProperties(properties, key)
	if err != nil {
		return fmt.Errorf("create cypher set properties: %w", err)
	}

	updateQueryTemplate := updateNodeQueryTemplate
	if w.entityType == config.EntityTypeRelationship {
		updateQueryTemplate = updateRelationshipQueryTemplate
	}

	query := fmt.Sprintf(updateQueryTemplate, w.entityLabels, cypherMatchProperties, cypherSetProperties)

	// execute the MATCH SET query
	if err := w.executeWriteQuery(ctx, session, query, properties); err != nil {
		return fmt.Errorf("execute write query: %w", err)
	}

	return nil
}

func (w *Writer) handleDelete(ctx context.Context, record opencdc.Record) error {
	session := w.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: w.databaseName,
	})
	defer session.Close(ctx)

	key, err := w.structurizeRawData(record.Key.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record key: %w", err)
	}

	// construct a MATCH DELETE query
	cypherMatchProperties, err := w.cypherMatchProperties(key, "")
	if err != nil {
		return fmt.Errorf("create cypher match properties: %w", err)
	}

	deleteQueryTemplate := deleteNodeQueryTemplate
	if w.entityType == config.EntityTypeRelationship {
		deleteQueryTemplate = deleteRelationshipQueryTemplate
	}

	query := fmt.Sprintf(deleteQueryTemplate, w.entityLabels, cypherMatchProperties)

	// execute the MATCH DELETE query
	if err := w.executeWriteQuery(ctx, session, query, key); err != nil {
		return fmt.Errorf("execute write query: %w", err)
	}

	return nil
}

func (w *Writer) createNode(ctx context.Context, session neo4j.SessionWithContext, record opencdc.Record) error {
	properties, err := w.structurizeRawData(record.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record payload: %w", err)
	}

	// construct a CREATE query
	cypherMatchProperties, err := w.cypherMatchProperties(properties, "")
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

//nolint:funlen // maybe refactor at some point
func (w *Writer) createRelationship(
	ctx context.Context,
	session neo4j.SessionWithContext,
	record opencdc.Record,
) error {
	properties, err := w.structurizeRawData(record.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("structurize record payload: %w", err)
	}

	// extract source and target nodes from the properties
	sourceNode, targetNode, err := w.sourceTargetNodesFromProperties(properties)
	if err != nil {
		return fmt.Errorf("extract source and target node from properties: %w", err)
	}

	// prepare source node
	sourceNodeLabels := strings.Join(sourceNode.Labels, ":")
	sourceNodeCypherMatchProperties, err := w.cypherMatchProperties(sourceNode.Key, interpolationSourcePrefix)
	if err != nil {
		return fmt.Errorf("create cypher match properties for source node: %w", err)
	}

	// prepare target node
	targetNodeLabels := strings.Join(targetNode.Labels, ":")
	targetNodeCypherMatchProperties, err := w.cypherMatchProperties(targetNode.Key, interpolationTargetPrefix)
	if err != nil {
		return fmt.Errorf("create cypher match properties for target node: %w", err)
	}

	// construct a CREATE query
	relationshipCypherMatchProperties, err := w.cypherMatchProperties(properties, "")
	if err != nil {
		return fmt.Errorf("create cypher match properties for relationship: %w", err)
	}

	query := fmt.Sprintf(createRelationshipQueryTemplate,
		sourceNodeLabels, sourceNodeCypherMatchProperties,
		targetNodeLabels, targetNodeCypherMatchProperties,
		w.entityLabels, relationshipCypherMatchProperties,
	)

	// add sourceNode and targetNode keys to the properties map because we need them
	// for interpolation within the executeWriteQuery method
	// and to avoid creating a third map
	for name, value := range sourceNode.Key {
		interpolatedName := interpolationSourcePrefix + name
		if _, ok := properties[interpolatedName]; !ok {
			properties[interpolatedName] = value
		}
	}

	for name, value := range targetNode.Key {
		interpolatedName := interpolationTargetPrefix + name
		if _, ok := properties[interpolatedName]; !ok {
			properties[interpolatedName] = value
		}
	}

	// execute the CREATE query
	if err := w.executeWriteQuery(ctx, session, query, properties); err != nil {
		return fmt.Errorf("execute write query: %w", err)
	}

	return nil
}

// sourceTargetNodesFromProperties extracts source and target nodes of type [schema.Node] from the properties map.
//
// The method also removes sourceNode and targetNode fields from the properties after extracting
// because we don't need them to be stored as relationship properties.
func (w *Writer) sourceTargetNodesFromProperties(properties map[string]any) (*schema.Node, *schema.Node, error) {
	// extract and parse sourceNode field
	sourceNodeRaw, ok := properties[sourceNodeField]
	if !ok {
		return nil, nil, ErrEmptySourceNode
	}

	sourceNode := new(schema.Node)
	if err := mapstructure.Decode(sourceNodeRaw, sourceNode); err != nil {
		return nil, nil, fmt.Errorf("decode source node: %w", err)
	}

	delete(properties, sourceNodeField)

	// extract and parse targetNode field
	targetNodeRaw, ok := properties[targetNodeField]
	if !ok {
		return nil, nil, ErrEmptyTargetNode
	}

	targetNode := new(schema.Node)
	if err := mapstructure.Decode(targetNodeRaw, targetNode); err != nil {
		return nil, nil, fmt.Errorf("decode target node: %w", err)
	}

	delete(properties, targetNodeField)

	return sourceNode, targetNode, nil
}

// structurizeRawData tries to unmarshal the [opencdc.RawData]
// and if the process fails or the [opencdc.RawData] is empty the method returns an error.
func (w *Writer) structurizeRawData(rawData opencdc.RawData) (map[string]any, error) {
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
		result, err := tx.Run(ctx, query, properties)
		if err != nil {
			return nil, fmt.Errorf("run tx: %w", err)
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
func (w *Writer) cypherMatchProperties(properties map[string]any, interpolationPrefix string) (string, error) {
	var sb strings.Builder
	for propertyName := range properties {
		_, err := sb.WriteString(
			propertyName + matchAssignSign + interpolationSign + interpolationPrefix + propertyName + ", ",
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
