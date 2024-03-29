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

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	// some Cypher queries that are used within the integration tests.
	testCreateNodeQueryTemplate = "CREATE (obj:%s {id: $id, name: $name}) RETURN obj.id as id, obj.name as name"
	// testURI is a connection URI pointed to a local Neo4j instance.
	testURI = "bolt://localhost:7687"
	// testLabelPrefix is a label prefix
	// that is used for integration tests to construct label names.
	testLabelPrefix      = "test_label"
	testOrderingProperty = "id"
	testDatabase         = "neo4j"
	testBatchSize        = "1000"
	testSnapshot         = "true"
	// test credentials that are used in a Neo4j Docker container.
	testUsername = "neo4j"
	testPassword = "supersecret"
)

// testAuthToken is a [neo4j.AuthToken] that is used within the integration tests for authentication.
var testAuthToken = neo4j.BasicAuth(testUsername, testPassword, "")

func TestSource_Read_successSnapshotNode(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	sourceConfig := prepareConfig(t, config.EntityTypeNode)

	source := New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := source.Configure(ctx, sourceConfig)
	is.NoErr(err)

	testNode := createTestElement(ctx, t, 1, sourceConfig)

	rawTestNode, err := json.Marshal(testNode)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	record, err := source.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationSnapshot)
	is.Equal(record.Payload.After, sdk.RawData(rawTestNode))
}

func TestSource_Read_successResumeSnapshotNode(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	sourceConfig := prepareConfig(t, config.EntityTypeNode)

	source := New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := source.Configure(ctx, sourceConfig)
	is.NoErr(err)

	firstTestNode := createTestElement(ctx, t, 1, sourceConfig)
	secondTestNode := createTestElement(ctx, t, 2, sourceConfig)

	rawFirstTestNode, err := json.Marshal(firstTestNode)
	is.NoErr(err)

	rawSecondTestNode, err := json.Marshal(secondTestNode)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	firstRecord, err := source.Read(ctx)
	is.NoErr(err)
	is.Equal(firstRecord.Operation, sdk.OperationSnapshot)
	is.Equal(firstRecord.Payload.After, sdk.RawData(rawFirstTestNode))

	is.NoErr(source.Teardown(ctx))

	is.NoErr(source.Open(ctx, firstRecord.Position))

	secondRecord, err := source.Read(ctx)
	is.NoErr(err)
	is.Equal(secondRecord.Operation, sdk.OperationSnapshot)
	is.Equal(secondRecord.Payload.After, sdk.RawData(rawSecondTestNode))

	_, err = source.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)
}

func TestSource_Read_successSnapshotPollingNode(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	sourceConfig := prepareConfig(t, config.EntityTypeNode)

	source := New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := source.Configure(ctx, sourceConfig)
	is.NoErr(err)

	testNode := createTestElement(ctx, t, 1, sourceConfig)
	rawTestNode, err := json.Marshal(testNode)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	record, err := source.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationSnapshot)
	is.Equal(record.Payload.After, sdk.RawData(rawTestNode))

	testNode = createTestElement(ctx, t, 2, sourceConfig)
	rawTestNode, err = json.Marshal(testNode)
	is.NoErr(err)

	record, err = source.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Payload.After, sdk.RawData(rawTestNode))
}

// prepareConfig prepares a config with the required fields.
func prepareConfig(t *testing.T, entityType config.EntityType) map[string]string {
	t.Helper()

	return map[string]string{
		config.KeyURI:             testURI,
		config.KeyEntityType:      string(entityType),
		config.KeyEntityLabels:    fmt.Sprintf("%s_%d", testLabelPrefix, time.Now().UnixNano()),
		config.KeyDatabase:        testDatabase,
		config.KeyAuthUsername:    testUsername,
		config.KeyAuthPassword:    testPassword,
		ConfigKeyOrderingProperty: testOrderingProperty,
		ConfigKeyBatchSize:        testBatchSize,
		ConfigKeySnapshot:         testSnapshot,
	}
}

// createTestElement creates a test element in Neo4j.
func createTestElement(ctx context.Context, t *testing.T, id float64, cfg map[string]string) map[string]any {
	t.Helper()

	is := is.New(t)

	neo4jDriver, err := neo4j.NewDriverWithContext(cfg[config.KeyURI], testAuthToken)
	is.NoErr(err)
	t.Cleanup(func() {
		is.NoErr(neo4jDriver.Close(context.Background()))
	})

	session := neo4jDriver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: cfg[config.KeyDatabase],
	})

	output, err := neo4j.ExecuteWrite(ctx, session, func(tx neo4j.ManagedTransaction) (map[string]any, error) {
		cypherQuery := fmt.Sprintf(testCreateNodeQueryTemplate, cfg[config.KeyEntityLabels])
		result, txErr := tx.Run(ctx, cypherQuery, map[string]any{"id": id, "name": gofakeit.Name()})
		if txErr != nil {
			return nil, fmt.Errorf("run tx: %w", txErr)
		}

		record, txErr := result.Single(ctx)
		if txErr != nil {
			return nil, fmt.Errorf("get single record: %w", txErr)
		}

		output := make(map[string]any, len(record.Keys))
		for _, key := range record.Keys {
			// skip the check because we know that the key exists
			// as we iterate over all existing keys
			output[key], _ = record.Get(key)
		}

		return output, nil
	})
	is.NoErr(err)
	is.NoErr(session.Close(ctx))

	return output
}
