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

package destination

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	// field names that are used within the integration tests.
	idFieldName   = "id"
	nameFieldName = "name"
	// testURI is a connection URI pointed to a local Neo4j instance.
	testURI = "bolt://localhost:7687"
	// testLabel is a label that is used for integration tests.
	testLabel = "Person"
	// test credentials that are used in a Neo4j Docker container.
	testUsername = "neo4j"
	testPassword = "supersecret"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	cfg := prepareConfig(t, config.EntityTypeNode)

	// create a Destination,
	// configure it with the prepared config, and open it
	destination := New()
	is.NoErr(destination.Configure(ctx, cfg))
	is.NoErr(destination.Open(ctx))
	// teardown the destination
	t.Cleanup(func() {
		is.NoErr(destination.Teardown(ctx))
	})

	// prepare some test payload
	snapshotRecordPayload := map[string]any{idFieldName: "a", nameFieldName: "Bob"}
	createRecordPayload := map[string]any{idFieldName: "b", nameFieldName: "John"}

	// initialize a slice with test records
	// that contains snapshot and create operations
	records := []opencdc.Record{
		{Operation: opencdc.OperationSnapshot, Payload: opencdc.Change{After: opencdc.StructuredData(snapshotRecordPayload)}},
		{Operation: opencdc.OperationCreate, Payload: opencdc.Change{After: opencdc.StructuredData(createRecordPayload)}},
	}

	// write the test records, check if there's no error,
	// and the returned len is equal to the len of the records slice
	n, err := destination.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, len(records))

	driver, err := neo4j.NewDriverWithContext(
		cfg[config.KeyURI], neo4j.BasicAuth(cfg[config.KeyAuthUsername], cfg[config.KeyAuthPassword], ""),
	)
	is.NoErr(err)
	t.Cleanup(func() {
		is.NoErr(driver.Close(ctx))
	})

	// compare the snapshot and create records payload with Neo4j records
	neo4jRecord, err := findRecord(ctx, driver, snapshotRecordPayload[idFieldName])
	is.NoErr(err)
	is.Equal(neo4jRecord, snapshotRecordPayload)

	neo4jRecord, err = findRecord(ctx, driver, createRecordPayload[idFieldName])
	is.NoErr(err)
	is.Equal(neo4jRecord, createRecordPayload)

	// create a record with the update operation
	updateRecordPayload := map[string]any{
		idFieldName:   snapshotRecordPayload[idFieldName],
		nameFieldName: "NewBob",
	}

	updateRecord := opencdc.Record{
		Operation: opencdc.OperationUpdate,
		Key:       opencdc.StructuredData{idFieldName: updateRecordPayload[idFieldName]},
		Payload:   opencdc.Change{After: opencdc.StructuredData(updateRecordPayload)},
	}

	// write the update record
	n, err = destination.Write(ctx, []opencdc.Record{updateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// compare the update record with a Neo4j record
	neo4jRecord, err = findRecord(ctx, driver, updateRecordPayload[idFieldName])
	is.NoErr(err)
	is.Equal(neo4jRecord, updateRecordPayload)

	// create a record with the delete operation
	deleteRecord := opencdc.Record{
		Operation: opencdc.OperationDelete,
		Key:       opencdc.StructuredData{idFieldName: updateRecordPayload[idFieldName]},
	}

	// write the delete record
	n, err = destination.Write(ctx, []opencdc.Record{deleteRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// check that the record has been deleted
	_, err = findRecord(ctx, driver, updateRecordPayload[idFieldName])
	var usageError *neo4j.UsageError
	is.True(errors.As(err, &usageError))
	// this message is from the neo4j driver
	is.Equal(usageError.Message, "Result contains no more records")
}

// prepareConfig creates a config with the test values and the provided entityType.
func prepareConfig(t *testing.T, entityType config.EntityType) map[string]string {
	t.Helper()

	return map[string]string{
		config.KeyURI:          testURI,
		config.KeyEntityType:   string(entityType),
		config.KeyEntityLabels: testLabel,
		config.KeyAuthUsername: testUsername,
		config.KeyAuthPassword: testPassword,
	}
}

// findRecord finds a record in Neo4j database by the provided id.
func findRecord(ctx context.Context, driver neo4j.DriverWithContext, id any) (map[string]any, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	record, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (map[string]any, error) {
		query := fmt.Sprintf("MATCH (p:%s {%s: $%s}) RETURN p.%s AS %s, p.%s AS %s",
			testLabel, idFieldName, idFieldName, idFieldName, idFieldName, nameFieldName, nameFieldName,
		)

		result, err := tx.Run(ctx, query, map[string]any{idFieldName: id})
		if err != nil {
			return nil, fmt.Errorf("run transaction: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("collect record: %w", err)
		}

		// we're sure that if the record exists,
		// it has the following fields, so we skip the checks
		id, _ := record.Get(idFieldName)
		name, _ := record.Get(nameFieldName)

		return map[string]any{
			idFieldName:   id,
			nameFieldName: name,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("execute read: %w", err)
	}

	return record, nil
}
