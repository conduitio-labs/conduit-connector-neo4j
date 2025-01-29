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

package neo4j

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	"github.com/conduitio-labs/conduit-connector-neo4j/destination"
	"github.com/conduitio-labs/conduit-connector-neo4j/source"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
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

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	idCounter int64
}

// GenerateRecord overrides the [sdk.ConfigurableAcceptanceTestDriver] GenerateRecord method.
func (d *driver) GenerateRecord(t *testing.T, operation opencdc.Operation) opencdc.Record {
	t.Helper()

	atomic.AddInt64(&d.idCounter, 1)

	return opencdc.Record{
		Operation: operation,
		Key:       opencdc.StructuredData{"id": float64(d.idCounter)},
		Payload: opencdc.Change{
			After: opencdc.RawData(fmt.Sprintf(`{"id":%v,"name":"%s"}`, float64(d.idCounter), gofakeit.Name())),
		},
	}
}

func TestAcceptance(t *testing.T) {
	srcCfg := map[string]string{
		source.ConfigUri:              testURI,
		source.ConfigEntityType:       string(config.EntityTypeNode),
		source.ConfigDatabase:         testDatabase,
		source.ConfigAuthUsername:     testUsername,
		source.ConfigAuthPassword:     testPassword,
		source.ConfigOrderingProperty: testOrderingProperty,
		source.ConfigBatchSize:        testBatchSize,
		source.ConfigSnapshot:         testSnapshot,
	}

	destCfg := map[string]string{
		destination.ConfigUri:          testURI,
		destination.ConfigEntityType:   string(config.EntityTypeNode),
		destination.ConfigDatabase:     testDatabase,
		destination.ConfigAuthUsername: testUsername,
		destination.ConfigAuthPassword: testPassword,
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      srcCfg,
				DestinationConfig: destCfg,
				BeforeTest:        beforeTest(srcCfg, destCfg),
				Skip:              []string{`.*_Configure_RequiredParams`},
			},
		},
	})
}

// beforeTest set the config labels field to a unique name prefixed with the testLabelPrefix.
func beforeTest(srcCfg map[string]string, destCfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		entityLabels := fmt.Sprintf("%s_%d", testLabelPrefix, time.Now().UnixNano())

		srcCfg[source.ConfigEntityLabels] = entityLabels
		destCfg[destination.ConfigEntityLabels] = entityLabels
	}
}
