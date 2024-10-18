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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-neo4j/config"
	"github.com/conduitio-labs/conduit-connector-neo4j/source/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// The mapstructure package that is used within the sdk.Util.ParseConfig
// has some problems with concurrent access so we don't place the t.Parallel inside the loop.
//
//nolint:paralleltest,tparallel,nolintlint
func TestSource_Configure(t *testing.T) {
	t.Parallel()

	s := Source{}

	tests := []struct {
		name          string
		raw           map[string]string
		expectedError string
	}{
		{
			name: "success",
			raw: map[string]string{
				config.KeyURI:             "bolt://localhost:7687",
				config.KeyEntityType:      "node",
				config.KeyEntityLabels:    "Person,Writer",
				ConfigKeyOrderingProperty: "created_at",
				ConfigKeyBatchSize:        "1000",
				ConfigKeySnapshot:         "true",
			},
			expectedError: "",
		},
		{
			name: "fail_invalid_batchSize",
			raw: map[string]string{
				config.KeyURI:             "bolt://localhost:7687",
				config.KeyEntityType:      "node",
				config.KeyEntityLabels:    "Person,Writer",
				ConfigKeyOrderingProperty: "created_at",
				ConfigKeyBatchSize:        "one",
				ConfigKeySnapshot:         "true",
			},
			expectedError: "parse config: config invalid: error validating \"batchSize\": \"one\" value is not an integer: invalid parameter type",
		},
		{
			name: "fail_invalid_snapshot",
			raw: map[string]string{
				config.KeyURI:             "bolt://localhost:7687",
				config.KeyEntityType:      "node",
				config.KeyEntityLabels:    "Person,Writer",
				ConfigKeyOrderingProperty: "created_at",
				ConfigKeyBatchSize:        "1000",
				ConfigKeySnapshot:         "yes",
			},
			expectedError: "parse config: config invalid: error validating \"snapshot\": \"yes\" value is not a boolean: invalid parameter type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Configure(context.Background(), tt.raw)
			if err != nil {
				if tt.expectedError == "" || !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Configure() error = %v, expectedError is %s", err, tt.expectedError)

					return
				}
			}
		})
	}
}

func TestSource_Read_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	key := make(opencdc.StructuredData)
	key["id"] = 1

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Time{})

	record := opencdc.Record{
		Position: opencdc.Position(`{"lastId": 1}`),
		Metadata: metadata,
		Key:      key,
		Payload: opencdc.Change{
			After: key,
		},
	}

	snapshotIt := mock.NewMockIterator(ctrl)
	snapshotIt.EXPECT().HasNext(ctx).Return(true, nil)
	snapshotIt.EXPECT().Next(ctx).Return(record, nil)

	s := Source{snapshot: snapshotIt}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_successPolling(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	key := make(opencdc.StructuredData)
	key["id"] = 1

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Time{})

	record := opencdc.Record{
		Position: opencdc.Position(`{"lastId": 1}`),
		Metadata: metadata,
		Key:      key,
		Payload: opencdc.Change{
			After: key,
		},
	}

	snapshotIt := mock.NewMockIterator(ctrl)
	snapshotIt.EXPECT().HasNext(ctx).Return(false, sdk.ErrBackoffRetry)

	pollingSnapshotIt := mock.NewMockIterator(ctrl)
	pollingSnapshotIt.EXPECT().HasNext(ctx).Return(true, nil)
	pollingSnapshotIt.EXPECT().Next(ctx).Return(record, nil)

	s := Source{snapshot: snapshotIt, pollingSnapshot: pollingSnapshotIt}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_failHasNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	snapshotIt := mock.NewMockIterator(ctrl)
	snapshotIt.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

	s := Source{snapshot: snapshotIt}

	_, err := s.Read(ctx)
	is.True(err != nil)
}

func TestSource_Read_failNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	snapshotIt := mock.NewMockIterator(ctrl)
	snapshotIt.EXPECT().HasNext(ctx).Return(true, nil)
	snapshotIt.EXPECT().Next(ctx).Return(opencdc.Record{}, errors.New("key is not exist"))

	s := Source{snapshot: snapshotIt}

	_, err := s.Read(ctx)
	is.True(err != nil)
}
