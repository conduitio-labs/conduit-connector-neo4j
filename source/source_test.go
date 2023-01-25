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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

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
			expectedError: "cannot parse 'batchSize' as int",
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
			expectedError: "cannot parse 'snapshot' as bool",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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

	key := make(sdk.StructuredData)
	key["id"] = 1

	metadata := make(sdk.Metadata)
	metadata.SetCreatedAt(time.Time{})

	record := sdk.Record{
		Position: sdk.Position(`{"lastId": 1}`),
		Metadata: metadata,
		Key:      key,
		Payload: sdk.Change{
			After: key,
		},
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(record, nil)

	s := Source{iterator: it}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_failHasNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

	s := Source{iterator: it}

	_, err := s.Read(ctx)
	is.True(err != nil)
}

func TestSource_Read_failNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

	s := Source{iterator: it}

	_, err := s.Read(ctx)
	is.True(err != nil)
}

func TestSource_Teardown_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop(ctx).Return(nil)

	s := Source{iterator: it}

	err := s.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Teardown_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop(ctx).Return(errors.New("some error"))

	s := Source{iterator: it}

	err := s.Teardown(context.Background())
	is.Equal(err.Error(), "stop iterator: some error")
}
