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

package config

import (
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     map[string]string
		want    Config
		wantErr bool
	}{
		{
			name: "success",
			raw: map[string]string{
				KeyURI:          "http://localhost:33575",
				KeyEntityType:   "node",
				KeyEntityLabels: "Person,Worker",
				KeyDatabase:     "neo4j",
				KeyAuthUsername: "admin",
				KeyAuthPassword: "secret",
				KeyAuthRealm:    "realm",
			},
			want: Config{
				URI:          "http://localhost:33575",
				EntityType:   EntityTypeNode,
				EntityLabels: []string{"Person", "Worker"},
				Database:     "neo4j",
				Auth: AuthConfig{
					Username: "admin",
					Password: "secret",
					Realm:    "realm",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got Config
			err := sdk.Util.ParseConfig(tt.raw, &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
