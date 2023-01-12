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

// Package config implements configurations shared between different parts of the connector.
package config

import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

const (
	// KeyURI is a config field name for a connection URI.
	KeyURI = "uri"
	// KeyKeyProperties is a config field name for a key properties.
	KeyKeyProperties = "keyProperties"
	// KeyDatabase is a config field name for a database.
	KeyDatabase = "database"
	// KeyAuthUsername is a config field name for a basic auth username.
	KeyAuthUsername = "auth.username"
	// KeyAuthPassword is a config field name for a basic auth password.
	KeyAuthPassword = "auth.password"
	// KeyAuthRealm is a config field name for a basic auth realm.
	KeyAuthRealm = "auth.realm"
)

// Config holds configurable values shared between Source and Destination.
type Config struct {
	// The connection URI pointed to a Neo4j instance.
	URI string `json:"uri" validate:"required"`
	// The comma separated list of column names to
	// build a WHERE clause in case sdk.Record.Key is empty (destination) or
	// build an sdk.Record.Key (source).
	KeyProperties []string `json:"keyProperties" validate:"required"`
	// The name of a database the connector should work with.
	Database string `json:"database" default:"neo4j"`
	// Auth holds auth-specific configurable values.
	Auth AuthConfig `json:"auth"`
}

// AuthConfig holds auth-specific configurable values.
type AuthConfig struct {
	// The username to use when performing basic auth.
	Username string `json:"username"`
	// The password to use when performing basic auth.
	Password string `json:"password"`
	// The realm to use when performing basic auth.
	Realm string `json:"realm"`
}

// AuthToken returns [neo4j.AuthToken] based on the [AuthConfig] values.
func (c AuthConfig) AuthToken() neo4j.AuthToken {
	if c.Username != "" || c.Password != "" || c.Realm != "" {
		return neo4j.BasicAuth(c.Username, c.Password, c.Realm)
	}

	return neo4j.NoAuth()
}
