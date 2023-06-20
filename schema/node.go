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

// Package schema holds definitions of models shared between different parts of the connector.
package schema

// Node defines a model for source and target nodes
// that need to be stored along with properties if the entityType is relationship.
type Node struct {
	Labels []string       `json:"labels"`
	Key    map[string]any `json:"key"`
}
