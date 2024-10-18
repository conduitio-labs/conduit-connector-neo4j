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

package writer

import "errors"

var (
	// ErrEmptyRawData occurs when trying to structurize empty [opencdc.RawData].
	ErrEmptyRawData = errors.New("empty raw data")
	// ErrUnsupportedEntityType occurs when the entityType is unsupported by the [Writer].
	ErrUnsupportedEntityType = errors.New("unsupported entity type")
	// ErrEmptySourceNode occurs when the entityType is relationship but a payload doesn't contain sourceNode.
	ErrEmptySourceNode = errors.New("empty source node")
	// ErrEmptyTargetNode occurs when the entityType is relationship but a payload doesn't contain targetNode.
	ErrEmptyTargetNode = errors.New("empty target node")
)
