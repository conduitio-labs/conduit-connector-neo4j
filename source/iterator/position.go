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

package iterator

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// PositionMode defines the [position] mode.
type PositionMode string

// The available position modes are listed below.
const (
	ModeSnapshot        PositionMode = "snapshot"
	ModeSnapshotPolling PositionMode = "snapshot_polling"
)

// Position is an iterator position.
type Position struct {
	Mode PositionMode `json:"mode"`
	// LastProcessedValue is a value of the last processed element by the snapshot capture.
	// This value is used if the mode is snapshot.
	LastProcessedValue any `json:"lastProcessedValue"`
	// MaxElement is a max value of an ordering property at the start of a snapshot.
	// This value is used if the mode is snapshot.
	MaxElement any `json:"maxElement,omitempty"`
}

// MarshalSDKPosition marshals the underlying [position] into a [sdk.Position] as JSON bytes.
func (p *Position) MarshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// ParsePosition converts an [sdk.Position] into a [position].
func ParsePosition(sdkPosition sdk.Position) (*Position, error) {
	if sdkPosition == nil {
		return nil, ErrNilSDKPosition
	}

	position := new(Position)
	if err := json.Unmarshal(sdkPosition, position); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into position: %w", err)
	}

	return position, nil
}
