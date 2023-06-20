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

import (
	"testing"
)

func BenchmarkWriter_cypherMatchProperties(b *testing.B) {
	var (
		writer     = New(Params{})
		properties = map[string]any{"name": "Alex", "age": 23}
	)

	for i := 0; i < b.N; i++ {
		_, err := writer.cypherMatchProperties(properties, "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriter_cypherSetProperties(b *testing.B) {
	var (
		writer     = New(Params{})
		key        = map[string]any{"_id": 1}
		properties = map[string]any{"name": "Alex", "age": 23}
	)

	for i := 0; i < b.N; i++ {
		_, err := writer.cypherSetProperties(properties, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
