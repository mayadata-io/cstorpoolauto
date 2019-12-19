/*
Copyright 2019 The MayaData Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package string

import "strings"

// List is a representation of list of strings
type List []string

// ContainsExact returns true if given string is exact
// match with one if the items in the list
func (l List) ContainsExact(given string) bool {
	for _, available := range l {
		if available == given {
			return true
		}
	}
	return false
}

// Contains returns true if given string is a
// substring of the items in the list
func (l List) Contains(substr string) bool {
	for _, available := range l {
		if strings.Contains(available, substr) {
			return true
		}
	}
	return false
}
