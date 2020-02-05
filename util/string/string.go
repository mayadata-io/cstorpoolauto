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

// Map is a mapped representation of string with
// boolean value
type Map map[string]bool

// String implements Stringer interface
func (l List) String() string {
	return strings.Join(l, ", ")
}

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

// Equality helps in finding difference or merging
// list of string based items.
type Equality struct {
	src  List
	dest List
}

// NewEquality returns a populated Equality structure
func NewEquality(src, dest List) Equality {
	return Equality{
		src:  src,
		dest: dest,
	}
}

// Diff finds the difference between source list & destination
// list and returns the no change, addition & removal items
// respectively
func (e Equality) Diff() (noops Map, additions []string, removals []string) {
	noops = map[string]bool{}
	for _, source := range e.src {
		if e.dest.ContainsExact(source) {
			noops[source] = true
			continue
		}
		removals = append(removals, source)
	}
	for _, destination := range e.dest {
		if e.src.ContainsExact(destination) {
			continue
		}
		additions = append(additions, destination)
	}
	return
}

// Merge merges the source items with destination items
// by keeping the order of source items. Source items that
// need to be replaced as replaced from new destination
// items. It appends new used items to the end of the resulting
// list.
//
// TODO (@amitkumardas):
//	This doesnot handle cases of
//	1/ removal in groups
//	2/ replacement in groups
func (e Equality) Merge() []string {
	var new []string
	var used = map[string]bool{}
	var merge []string
	for _, destItem := range e.dest {
		if e.src.ContainsExact(destItem) {
			// nothing to be done here
			continue
		}
		// store this is a new item
		new = append(new, destItem)
	}
	// we want to merge by following the order of source list
	for _, sourceItem := range e.src {
		if e.dest.ContainsExact(sourceItem) {
			// no change; use this as merge item
			merge = append(merge, sourceItem)
			continue
		}
		// donot use this source item
		// replace source item with a new item if available
		if len(new) == 0 {
			continue
		}
		newItem := new[len(used)]
		merge = append(merge, newItem)
		// mark this new item as used
		used[newItem] = true
	}
	// check for extras
	for _, newItem := range new {
		if len(used) == 0 || !used[newItem] {
			// use this new item since it has not been
			// used as a replacement previously
			merge = append(merge, newItem)
		}
	}
	return merge
}
