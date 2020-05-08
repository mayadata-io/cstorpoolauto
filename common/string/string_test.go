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

import "testing"

func TestEqualityDiff(t *testing.T) {
	var tests = map[string]struct {
		src      List
		dest     List
		noops    map[string]bool
		adds     []string
		removals []string
	}{
		"src == dest": {
			src:  List([]string{"hi", "hello"}),
			dest: List([]string{"hi", "hello"}),
			noops: map[string]bool{
				"hi":    true,
				"hello": true,
			},
			adds:     nil,
			removals: nil,
		},
		"src != dest; extra values in dest": {
			src:  List([]string{"hi", "hello"}),
			dest: List([]string{"hi", "hello", "there", "how are you"}),
			noops: map[string]bool{
				"hi":    true,
				"hello": true,
			},
			adds:     []string{"there", "how are you"},
			removals: nil,
		},
		"src != dest; missing values in dest": {
			src:  List([]string{"hi", "hello"}),
			dest: List([]string{"hello"}),
			noops: map[string]bool{
				"hello": true,
			},
			adds:     nil,
			removals: []string{"hi"},
		},
		"src != dest; missing & new values in dest": {
			src:  List([]string{"hi", "hello"}),
			dest: List([]string{"hello", "how", "are", "you"}),
			noops: map[string]bool{
				"hello": true,
			},
			adds:     []string{"how", "are", "you"},
			removals: []string{"hi"},
		},
		"src != dest; nil src": {
			src:      List(nil),
			dest:     List([]string{"hello", "how", "are", "you"}),
			noops:    map[string]bool{},
			adds:     []string{"hello", "how", "are", "you"},
			removals: nil,
		},
		"src != dest; empty src": {
			src:      List([]string{}),
			dest:     List([]string{"hello", "how", "are", "you"}),
			noops:    map[string]bool{},
			adds:     []string{"hello", "how", "are", "you"},
			removals: nil,
		},
		"src != dest; nil dest": {
			src:      List([]string{"hi", "hello"}),
			dest:     List(nil),
			noops:    map[string]bool{},
			adds:     nil,
			removals: []string{"hi", "hello"},
		},
		"src != dest; empty dest": {
			src:      List([]string{"hi", "hello"}),
			dest:     List([]string{}),
			noops:    map[string]bool{},
			adds:     nil,
			removals: []string{"hi", "hello"},
		},
		"src == dest == nil": {
			src:      List(nil),
			dest:     List(nil),
			noops:    map[string]bool{},
			adds:     nil,
			removals: nil,
		},
		"src == dest == empty": {
			src:      List([]string{}),
			dest:     List([]string{}),
			noops:    map[string]bool{},
			adds:     nil,
			removals: nil,
		},
	}
	for name, mock := range tests {
		t.Run(name, func(t *testing.T) {
			e := &Equality{
				src:  mock.src,
				dest: mock.dest,
			}
			noops, adds, removals := e.Diff()
			if len(noops) != len(mock.noops) {
				t.Fatalf("Expected noops [%+v] got [%+v]", mock.noops, noops)
			}
			if len(adds) != len(mock.adds) {
				t.Fatalf("Expected adds count %d got %d", len(mock.adds), len(adds))
			}
			if len(removals) != len(mock.removals) {
				t.Fatalf("Expected removals count %d got %d", len(mock.removals), len(removals))
			}
			addsList := List(adds)
			for _, addItem := range mock.adds {
				if !addsList.ContainsExact(addItem) {
					t.Fatalf("Expected adds [%+v] got [%+v]", mock.adds, adds)
				}
			}
			removalList := List(removals)
			for _, removalItem := range mock.removals {
				if !removalList.ContainsExact(removalItem) {
					t.Fatalf("Expected removals [%+v] got [%+v]", mock.removals, removals)
				}
			}
		})
	}
}

func TestEqualityMerge(t *testing.T) {
	var tests = map[string]struct {
		src    List
		dest   List
		expect []string
	}{
		"src != dest && dest count = 0": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{}),
			expect: []string{},
		},
		"src != dest && dest count < src count": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{"4", "5"}),
			expect: []string{"4", "5"},
		},
		"src != dest && dest count > src count": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{"4", "5", "6", "7"}),
			expect: []string{"4", "5", "6", "7"},
		},
		"src != dest && dest count == src count": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{"4", "5", "6"}),
			expect: []string{"4", "5", "6"},
		},
		"src == dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hi", "hello"}),
			expect: []string{"hi", "hello"},
		},
		"dest >> src; extra values in dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hi", "hello", "there", "how are you"}),
			expect: []string{"hi", "hello", "there", "how are you"},
		},
		"src >> dest; missing values in dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hello"}),
			expect: []string{"hello"},
		},
		"src != dest; missing & new values in dest": {
			src:  List([]string{"hi", "hello"}),
			dest: List([]string{"hello", "how", "are", "you"}),
			// note that order of src is maintained
			expect: []string{"how", "hello", "are", "you"},
		},
		"src != dest; multiple missing & multiple new values in dest": {
			src:  List([]string{"hi", "hello", "app", "cstor", "type", "nice"}),
			dest: List([]string{"hello", "nice", "how", "are", "you", "app"}),
			// note that order of src is maintained
			expect: []string{"how", "hello", "app", "are", "you", "nice"},
		},
		"src != dest; nil src": {
			src:    List(nil),
			dest:   List([]string{"hello", "how", "are", "you"}),
			expect: []string{"hello", "how", "are", "you"},
		},
		"src != dest; empty src": {
			src:    List([]string{}),
			dest:   List([]string{"hello", "how", "are", "you"}),
			expect: []string{"hello", "how", "are", "you"},
		},
		"src != dest; nil dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List(nil),
			expect: nil,
		},
		"src != dest; empty dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{}),
			expect: nil,
		},
		"src == dest == nil": {
			src:    List(nil),
			dest:   List(nil),
			expect: nil,
		},
		"src == dest == empty": {
			src:    List([]string{}),
			dest:   List([]string{}),
			expect: nil,
		},
	}
	for name, mock := range tests {
		t.Run(name, func(t *testing.T) {
			e := &Equality{
				src:  mock.src,
				dest: mock.dest,
			}
			got := e.Merge()
			if len(got) != len(mock.expect) {
				t.Fatalf("Expected [%+v] got [%+v]", mock.expect, got)
			}
			for idx, gotItem := range got {
				if gotItem != mock.expect[idx] {
					t.Fatalf("Expected %s got %s at index %d", mock.expect[idx], gotItem, idx)
				}
			}
		})
	}
}

func TestEqualityIsDiff(t *testing.T) {
	var tests = map[string]struct {
		src    List
		dest   List
		isDiff bool
	}{
		"src != dest && dest count = 0": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{}),
			isDiff: true,
		},
		"src != dest && dest count < src count": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{"4", "5"}),
			isDiff: true,
		},
		"src != dest && dest count > src count": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{"4", "5", "6", "7"}),
			isDiff: true,
		},
		"src != dest && dest count == src count": {
			src:    List([]string{"1", "2", "3"}),
			dest:   List([]string{"4", "5", "6"}),
			isDiff: true,
		},
		"src == dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hi", "hello"}),
			isDiff: false,
		},
		"src == dest but different order": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hello", "hi"}),
			isDiff: false,
		},
		"src != dest with only case mismatch": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"Hello", "Hi"}),
			isDiff: true,
		},
		"dest >> src; extra values in dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hi", "hello", "there", "how are you"}),
			isDiff: true,
		},
		"src >> dest; missing values in dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hello"}),
			isDiff: true,
		},
		"src != dest; missing & new values in dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{"hello", "how", "are", "you"}),
			isDiff: true,
		},
		"src != dest; multiple missing & multiple new values in dest": {
			src:    List([]string{"hi", "hello", "app", "cstor", "type", "nice"}),
			dest:   List([]string{"hello", "nice", "how", "are", "you", "app"}),
			isDiff: true,
		},
		"src != dest; nil src": {
			src:    List(nil),
			dest:   List([]string{"hello", "how", "are", "you"}),
			isDiff: true,
		},
		"src != dest; empty src": {
			src:    List([]string{}),
			dest:   List([]string{"hello", "how", "are", "you"}),
			isDiff: true,
		},
		"src != dest; nil dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List(nil),
			isDiff: true,
		},
		"src != dest; empty dest": {
			src:    List([]string{"hi", "hello"}),
			dest:   List([]string{}),
			isDiff: true,
		},
		"src == dest == nil": {
			src:    List(nil),
			dest:   List(nil),
			isDiff: false,
		},
		"src == dest == empty": {
			src:    List([]string{}),
			dest:   List([]string{}),
			isDiff: false,
		},
		"src = nil && dest == empty": {
			src:    List(nil),
			dest:   List([]string{}),
			isDiff: false,
		},
	}
	for name, mock := range tests {
		t.Run(name, func(t *testing.T) {
			e := &Equality{
				src:  mock.src,
				dest: mock.dest,
			}
			got := e.IsDiff()
			if got != mock.isDiff {
				t.Fatalf("Expected %t got %t", mock.isDiff, got)
			}
		})
	}
}
