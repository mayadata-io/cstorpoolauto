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

package lib

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestIsAnnotationMatch(t *testing.T) {
	tests := map[string]struct {
		selector AnySelectorTerm
		target   *unstructured.Unstructured
		isError  bool
		isMatch  bool
	}{
		"Empty selector": {
			selector: AnySelectorTerm{},
			target:   &unstructured.Unstructured{},
			isError:  false,
			isMatch:  true,
		},
		"Empty selector && nil target": {
			selector: AnySelectorTerm{},
			target:   nil,
			isError:  false,
			isMatch:  true,
		},
		"MatchAnnotations selector && nil target": {
			selector: AnySelectorTerm{
				MatchAnnotations: map[string]string{
					"app": "trial",
				},
			},
			target:  nil,
			isError: true,
			isMatch: false,
		},
		"MatchAnnotations selector && matching annotations": {
			selector: AnySelectorTerm{
				MatchAnnotations: map[string]string{
					"app": "trial",
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"Match && Expression annotation selector && matching annotations - 1": {
			selector: AnySelectorTerm{
				MatchAnnotations: map[string]string{
					"app": "trial",
				},
				MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"it"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"Match && Expression annotation selector && matching annotations - Exhaustive": {
			selector: AnySelectorTerm{
				MatchAnnotations: map[string]string{
					"do":  "it",
					"app": "trial",
				},
				MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "nope",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "hulla",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"huh"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "hulla",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"huh", "it", "trial"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh", "it"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh", "it", "trial"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchAnnotations selector && non matching annotations": {
			selector: AnySelectorTerm{
				MatchAnnotations: map[string]string{
					"donot": "doit",
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
		"Invalid MatchAnnotationExpression selector && matching annotations": {
			selector: AnySelectorTerm{
				MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"it"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
		"MatchAnnotationExpression selector && match & non match annotations": {
			selector: AnySelectorTerm{
				MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "hulla",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
	}
	for name, mock := range tests {
		name := name // pin it
		mock := mock // pin it
		t.Run(name, func(t *testing.T) {
			match, err := isAnnotationMatch(mock.selector, mock.target)
			if mock.isError && err == nil {
				t.Fatalf("%s: Expected error: Got none", name)
			}
			if mock.isMatch && !match {
				t.Fatalf("%s: Expected match: Got no match", name)
			}

		})
	}
}

func TestIsLabelMatch(t *testing.T) {
	tests := map[string]struct {
		selector AnySelectorTerm
		target   *unstructured.Unstructured
		isError  bool
		isMatch  bool
	}{
		"Empty selector": {
			selector: AnySelectorTerm{},
			target:   &unstructured.Unstructured{},
			isError:  false,
			isMatch:  true,
		},
		"Empty selector && nil target": {
			selector: AnySelectorTerm{},
			target:   nil,
			isError:  false,
			isMatch:  true,
		},
		"MatchLabels selector && nil target": {
			selector: AnySelectorTerm{
				MatchLabels: map[string]string{
					"app": "trial",
				},
			},
			target:  nil,
			isError: true,
			isMatch: false,
		},
		"MatchLabels selector && matching labels": {
			selector: AnySelectorTerm{
				MatchLabels: map[string]string{
					"app": "trial",
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"Match && Expression label selector && matching labels": {
			selector: AnySelectorTerm{
				MatchLabels: map[string]string{
					"app": "trial",
				},
				MatchLabelExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"it"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"Match && Expression label selector && matching labels - Exhaustive": {
			selector: AnySelectorTerm{
				MatchLabels: map[string]string{
					"do":  "it",
					"app": "trial",
				},
				MatchLabelExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "nope",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "hulla",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"huh"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"huh"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh", "it"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"huh"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh", "trial"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchLabels selector && non matching labels": {
			selector: AnySelectorTerm{
				MatchLabels: map[string]string{
					"donot": "doit",
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
		"Invalid MatchLabelExpressions selector && matching labels": {
			selector: AnySelectorTerm{
				MatchLabelExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "do",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"it"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
		"MatchLabelExpressions selector && match & non match labels": {
			selector: AnySelectorTerm{
				MatchLabelExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "app",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "hulla",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
	}
	for name, mock := range tests {
		name := name // pin it
		mock := mock // pin it
		t.Run(name, func(t *testing.T) {
			match, err := isLabelMatch(mock.selector, mock.target)
			if mock.isError && err == nil {
				t.Fatalf("%s: Expected error: Got none", name)
			}
			if !mock.isError && err != nil {
				t.Fatalf("%s: Expected no error: Got %v", name, err)
			}
			if mock.isMatch && !match {
				t.Fatalf("%s: Expected match: Got no match", name)
			}
			if !mock.isMatch && match {
				t.Fatalf("%s: Expected no match: Got match", name)
			}
		})
	}
}

func TestIsSliceMatchFinalizers(t *testing.T) {
	tests := map[string]struct {
		selector AnySelectorTerm
		target   *unstructured.Unstructured
		isError  bool
		isMatch  bool
	}{
		"Match finalizers": {
			selector: AnySelectorTerm{
				MatchSliceExpressions: []SliceSelectorRequirement{
					SliceSelectorRequirement{
						Key:      "metadata.finalizers",
						Operator: SliceSelectorOpIn,
						Values:   []string{"pvc-protect", "storage-protect", "app-protect"},
					},
					SliceSelectorRequirement{
						Key:      "metadata.finalizers",
						Operator: SliceSelectorOpIn,
						Values:   []string{"pvc-protect", "storage-protect"},
					},
					SliceSelectorRequirement{
						Key:      "metadata.finalizers",
						Operator: SliceSelectorOpNotIn,
						Values:   []string{"unknown-protect"},
					},
					SliceSelectorRequirement{
						Key:      "metadata.finalizers",
						Operator: SliceSelectorOpNotIn,
						Values:   []string{"unknown-protect", "storage-protect"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"finalizers": []string{
							"pvc-protect",
							"storage-protect",
							"app-protect",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
	}
	for name, mock := range tests {
		name := name // pin it
		mock := mock // pin it
		t.Run(name, func(t *testing.T) {
			match, err := isFieldMatch(mock.selector, mock.target)
			if mock.isError && err == nil {
				t.Fatalf("%s: Expected error: Got none", name)
			}
			if !mock.isError && err != nil {
				t.Fatalf("%s: Expected no error: Got %v", name, err)
			}
			if mock.isMatch && !match {
				t.Fatalf("%s: Expected match: Got no match", name)
			}
			if !mock.isMatch && match {
				t.Fatalf("%s: Expected no match: Got match", name)
			}
		})
	}
}

func TestIsFieldMatch(t *testing.T) {
	tests := map[string]struct {
		selector AnySelectorTerm
		target   *unstructured.Unstructured
		isError  bool
		isMatch  bool
	}{
		"Empty selector": {
			selector: AnySelectorTerm{},
			target:   &unstructured.Unstructured{},
			isError:  false,
			isMatch:  true,
		},
		"Empty selector && nil target": {
			selector: AnySelectorTerm{},
			target:   nil,
			isError:  false,
			isMatch:  true,
		},
		"MatchFields selector && nil target": {
			selector: AnySelectorTerm{
				MatchFields: map[string]string{
					"metadata.annotations.app": "trial",
				},
			},
			target:  nil,
			isError: true,
			isMatch: false,
		},
		"MatchFields selector && matching fields": {
			selector: AnySelectorTerm{
				MatchFields: map[string]string{
					"metadata.labels.app":     "trial",
					"metadata.annotations.do": "it",
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
						"annotations": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"Match && Expression field selector && matching fields - 1": {
			selector: AnySelectorTerm{
				MatchFields: map[string]string{
					"metadata.labels.app": "trial",
				},
				MatchFieldExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.app",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"it"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"Match && Expression field selector && matching fields - Exhaustive": {
			selector: AnySelectorTerm{
				MatchFields: map[string]string{
					"metadata.labels.do":  "it",
					"metadata.labels.app": "trial",
				},
				MatchFieldExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.app",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpExists,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.nope",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.hulla",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"huh"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.app",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"trial"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.app",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"trial", "it", "itsss"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.app",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"trialsss"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"it"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"it", "trial", "trialsss"},
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"ittt"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields selector && non matching fields": {
			selector: AnySelectorTerm{
				MatchFields: map[string]string{
					"metadata.labels.donot": "doit",
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
		"Invalid MatchFieldExpressions selector && matching fields": {
			selector: AnySelectorTerm{
				MatchFieldExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.do",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"it"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"do": "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
		"MatchFieldExpressions selector && match & non match fields": {
			selector: AnySelectorTerm{
				MatchFieldExpressions: []metav1.LabelSelectorRequirement{
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.app",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					metav1.LabelSelectorRequirement{
						Key:      "metadata.labels.hulla",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"huh"},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
							"do":  "it",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
	}
	for name, mock := range tests {
		name := name // pin it
		mock := mock // pin it
		t.Run(name, func(t *testing.T) {
			match, err := isFieldMatch(mock.selector, mock.target)
			if mock.isError && err == nil {
				t.Fatalf("%s: Expected error: Got none", name)
			}
			if !mock.isError && err != nil {
				t.Fatalf("%s: Expected no error: Got %v", name, err)
			}
			if mock.isMatch && !match {
				t.Fatalf("%s: Expected match: Got no match", name)
			}
			if !mock.isMatch && match {
				t.Fatalf("%s: Expected no match: Got match", name)
			}
		})
	}
}

func TestIsMatch(t *testing.T) {
	tests := map[string]struct {
		selectExecutor AnySelector
		target         *unstructured.Unstructured
		isError        bool
		isMatch        bool
	}{
		"Empty selector": {
			selectExecutor: []*AnySelectorTerm{},
			target:         &unstructured.Unstructured{},
			isError:        false,
			isMatch:        true,
		},
		"Empty selector && nil target": {
			selectExecutor: []*AnySelectorTerm{},
			target:         nil,
			isError:        false,
			isMatch:        true,
		},
		"MatchAnnotations selector && nil target": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchAnnotations: map[string]string{
						"app": "trial",
					},
				},
			},
			target:  nil,
			isError: true,
			isMatch: false,
		},
		"MatchLabels selector && nil target": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trial",
					},
				},
			},
			target:  nil,
			isError: true,
			isMatch: false,
		},
		"MatchFields selector && nil target": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trial",
					},
				},
			},
			target:  nil,
			isError: true,
			isMatch: false,
		},
		"MatchAnnotations selector && matching annotations": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchAnnotations: map[string]string{
						"app": "trial",
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchLabels selector && matching annotations": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trial",
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields selector && matching annotations": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trial",
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields (T) || MatchLabels (F) selector": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trial",
					},
				},
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trialss",
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields (F) || MatchLabels (T) selector": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trial",
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields (F) || MatchLabels (F) || MatchAnnotations (T) selector": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchAnnotations: map[string]string{
						"app": "trial",
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
						"annotations": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields (All F) || MatchLabels (All F) || MatchAnnotations (One T) selector": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchFieldExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "metadata.labels.app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"trialss"},
						},
					},
				},
				&AnySelectorTerm{
					MatchFieldExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "metadata.labels.app",
							Operator: metav1.LabelSelectorOpDoesNotExist,
						},
					},
				},
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchLabelExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"trialss"},
						},
					},
				},
				&AnySelectorTerm{
					MatchAnnotations: map[string]string{
						"app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   []string{"trial"},
						},
					},
				},
				&AnySelectorTerm{
					MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"trialss"},
						},
					},
				},
				&AnySelectorTerm{
					MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpDoesNotExist,
						},
					},
				},
				&AnySelectorTerm{
					MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"trial"},
						},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
						"annotations": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: true,
		},
		"MatchFields (All F) || MatchLabels (All F) || MatchAnnotations (All F) selector": {
			selectExecutor: []*AnySelectorTerm{
				&AnySelectorTerm{
					MatchFields: map[string]string{
						"metadata.labels.app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchFieldExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "metadata.labels.app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"trialss"},
						},
					},
				},
				&AnySelectorTerm{
					MatchFieldExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "metadata.labels.app",
							Operator: metav1.LabelSelectorOpDoesNotExist,
						},
					},
				},
				&AnySelectorTerm{
					MatchLabels: map[string]string{
						"app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchLabelExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"trialss"},
						},
					},
				},
				&AnySelectorTerm{
					MatchAnnotations: map[string]string{
						"app": "trialss",
					},
				},
				&AnySelectorTerm{
					MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "app",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   []string{"trial"},
						},
					},
				},
				&AnySelectorTerm{
					MatchAnnotationExpressions: []metav1.LabelSelectorRequirement{
						metav1.LabelSelectorRequirement{
							Key:      "jumbo",
							Operator: metav1.LabelSelectorOpExists,
						},
					},
				},
			},
			target: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "trial",
						},
						"annotations": map[string]interface{}{
							"app": "trial",
						},
					},
				},
			},
			isError: false,
			isMatch: false,
		},
	}
	for name, mock := range tests {
		name := name // pin it
		mock := mock // pin it
		t.Run(name, func(t *testing.T) {
			match, err := mock.selectExecutor.Match(mock.target)
			if mock.isError && err == nil {
				t.Fatalf("%s: Expected error: Got none", name)
			}
			if !mock.isError && err != nil {
				t.Fatalf("%s: Expected no error: Got %v", name, err)
			}
			if mock.isMatch && !match {
				t.Fatalf("%s: Expected match: Got no match", name)
			}
			if !mock.isMatch && match {
				t.Fatalf("%s: Expected no match: Got match", name)
			}
		})
	}
}
