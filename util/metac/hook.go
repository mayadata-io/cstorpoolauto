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

package metac

import (
	"fmt"
	"strings"

	"openebs.io/metac/controller/generic"
)

// GetDetailsFromRequest returns details of provided
// response in string format
func GetDetailsFromRequest(req *generic.SyncHookRequest) string {
	if req == nil {
		return ""
	}
	var message string = "Request -"
	if req.Watch == nil {
		return message + "Watch=nil"
	}
	var details []string
	details = append(
		details,
		message,
		fmt.Sprintf("[Watch %s %s]", req.Watch.GetNamespace(), req.Watch.GetName()),
	)
	var attachmentKinds map[string]int = map[string]int{}
	for _, attachment := range req.Attachments.List() {
		count := attachmentKinds[attachment.GetKind()]
		attachmentKinds[attachment.GetKind()] = count + 1
	}
	for kind, count := range attachmentKinds {
		details = append(
			details, fmt.Sprintf("[%s %d]", kind, count),
		)
	}
	return strings.Join(details, " ")
}

// GetDetailsFromResponse returns details of provided
// response in string format
//
// TODO:
//	Refactor this logic similar to GetDetailsFromRequest
func GetDetailsFromResponse(resp *generic.SyncHookResponse) string {
	if resp == nil || len(resp.Attachments) == 0 {
		return ""
	}
	var attachmentKinds map[string]int = map[string]int{}
	for _, attachment := range resp.Attachments {
		count := attachmentKinds[attachment.GetKind()]
		attachmentKinds[attachment.GetKind()] = count + 1
	}
	var attachmentResponse string = "Response Kind(s) -"
	for kind, count := range attachmentKinds {
		attachmentResponse = attachmentResponse + fmt.Sprintf(" [%s %d] ", kind, count)
	}
	return attachmentResponse
}
