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

	"openebs.io/metac/controller/generic"
)

// GetDetailsFromResponse returns details of provided
// response in string format
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
