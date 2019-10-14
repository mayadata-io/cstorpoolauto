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

package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"cspcauto/lib"

	"k8s.io/apimachinery/pkg/util/json"
)

func genericHookHandle(
	w http.ResponseWriter,
	r *http.Request,
	ghFn func(*lib.GenericHookRequest) (*lib.GenericHookResponse, error),
) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	request := &lib.GenericHookRequest{}
	if err := json.Unmarshal(body, request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	response, err := ghFn(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body, err = json.Marshal(&response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func sync(w http.ResponseWriter, r *http.Request) {
	genericHookHandle(w, r, lib.SyncCSPCAuto)
}

// this is a api service that handles http requests
func main() {
	http.HandleFunc("/sync", sync)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
