// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"log"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
)

// this is a mock struct describing the states of the plugin after being register
type MockFLBPlugin struct {
	name string
	desc string
}

// this function mocks output.FLBPluginRegister by setting the fields of the MockFLBPlugin struct to the input parameter
// and returning a 0 (to imply success)
func (m *MockFLBPlugin) mockOutputRegister(def unsafe.Pointer, currname string, currdesc string) int {
	m.name = currname
	m.desc = currdesc

	return 0
}

// this function tests FLBPluginRegister
func TestFLBPluginRegister(t *testing.T) {
	currplugin := &MockFLBPlugin{}

	patch := monkey.Patch(output.FLBPluginRegister, currplugin.mockOutputRegister)

	defer patch.Unpatch()

	result := FLBPluginRegister(nil)

	assert.Equal(t, 0, result)
	assert.Equal(t, "writeapi", currplugin.name)

}

// this is a struct keeping track of whether the correct options are sent in NewManagedStream
type OptionChecks struct {
	configProjectID        bool
	configDatasetID        bool
	configTableID          bool
	configMaxChunkSize     bool
	configMaxQueueSize     bool
	configMaxQueueRequests bool
	calledGetClient        bool
	calledNewManagedStream bool
	calledGetWriteStream   bool
	calledSetContext       bool
	numInputs              bool
	mapSizeIncremented     bool
}

type MockManagedWriterClient struct {
	client               *managedwriter.Client
	NewManagedStreamFunc func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStreamFunc   func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	CloseFunc            func() error
}

func (m *MockManagedWriterClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
	return m.NewManagedStreamFunc(ctx, opts...)
}

func (m *MockManagedWriterClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return m.GetWriteStreamFunc(ctx, req, opts...)
}

func (m *MockManagedWriterClient) Close() error {
	return m.client.Close()
}

// TestFLBPluginInit tests the FLBPluginInit function
func TestFLBPluginInit(t *testing.T) {
	count := 0
	var currChecks OptionChecks
	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			count = count + 1
			if count == 3 {
				currChecks.calledNewManagedStream = true
			}
			if len(opts) == 6 {
				currChecks.numInputs = true
			}
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			count = count + 1
			if count == 2 {
				currChecks.calledGetWriteStream = true
			}
			return &storagepb.WriteStream{
				Name: "mockstream",
				TableSchema: &storagepb.TableSchema{
					Fields: []*storagepb.TableFieldSchema{
						{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
						{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					},
				},
			}, nil
		},
	}

	originalFunc := getClient
	getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
		count = count + 1
		if count == 1 {
			currChecks.calledGetClient = true
		}
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		log.Println("Mock out.FLBPluginConfigKey called")
		switch key {
		case "ProjectID":
			currChecks.configProjectID = true
			return "bigquerytestdefault"
		case "DatasetID":
			currChecks.configDatasetID = true
			return "siddag_summer2024"
		case "TableID":
			currChecks.configTableID = true
			return "raahi_summer2024table1"
		case "Max_Chunk_Size":
			currChecks.configMaxChunkSize = true
			return "1048576"
		case "Max_Queue_Requests":
			currChecks.configMaxQueueRequests = true
			return "100"
		case "Max_Queue_Bytes":
			currChecks.configMaxQueueSize = true
			return "52428800"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patch2 := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		currChecks.calledSetContext = true
	})
	defer patch2.Unpatch()

	plugin := unsafe.Pointer(nil)
	initsize := len(configMap)
	result := FLBPluginInit(plugin)
	finsize := len(configMap)
	if (finsize - 1) == initsize {
		currChecks.mapSizeIncremented = true
	}
	assert.Equal(t, output.FLB_OK, result)
	assert.True(t, currChecks.configProjectID)
	assert.True(t, currChecks.configDatasetID)
	assert.True(t, currChecks.configTableID)
	assert.True(t, currChecks.configMaxChunkSize)
	assert.True(t, currChecks.configMaxQueueRequests)
	assert.True(t, currChecks.configMaxQueueSize)
	assert.True(t, currChecks.calledGetClient)
	assert.True(t, currChecks.calledGetWriteStream)
	assert.True(t, currChecks.calledNewManagedStream)
	assert.True(t, currChecks.calledSetContext)
	assert.True(t, currChecks.numInputs)
	assert.True(t, currChecks.mapSizeIncremented)

}
