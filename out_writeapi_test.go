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
	configExactlyOnce      bool
	calledGetClient        int
	calledNewManagedStream int
	calledGetWriteStream   int
	calledSetContext       int
	numInputs              bool
	mapSizeIncremented     bool
}

type MockManagedWriterClient struct {
	client                      *managedwriter.Client
	NewManagedStreamFunc        func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStreamFunc          func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	CloseFunc                   func() error
	BatchCommitWriteStreamsFunc func(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error)
	CreateWriteStreamFunc       func(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
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

func (m *MockManagedWriterClient) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	return m.client.BatchCommitWriteStreams(ctx, req, opts...)
}

func (m *MockManagedWriterClient) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return m.client.CreateWriteStream(ctx, req, opts...)
}

// TestFLBPluginInit tests the FLBPluginInit function
func TestFLBPluginInitExactlyOnce(t *testing.T) {
	var currChecks OptionChecks
	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			currChecks.calledNewManagedStream++
			if len(opts) == 6 {
				currChecks.numInputs = true
			}
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			currChecks.calledGetWriteStream++
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
		currChecks.calledGetClient++
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		log.Println("Mock out.FLBPluginConfigKey called")
		switch key {
		case "ProjectID":
			currChecks.configProjectID = true
			return "DummyProjectId"
		case "DatasetID":
			currChecks.configDatasetID = true
			return "DummyDatasetId"
		case "TableID":
			currChecks.configTableID = true
			return "DummyTableId"
		case "Max_Chunk_Size":
			currChecks.configMaxChunkSize = true
			return "0"
		case "Max_Queue_Requests":
			currChecks.configMaxQueueRequests = true
			return "0"
		case "Max_Queue_Bytes":
			currChecks.configMaxQueueSize = true
			return "0"
		case "Exactly_Once":
			currChecks.configExactlyOnce = true
			return "False"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patch2 := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		currChecks.calledSetContext++
	})
	defer patch2.Unpatch()

	plugin := unsafe.Pointer(nil)
	initsize := getInstanceCount()
	result := FLBPluginInit(plugin)
	finsize := getInstanceCount()
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
	assert.True(t, currChecks.configExactlyOnce)
	assert.Equal(t, 1, currChecks.calledGetClient)
	assert.Equal(t, 1, currChecks.calledGetWriteStream)
	assert.Equal(t, 1, currChecks.calledNewManagedStream)
	assert.Equal(t, 1, currChecks.calledSetContext)
	assert.True(t, currChecks.numInputs)
	assert.True(t, currChecks.mapSizeIncremented)

}

func TestFLBPluginInit(t *testing.T) {
	var currChecks OptionChecks
	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			currChecks.calledNewManagedStream++
			if len(opts) == 6 {
				currChecks.numInputs = true
			}
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			currChecks.calledGetWriteStream++
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
		currChecks.calledGetClient++
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		log.Println("Mock out.FLBPluginConfigKey called")
		switch key {
		case "ProjectID":
			currChecks.configProjectID = true
			return "DummyProjectId"
		case "DatasetID":
			currChecks.configDatasetID = true
			return "DummyDatasetId"
		case "TableID":
			currChecks.configTableID = true
			return "DummyTableId"
		case "Max_Chunk_Size":
			currChecks.configMaxChunkSize = true
			return "0"
		case "Max_Queue_Requests":
			currChecks.configMaxQueueRequests = true
			return "0"
		case "Max_Queue_Bytes":
			currChecks.configMaxQueueSize = true
			return "0"
		case "Exactly_Once":
			currChecks.configExactlyOnce = true
			return "True"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patch2 := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		currChecks.calledSetContext++
	})
	defer patch2.Unpatch()

	plugin := unsafe.Pointer(nil)
	initsize := getInstanceCount()
	result := FLBPluginInit(plugin)
	finsize := getInstanceCount()
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
	assert.True(t, currChecks.configExactlyOnce)
	assert.Equal(t, 1, currChecks.calledGetClient)
	assert.Equal(t, 1, currChecks.calledGetWriteStream)
	assert.Equal(t, 1, currChecks.calledNewManagedStream)
	assert.Equal(t, 1, currChecks.calledSetContext)
	assert.True(t, currChecks.numInputs)
	assert.True(t, currChecks.mapSizeIncremented)

}
