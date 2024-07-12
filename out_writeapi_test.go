package main

import (
	"context"
	"log"
	_ "log"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
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
			return "tanip_summer2024table"
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
	// assert.True(t, currChecks.calledNewManagedStream)
	assert.True(t, currChecks.calledSetContext)
	assert.True(t, currChecks.numInputs)
	assert.True(t, currChecks.mapSizeIncremented)

}

type StreamChecks struct {
	calledGetContext     bool
	calledcheckResponses bool
	createDecoder        bool
	gotRecord            bool
	calledparseMap       bool
	calledJTB            bool
	appendRows           bool
	appendQueue          bool
}

type MockManagedStream struct {
	managedstream  *managedwriter.ManagedStream
	AppendRowsFunc func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error)
	StreamName     func() string
	CloseFunc      func() error
}

func (m *MockManagedStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
	return m.AppendRowsFunc(ctx, data, opts...)
}

func (m *MockManagedStream) Close() error {
	return m.managedstream.Close()
}

func TestFLBPluginFlushCtx(t *testing.T) {
	// Calling Init
	// how do we use init with the bottom code
	// we need to mock everything in init too

	var checks StreamChecks
	var setID int = 0
	// var getID int = 0
	// getIDUintptr := uintptr(getID)

	mockMS := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			checks.appendRows = true
			return nil, nil
		},
	}

	originalFunc := getWriter
	getWriter = func(ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
		return mockMS, nil
	}
	defer func() { getWriter = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		switch key {
		case "ProjectID":
			return "bigquerytestdefault"
		case "DatasetID":
			return "siddag_summer2024"
		case "TableID":
			return "raahi_summer2024table1"
		case "Max_Chunk_Size":
			return "1048576"
		case "Max_Queue_Requests":
			return "100"
		case "Max_Queue_Bytes":
			return "52428800"
		default:
			return ""
		}
	})
	defer patch1.Unpatch()

	patchDescriptor := monkey.Patch(getDescriptors, func(curr_ctx context.Context, managed_writer_client ManagedWriterClient, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
		return nil, nil
	})
	defer patchDescriptor.Unpatch()

	patchSetContext := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		setID = setID + 1
	})
	defer patchSetContext.Unpatch()

	plugin := unsafe.Pointer(nil)
	// ctx := unsafe.Pointer(nil)
	initRes := FLBPluginInit(plugin)

	// patchGetContext := monkey.Patch(output.FLBPluginGetContext, func(proxyCtx unsafe.Pointer) interface{} {
	// 	getID = setID - 1
	// 	checks.calledGetContext = true
	// 	return getID
	// })
	// defer patchGetContext.Unpatch()

	patchCheckResponses := monkey.Patch(checkResponses, func(curr_ctx context.Context, currQueuePointer *[]*managedwriter.AppendResult, waitForResponse bool) int {
		checks.calledcheckResponses = true
		return 0
	})
	defer patchCheckResponses.Unpatch()

	patchDecoder := monkey.Patch(output.NewDecoder, func(data unsafe.Pointer, length int) *output.FLBDecoder {
		checks.createDecoder = true
		return nil
	})
	defer patchDecoder.Unpatch()

	var loopCount int = 0
	patchRecord := monkey.Patch(output.GetRecord, func(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{}) {
		checks.gotRecord = true
		loopCount++
		return loopCount - 1, nil, nil
	})
	defer patchRecord.Unpatch()

	patchParse := monkey.Patch(parseMap, func(mapInterface map[interface{}]interface{}) map[string]interface{} {
		checks.calledparseMap = true
		return nil
	})
	defer patchParse.Unpatch()

	patchJTB := monkey.Patch(json_to_binary, func(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
		checks.calledJTB = true
		tempBD := make([]byte, 1)
		tempBD[0] = 1
		return tempBD, nil
	})
	defer patchJTB.Unpatch()

	config := configMap[0]
	initsize := len(*config.appendResults)

	// // should we call flush twice to ensure the queue is remaining updated between calls?
	//result := FLBPluginFlushCtx(unsafe.Pointer(getIDUintptr), plugin, 1, nil)
	log.Println("Hi!")
	result := FLBPluginFlushCtx(unsafe.Pointer(uintptr(0)), plugin, 1, nil)
	loopCount = 0
	result = FLBPluginFlushCtx(unsafe.Pointer(uintptr(0)), plugin, 1, nil)
	// result = FLBPluginFlushCtx(ctx, plugin, 1, nil)

	// print(result)

	finsize := len(*config.appendResults)
	if finsize-2 == initsize {
		checks.appendQueue = true
	}

	assert.Equal(t, output.FLB_OK, initRes)
	assert.Equal(t, output.FLB_OK, result)
	assert.True(t, checks.appendRows)
	assert.True(t, checks.calledcheckResponses)
	// // assert.True(t, checks.calledGetContext)
	assert.True(t, checks.appendQueue)
	assert.True(t, checks.createDecoder)
	assert.True(t, checks.gotRecord)
}
