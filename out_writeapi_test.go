package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
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

type StreamChecks struct {
	calledGetContext int
	createDecoder    int
	gotRecord        int
	appendRows       int
	getResultsCount  int
	checkReady       int
}

type MockManagedStream struct {
	managedstream  *managedwriter.ManagedStream
	AppendRowsFunc func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error)
	CloseFunc      func() error
	FinalizeFunc   func(ctx context.Context, opts ...gax.CallOption) (int64, error)
	FlushRowsFunc  func(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error)
	StreamNameFunc func() string
}

func (m *MockManagedStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
	return m.AppendRowsFunc(ctx, data, opts...)
}

func (m *MockManagedStream) Finalize(ctx context.Context, opts ...gax.CallOption) (int64, error) {
	return m.FinalizeFunc(ctx, opts...)
}

func (m *MockManagedStream) FlushRows(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error) {
	return m.FlushRowsFunc(ctx, offset, opts...)
}

func (m *MockManagedStream) StreamName() string {
	return m.StreamNameFunc()
}

func (m *MockManagedStream) Close() error {
	return m.managedstream.Close()
}

func TestFLBPluginFlushCtx(t *testing.T) {
	checks := new(StreamChecks)
	var setID int
	mockClient := &MockManagedWriterClient{
		NewManagedStreamFunc: func(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
			return nil, nil

		},
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
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
		return mockClient, nil
	}
	defer func() { getClient = originalFunc }()

	// Slice that holds result of AppendRows to check in checkResponses
	appendResult := []bool{}

	md, _ := getDescriptors(ms_ctx, mockClient, "dummy", "dummy", "dummy")
	mockMS := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			checks.appendRows++

			var combinedData []byte
			for _, tempData := range data {
				combinedData = append(combinedData, tempData...)
			}

			message := dynamicpb.NewMessage(md)
			err := proto.Unmarshal(combinedData, message)
			if err != nil {
				return nil, fmt.Errorf("Failed to unmarshal")
			}

			textField := message.Get(md.Fields().ByJSONName("Text"))
			timeField := message.Get(md.Fields().ByJSONName("Time"))

			assert.Equal(t, "FOO", textField.String())
			assert.Equal(t, "000", timeField.String())

			appendResult = append(appendResult, true)
			return nil, nil
		},
		FinalizeFunc: func(ctx context.Context, opts ...gax.CallOption) (int64, error) {
			return 0, nil
		},
		FlushRowsFunc: func(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error) {
			return 0, nil
		},
		StreamNameFunc: func() string {
			return ""
		},
	}

	origFunc := getWriter
	getWriter = func(client ManagedWriterClient, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
		return mockMS, nil
	}
	defer func() { getWriter = origFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		return ""
	})
	defer patch1.Unpatch()

	patchSetContext := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		setID = ctx.(int)
	})
	defer patchSetContext.Unpatch()

	plugin := unsafe.Pointer(nil)
	initRes := FLBPluginInit(plugin)

	orgFunc := getFLBPluginContext
	getFLBPluginContext = func(ctx unsafe.Pointer) int {
		checks.calledGetContext++
		return setID
	}
	defer func() { getFLBPluginContext = orgFunc }()

	origReadyFunc := isReady
	isReady = func(queueHead *managedwriter.AppendResult) bool {
		return true
	}
	defer func() { isReady = origReadyFunc }()

	origResultFunc := pluginGetResult
	pluginGetResult = func(queueHead *managedwriter.AppendResult, ctx context.Context) (int64, error) {
		if appendResult[0] {
			checks.getResultsCount++
			return -1, nil
		}
		err := errors.New("Failed to Get Result")
		return 0, err
	}
	defer func() { pluginGetResult = origResultFunc }()

	patchDecoder := monkey.Patch(output.NewDecoder, func(data unsafe.Pointer, length int) *output.FLBDecoder {
		checks.createDecoder++
		return nil
	})
	defer patchDecoder.Unpatch()

	var rowSent int = 0
	var rowCount int = 2
	patchRecord := monkey.Patch(output.GetRecord, func(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{}) {
		checks.gotRecord++
		dummyRecord := make(map[interface{}]interface{})
		if rowSent < rowCount {
			rowSent++
			// Represents "FOO" in bytes as the data for the Text field
			dummyRecord["Text"] = []byte{70, 79, 79}
			// Represents "000" in bytes as the data for the Time field
			dummyRecord["Time"] = []byte{48, 48, 48}
			return 0, nil, dummyRecord
		}
		return 1, nil, nil
	})
	defer patchRecord.Unpatch()

	config := configMap[setID]
	config.messageDescriptor = md

	// Converts id (int) to type unsafe.Pointer to be used as the ctx
	uintptrValue := uintptr(setID)
	pointerValue := unsafe.Pointer(uintptrValue)

	// Calls FlushCtx with this ID
	result := FLBPluginFlushCtx(pointerValue, plugin, 1, nil)
	result = FLBPluginFlushCtx(pointerValue, plugin, 1, nil)

	assert.Equal(t, output.FLB_OK, initRes)
	assert.Equal(t, output.FLB_OK, result)
	assert.Equal(t, 2, checks.appendRows)
	assert.Equal(t, 2, checks.calledGetContext)
	assert.Equal(t, 2, checks.getResultsCount)
	assert.Equal(t, 2, checks.createDecoder)
	assert.Equal(t, 4, checks.gotRecord)
}
