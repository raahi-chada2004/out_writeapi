package main

import (
	"context"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
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
	plugin := unsafe.Pointer(nil)
	ctx := unsafe.Pointer(nil)
	initRes := FLBPluginInit(plugin)

	var checks StreamChecks

	mockMS := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			checks.appendRows = true
			return nil, nil
		},
	}

	patchGetContext := monkey.Patch(output.FLBPluginGetContext, func(plugin unsafe.Pointer) interface{} {
		checks.calledGetContext = true
		return 0
	})
	defer patchGetContext.Unpatch()

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

	patchRecord := monkey.Patch(output.GetRecord, func(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{}) {
		checks.gotRecord = true
		return 0, nil, nil
	})
	defer patchRecord.Unpatch()

	patchParse := monkey.Patch(parseMap, func(mapInterface map[interface{}]interface{}) map[string]interface{} {
		checks.calledparseMap = true
		return nil
	})
	defer patchParse.Unpatch()

	patchJTB := monkey.Patch(json_to_binary, func(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
		checks.calledJTB = true
		return nil, nil
	})
	defer patchJTB.Unpatch()

	config := configMap[0]
	config.managedStream = mockMS
	initsize := len(*config.appendResults)

	// should we call flush twice to ensure the queue is remaining updated between calls?
	result := FLBPluginFlushCtx(ctx, plugin, 0, nil)

	finsize := len(*config.appendResults)
	if finsize-1 == initsize {
		checks.appendQueue = true
	}

	assert.Equal(t, output.FLB_OK, initRes)
	assert.Equal(t, output.FLB_OK, result)
	assert.True(t, checks.appendRows)
	assert.True(t, checks.calledcheckResponses)
	assert.True(t, checks.calledGetContext)
	assert.True(t, checks.appendQueue)
	assert.True(t, checks.createDecoder)
	assert.True(t, checks.gotRecord)
}
