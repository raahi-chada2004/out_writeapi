package main

import (
	"context"
	"log"
	_ "log"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
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
	var checks StreamChecks
	var setID interface{}
	var id int

	mockMS := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			checks.appendRows = true
			return nil, nil
		},
	}

	originalFunc := getWriter
	getWriter = func(client *managedwriter.Client, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
		return mockMS, nil
	}
	defer func() { getWriter = originalFunc }()

	patch1 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		return ""
	})
	defer patch1.Unpatch()

	patchDescriptor := monkey.Patch(getDescriptors, func(curr_ctx context.Context, managed_writer_client *managedwriter.Client, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
		return nil, nil
	})
	defer patchDescriptor.Unpatch()

	patchSetContext := monkey.Patch(output.FLBPluginSetContext, func(plugin unsafe.Pointer, ctx interface{}) {
		log.Println("Ran set context")
		setID = ctx
	})
	defer patchSetContext.Unpatch()

	plugin := unsafe.Pointer(nil)
	// ctx := unsafe.Pointer(nil)
	initRes := FLBPluginInit(plugin)

	orgFunc := getContext
	getContext = func(ctx unsafe.Pointer) int {
		checks.calledGetContext = true
		uintptrValueBack := uintptr(ctx)
		id = int(uintptrValueBack)
		return id
	}
	defer func() { getContext = orgFunc }()

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
		if loopCount%2 == 0 {
			loopCount++
			return 0, nil, nil
		} else {
			loopCount++
			return 1, nil, nil
		}
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

	// Gets config so we can check length of initial results queue
	config := configMap[id]
	initsize := len(*config.appendResults)

	// Converts id (int) to type unsafe.Pointer to be used as the ctx
	intValue := setID.(int)
	uintptrValue := uintptr(intValue)
	pointerValue := unsafe.Pointer(uintptrValue)

	// Calls FlushCtx with this ID
	result := FLBPluginFlushCtx(pointerValue, plugin, 1, nil)
	result = FLBPluginFlushCtx(pointerValue, plugin, 1, nil)

	finsize := len(*config.appendResults)
	if finsize-2 == initsize {
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
