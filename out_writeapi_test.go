package main

import (
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/stretchr/testify/assert"
)

type MockFLBPlugin struct {
	name string
	desc string
}

// this function checks if the name parameter in FLBPluginRegister is always "writeapi"
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

	assert.Equal(t, 0, result, "Expected result to be 1")
	assert.Equal(t, "writeapi", currplugin.name, "Expected name to be writeapi, got %s", currplugin.name)

}
