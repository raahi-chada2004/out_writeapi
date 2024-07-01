package main

import (
	"log"
	"reflect"
	"testing"
	"unsafe"

	"bou.ke/monkey"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"

	"context"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
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

// this is a struct keeping track of whether the correct options are sent in NewManagedStream
type OptionChecks struct {
	*managedwriter.ManagedStream
	correctStreamType          bool
	correctTableReference      bool
	correctDescriptor          bool
	correctEnableWriteRetries  bool
	correctMaxInflightBytes    bool
	correctMaxInflightRequests bool
}

// func createMockPluginContext() unsafe.Pointer {
// 	mockConfig := map[string]string{
// 		"ProjectID":          "bigquerytestdefault",
// 		"DatasetID":          "siddag_summer2024",
// 		"TableID":            "raahi_summer2024table1",
// 		"Max_Chunk_Size":     "1048576",
// 		"Max_Queue_Requests": "100",
// 		"Max_Queue_Bytes":    "52428800",
// 	}
// 	return unsafe.Pointer(&mockConfig)
// }

// this tests the FLBPluginInit method
func TestFLBPluginInit(t *testing.T) {
	//Mock the managedwriter.NewClient with an empty client
	patch1 := monkey.Patch(managedwriter.NewClient, func(ctx context.Context, projectID string, opts ...option.ClientOption) (*managedwriter.Client, error) {
		log.Println("Mock NewClient called")
		return &managedwriter.Client{}, nil
	})
	defer patch1.Unpatch()

	patch2 := monkey.Patch(output.FLBPluginConfigKey, func(plugin unsafe.Pointer, key string) string {
		log.Println("Mock output.FLBPluginConfigKey called")
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
	defer patch2.Unpatch()

	patch3 := monkey.PatchInstanceMethod(reflect.TypeOf(&managedwriter.Client{}), "GetWriteStream", func(m *managedwriter.Client, ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
		log.Println("Mock GetWriteStream called")
		return &storagepb.WriteStream{
			Name: "mockstream",
			TableSchema: &storagepb.TableSchema{
				Fields: []*storagepb.TableFieldSchema{
					{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		}, nil
	})

	defer patch3.Unpatch()

	currTableReference := "projects/bigquerytestdefault/datasets/siddag_summer2024/tables/raahi_summer2024table1"
	SchemaDescriptor := &descriptorpb.DescriptorProto{
		Name: proto.String("root"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("Time"), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
			{Name: proto.String("Text"), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
		},
	}

	var currChecks OptionChecks
	patch4 := monkey.PatchInstanceMethod(reflect.TypeOf(&managedwriter.Client{}), "NewManagedStream", func(m *managedwriter.Client, ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
		log.Println("Mock NewManagedStream called")
		for _, opt := range opts {

			if reflect.ValueOf(opt).Pointer() == reflect.ValueOf(managedwriter.WithType(managedwriter.DefaultStream)).Pointer() {
				currChecks.correctStreamType = true
			} else if reflect.ValueOf(opt).Pointer() == reflect.ValueOf(managedwriter.WithDestinationTable(currTableReference)).Pointer() {
				currChecks.correctTableReference = true
			} else if reflect.ValueOf(opt).Pointer() == reflect.ValueOf(managedwriter.WithSchemaDescriptor(SchemaDescriptor)).Pointer() {
				currChecks.correctDescriptor = true
			} else if reflect.ValueOf(opt).Pointer() == reflect.ValueOf(managedwriter.EnableWriteRetries(true)).Pointer() {
				currChecks.correctEnableWriteRetries = true
			} else if reflect.ValueOf(opt).Pointer() == reflect.ValueOf(managedwriter.WithMaxInflightBytes(52428800)).Pointer() {
				currChecks.correctMaxInflightBytes = true
			} else if reflect.ValueOf(opt).Pointer() == reflect.ValueOf(managedwriter.WithMaxInflightRequests(100)).Pointer() {
				currChecks.correctMaxInflightRequests = true
			}
		}
		return &managedwriter.ManagedStream{}, nil
	})

	defer patch4.Unpatch()

	plugin := unsafe.Pointer(nil)
	result := FLBPluginInit(plugin)
	assert.Equal(t, output.FLB_OK, result)
	assert.True(t, currChecks.correctDescriptor)
	assert.True(t, currChecks.correctEnableWriteRetries)
	assert.True(t, currChecks.correctMaxInflightBytes)
	assert.True(t, currChecks.correctMaxInflightRequests)
	assert.True(t, currChecks.correctStreamType)
	assert.True(t, currChecks.correctTableReference)

}
