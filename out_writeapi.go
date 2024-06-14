//go:build plugin

package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/fluent/fluent-bit-go/output"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)
import (
	"strconv"
)

var (
	err           error
	ctx           context.Context
	client        *managedwriter.Client
	projectID     string
	datasetID     string
	tableID       string
	md            protoreflect.MessageDescriptor
	managedStream *managedwriter.ManagedStream
	results       []*managedwriter.AppendResult
	maxChunkSize  = float64(9)
)

// This function handles getting data on the schema of the table data is being written to. It uses GetWriteStream as well as adapt functions to get the relevant descriptors. The inputs for this function are the context, managed writer client,
// projectID, datasetID, and tableID. getDescriptors returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto(which sends the table schema to the stream when created with
// NewManagedStream as shown in line 54 and 63 of source.go).

func getDescriptors(ctx context.Context, managed_writer_client *managedwriter.Client, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
	//create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	//create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	//call getwritestream to get data on the table
	table_data, err2 := managed_writer_client.GetWriteStream(ctx, &req)
	if err2 != nil {
		log.Fatalf("getWriteStream command failed: %v", err2)
	}
	//get the schema from table data
	table_schema := table_data.GetTableSchema()
	//storage schema ->proto descriptor
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
	if err != nil {
		log.Fatalf("adapt.StorageSchemaToDescriptor: %v", err)
	}
	//proto descriptor -> messageDescriptor
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		log.Fatalf("adapted descriptor is not a message descriptor")
	}

	//messageDescriptor -> descriptor proto
	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		log.Fatalf("NormalizeDescriptor: %v", err)
	}

	return messageDescriptor, dp
}

// This function handles the data transformation from JSON to binary for a single json row. In practice, this function would be utilized within a loop to transform all of the json data. The inputs are the message descriptor
//(which was returned in getDescriptors) as well as the relevant jsonRow (of type map[string]interface{} - which is the output of unmarshalling the json data). The outputs are the corresponding binary data as well as any error that occurs.
//Various json, protojson, and proto marshalling/unmarshalling functions are utilized to transform the data. The message descriptor is used when creating an empty new proto message (which the data is placed into).

func json_to_binary(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
	//JSON map -> JSON byte
	row, err := json.Marshal(jsonRow)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json map: %w", err)
	}
	//create empty message
	message := dynamicpb.NewMessage(message_descriptor)

	// First, json->proto message
	err = protojson.Unmarshal(row, message)
	if err != nil {
		return nil, fmt.Errorf("failed to Unmarshal json message: %w", err)
	}

	// Then, proto message -> bytes.
	b, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto bytes: %w ", err)
	}

	return b, nil
}

// from https://github.com/majst01/fluent-bit-go-redis-output.git
// function is used to transform pluent-bit record to a JSON map
func parseMap(mapInterface map[interface{}]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			// prevent encoding to base64
			m[k.(string)] = string(t)
		case map[interface{}]interface{}:
			m[k.(string)] = parseMap(t)
		default:
			m[k.(string)] = v
		}
	}
	return m
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "writeapi", "Sends data to BigQuery through WriteAPI")
}

// (fluentbit will call this)
// plugin (context) pointer to fluentbit context (state/ c code)
//
//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	//create context
	ctx = context.Background()

	//set projectID, datasetID, and tableID from config file params
	projectID = output.FLBPluginConfigKey(plugin, "ProjectID")
	datasetID = output.FLBPluginConfigKey(plugin, "DatasetID")
	tableID = output.FLBPluginConfigKey(plugin, "TableID")

	//optional maxchunksize param
	str := output.FLBPluginConfigKey(plugin, "Max_Chunk_Size")
	if str != "" {
		maxChunkSize, err = strconv.ParseFloat(str, 64)
		if err != nil {
			log.Printf("Invalid Max Chunk Size, defaulting to 9:%v", err)
			maxChunkSize = 9
		}
	}

	//create new client
	client, err = managedwriter.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
		return output.FLB_ERROR
	}

	//use getDescriptors to get the message descriptor, and descriptor proto
	var descriptor *descriptorpb.DescriptorProto
	md, descriptor = getDescriptors(ctx, client, projectID, datasetID, tableID)

	// streamname
	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	// Create stream using NewManagedStream
	managedStream, err = client.NewManagedStream(ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		//use the descriptor proto when creating the new managed stream
		managedwriter.WithSchemaDescriptor(descriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		log.Fatal("NewManagedStream: ", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	var binaryData [][]byte
	var currsize int
	// Iterate Records
	for {
		// Extract Record
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		row := parseMap(record)

		//serialize data

		//transform each row of data into binary using the json_to_binary function and the message descriptor from the getDescriptors function
		buf, err := json_to_binary(md, row)
		if err != nil {
			log.Fatal("converting from json to binary failed: ", err)
			return output.FLB_ERROR
		}

		if float64(currsize+len(buf)) > float64(maxChunkSize*1024*1024) {
			// Appending Rows
			stream, err := managedStream.AppendRows(ctx, binaryData)
			if err != nil {
				log.Fatal("AppendRows: ", err)
				return output.FLB_ERROR
			}
			results = append(results, stream)

			log.Println("Done")

			binaryData = nil
			currsize = 0

		}
		binaryData = append(binaryData, buf)
		currsize += len(buf)

	}

	if len(binaryData) > 0 {
		// Appending Rows
		stream, err := managedStream.AppendRows(ctx, binaryData)
		if err != nil {
			log.Fatal("AppendRows: ", err)
			return output.FLB_ERROR
		}
		results = append(results, stream)

		log.Println("Done")
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	if managedStream != nil {
		if err = managedStream.Close(); err != nil {
			log.Printf("Couldn't close managed stream:%v", err)
			return output.FLB_ERROR
		}
	}

	if client != nil {
		if err = client.Close(); err != nil {
			log.Printf("Couldn't close managed writer client:%v", err)
			return output.FLB_ERROR
		}
	}

	// Checks if all results were successful
	for k, v := range results {
		// GetResult blocks until we receive a response from the API.
		recvOffset, err := v.GetResult(ctx)
		if err != nil {
			log.Fatalf("append %d returned error: %v", k, err)
			return output.FLB_ERROR
		}
		log.Printf("Successfully appended data at offset %d.\n", recvOffset)
	}

	return output.FLB_OK
}

func main() {
}
