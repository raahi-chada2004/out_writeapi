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
	"C"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Struct for each stream - one stream per output
type StreamConfig struct {
	md            protoreflect.MessageDescriptor
	managedStream *managedwriter.ManagedStream
	client        ManagedWriterClient
	maxChunkSize  int
	results       *[]*managedwriter.AppendResult
	mu            sync.Mutex
}

var (
	err       error
	ms_ctx    = context.Background()
	configMap = make(map[int]*StreamConfig)
	counter   = 0
)

// This function handles getting data on the schema of the table data is being written to.
// getDescriptors returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto

func getDescriptors(curr_ctx context.Context, mw_client ManagedWriterClient, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
	//create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	//create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	//call getwritestream to get data on the table
	table_data, err2 := mw_client.GetWriteStream(curr_ctx, &req)
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

// This function handles the data transformation from JSON to binary for a single json row.
// The outputs of this function are the corresponding binary data as well as any error that occurs.
func jsonToBinary(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
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
// function is used to transform fluent-bit record to a JSON map
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

// this function is used for asynchronous WriteAPI response checking
// it takes in the relevant queue of responses as well as boolean that indicates whether we should block the AppendRows function
// and wait for the next response from WriteAPI
func checkResponses(curr_ctx context.Context, currQueuePointer *[]*managedwriter.AppendResult, waitForResponse bool) (int, error) {
	for len(*currQueuePointer) > 0 {
		queueHead := (*currQueuePointer)[0]
		if waitForResponse {
			recvOffset, err := queueHead.GetResult(curr_ctx)
			*currQueuePointer = (*currQueuePointer)[1:]
			if err != nil {
				log.Printf("Append response error: %s", err)
				return 1, err
			}
			log.Printf("Successfully appended data at offset %d.\n", recvOffset)
		} else {
			select {
			case <-queueHead.Ready():
				recvOffset, err := queueHead.GetResult(curr_ctx)
				*currQueuePointer = (*currQueuePointer)[1:]
				if err != nil {
					log.Printf("Append response error: %s", err)
					return 1, err
				}
				log.Printf("Successfully appended data at offset %d.\n", recvOffset)
			default:
				return 0, nil
			}
		}

	}
	return 0, nil
}

func getConfigField(plugin unsafe.Pointer, key string, errmsg string, defaultval int) int {
	currstr := output.FLBPluginConfigKey(plugin, key)
	var finval int
	if currstr == "" {
		finval = defaultval
	} else {
		finval, err = strconv.Atoi(currstr)
		if err != nil {
			log.Printf("%s: %s", errmsg, err)
			finval = defaultval
		}
	}
	return finval
}

type ManagedWriterClient interface {
	NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	Close() error
}

type realManagedWriterClient struct {
	currClient *managedwriter.Client
}

func (r *realManagedWriterClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
	return r.currClient.NewManagedStream(ctx, opts...)
}

func (r *realManagedWriterClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.GetWriteStream(ctx, req, opts...)
}

func (r *realManagedWriterClient) Close() error {
	return r.currClient.Close()
}

var getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
	client, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &realManagedWriterClient{currClient: client}, nil
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "writeapi", "Sends data to BigQuery through WriteAPI")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	//set projectID, datasetID, and tableID from config file params
	projectID := output.FLBPluginConfigKey(plugin, "ProjectID")
	datasetID := output.FLBPluginConfigKey(plugin, "DatasetID")
	tableID := output.FLBPluginConfigKey(plugin, "TableID")

	//optional maxchunksize param
	maxChunkSize_init := getConfigField(plugin, "Max_Chunk_Size", "Invalid Max Chunk Size, defaulting to 9 MB", (9 * 1024 * 1024))
	if maxChunkSize_init > 9*1024*1024 {
		log.Println("A single call to AppendRows cannot exceed 9 MB.")
		maxChunkSize_init = 9 * 1024 * 1024
	}

	//optional max queue size params
	queueSize := getConfigField(plugin, "Max_Queue_Requests", "Invalid Max Queue Requests, defaulting to 1000", 1000)
	queueByteSize := getConfigField(plugin, "Max_Queue_Bytes", "Invalid Max Queue Bytes, defaulting to 100 MB", (100 * 1024 * 1024))

	//create new client
	client, err := getClient(ms_ctx, projectID)
	if err != nil {
		log.Fatalf("getClient failed in FLBPlugininit: %s", err)
		return output.FLB_ERROR
	}

	//use getDescriptors to get the message descriptor, and descriptor proto
	md, descriptor := getDescriptors(ms_ctx, client, projectID, datasetID, tableID)

	// streamname
	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	// Create stream using NewManagedStream
	managedStream, err := client.NewManagedStream(ms_ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		//use the descriptor proto when creating the new managed stream
		managedwriter.WithSchemaDescriptor(descriptor),
		managedwriter.EnableWriteRetries(true),
		managedwriter.WithMaxInflightBytes(queueByteSize),
		managedwriter.WithMaxInflightRequests(queueSize),
	)
	if err != nil {
		log.Fatal("NewManagedStream failed in FLBPluginInit: ", err)
		return output.FLB_ERROR
	}

	var res_temp []*managedwriter.AppendResult

	// Instantiates stream
	config := StreamConfig{
		md:            md,
		managedStream: managedStream,
		client:        client,
		maxChunkSize:  maxChunkSize_init,
		results:       &res_temp,
	}

	configMap[counter] = &config

	// Creating FLB context for each output, enables multiinstancing
	config.mu.Lock()
	output.FLBPluginSetContext(plugin, counter)
	counter = counter + 1
	config.mu.Unlock()

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Print("[multiinstance] Flush called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Get Fluentbit Context
	id := output.FLBPluginGetContext(ctx).(int)
	log.Printf("[multiinstance] Flush called for id: %d", id)

	// Locate stream in map
	// Look up through reference
	config, ok := configMap[id]
	if !ok {
		log.Printf("Skipping flush because config is not found for tag: %d.", id)
		return output.FLB_ERROR
	}

	responseErr, currerr := checkResponses(ms_ctx, config.results, false)
	if responseErr != 0 {
		log.Fatalf("error in checking responses noticed in flush:%s", currerr)
		return output.FLB_ERROR
	}

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

		rowJsonMap := parseMap(record)

		//serialize data

		//transform each row of data into binary using the jsonToBinary function and the message descriptor from the getDescriptors function
		buf, err := jsonToBinary(config.md, rowJsonMap)
		if err != nil {
			log.Fatal("converting from json to binary failed: ", err)
			return output.FLB_ERROR
		}

		if ((currsize + len(buf)) >= config.maxChunkSize) && len(binaryData) != 0 {
			// Appending Rows
			asyncResult, err := config.managedStream.AppendRows(ms_ctx, binaryData)
			if err != nil {
				log.Fatal("AppendRows: ", err)
				return output.FLB_ERROR
			}
			*config.results = append(*config.results, asyncResult)

			binaryData = nil
			currsize = 0

		}
		binaryData = append(binaryData, buf)
		currsize += (len(buf) + 2)

	}

	if len(binaryData) > 0 {
		// Appending Rows
		asyncResult, err := config.managedStream.AppendRows(ms_ctx, binaryData)
		if err != nil {
			log.Fatal("AppendRows: ", err)
			return output.FLB_ERROR
		}
		*config.results = append(*config.results, asyncResult)
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[multiinstance] Exit called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	// Get context
	id := output.FLBPluginGetContext(ctx).(int)
	log.Printf("[multiinstance] Flush called for id: %d", id)

	// Locate stream in map
	config, ok := configMap[id]
	if !ok {
		log.Printf("Skipping flush because config is not found for tag: %d.", id)
		return output.FLB_OK
	}

	responseErr, currerr := checkResponses(ms_ctx, config.results, true)
	if responseErr != 0 {
		log.Fatalf("error in checking responses noticed in exit:%s", currerr)
		return output.FLB_ERROR
	}

	if config.managedStream != nil {
		if err = config.managedStream.Close(); err != nil {
			log.Printf("Couldn't close managed stream:%v", err)
			return output.FLB_ERROR
		}
	}

	if config.client != nil {
		if err = config.client.Close(); err != nil {
			log.Printf("Couldn't close managed writer client:%v", err)
			return output.FLB_ERROR
		}
	}

	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[multiinstance] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}
