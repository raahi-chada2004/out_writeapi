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
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
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
type outputConfig struct {
	messageDescriptor protoreflect.MessageDescriptor
	managedStream     *managedwriter.ManagedStream
	client            ManagedWriterClient
	maxChunkSize      int
	appendResults     *[]*managedwriter.AppendResult
	mutex             sync.Mutex
	exactlyOnce       bool
	offsetCounter     int64
	numRetries        int
}

var (
	ms_ctx    = context.Background()
	configMap = make(map[int]*outputConfig)
	configID  = 0
)

const (
	chunkSizeLimit      = 9 * 1024 * 1024
	queueRequestDefault = 1000
	queueByteDefault    = 100 * 1024 * 1024
	exactlyOnceDefault  = false
	numRetriesDefault   = 4
)

// This function handles getting data on the schema of the table data is being written to.
// getDescriptors returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto
func getDescriptors(curr_ctx context.Context, mw_client ManagedWriterClient, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	//create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	//create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	//call getwritestream to get data on the table
	table_data, err := mw_client.GetWriteStream(curr_ctx, &req)
	if err != nil {
		return nil, nil, err
	}
	//get the schema from table data
	table_schema := table_data.GetTableSchema()
	//storage schema ->proto descriptor
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
	if err != nil {
		return nil, nil, err
	}
	//proto descriptor -> messageDescriptor
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, errors.New("Message descriptor could not be created from table's proto descriptor")
	}

	//messageDescriptor -> descriptor proto
	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, err
	}

	return messageDescriptor, dp, nil
}

// This function handles the data transformation from JSON to binary for a single json row.
// The outputs of this function are the corresponding binary data as well as any error that occurs.
func jsonToBinary(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
	//JSON map -> JSON byte
	row, err := json.Marshal(jsonRow)
	if err != nil {
		return nil, err
	}
	//create empty message
	message := dynamicpb.NewMessage(message_descriptor)

	// First, json->proto message
	err = protojson.Unmarshal(row, message)
	if err != nil {
		return nil, err
	}

	// Then, proto message -> bytes.
	b, err := proto.Marshal(message)
	if err != nil {
		return nil, err
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
// This function returns an error which is nil if the reponses were checked successfully and populated any were unsuccesful
func checkResponses(curr_ctx context.Context, currQueuePointer *[]*managedwriter.AppendResult, waitForResponse bool, currMutex *sync.Mutex, exactlyOnceConf bool) error {
	(*currMutex).Lock()
	defer (*currMutex).Unlock()
	for len(*currQueuePointer) > 0 {
		if exactlyOnceConf {
			return errors.New("Asynchronous response queue has non-zero size when exactly-once is configured")
		}
		queueHead := (*currQueuePointer)[0]
		if waitForResponse {
			_, err := queueHead.GetResult(curr_ctx)
			*currQueuePointer = (*currQueuePointer)[1:]
			if err != nil {
				return err
			}
		} else {
			select {
			case <-queueHead.Ready():
				_, err := queueHead.GetResult(curr_ctx)
				*currQueuePointer = (*currQueuePointer)[1:]
				if err != nil {
					return err
				}
			default:
				return nil
			}
		}

	}
	return nil
}

// this function gets the value of various configuration fields and returns an error if the field could not be parsed
func getConfigField[T int | bool](plugin unsafe.Pointer, key string, defaultval T) (T, error) {
	currstr := output.FLBPluginConfigKey(plugin, key)
	finval := defaultval
	if currstr != "" {
		switch any(defaultval).(type) {
		case int:
			intval, err := strconv.Atoi(currstr)
			if err != nil {
				return defaultval, err
			} else {
				finval = any(intval).(T)
			}
		case bool:
			boolval, err := strconv.ParseBool(currstr)
			if err != nil {
				return defaultval, err
			} else {
				finval = any(boolval).(T)
			}
		}

	}
	return finval, nil
}

// this function sends and checks the responses for data through a committed stream with exactly once functionality
func sendRequestExactlyOnce(ctx context.Context, data [][]byte, config **outputConfig) error {
	(*config).mutex.Lock()
	defer (*config).mutex.Unlock()

	appendResult, err := (*config).managedStream.AppendRows(ctx, data, managedwriter.WithOffset((*config).offsetCounter))
	if err != nil {
		return err
	}
	//synchronously check the response immediately after appending data with exactly once semantics
	_, err = appendResult.GetResult(ctx)
	if err != nil {
		return err
	}
	return nil
}

func sendRequestRetries(ctx context.Context, data [][]byte, config **outputConfig) error {
	retryer := newStatelessRetryer((*config).numRetries)
	attempt := 0
	for {
		err := sendRequestExactlyOnce(ctx, data, config)
		if err == nil {
			break
		}
		backoffPeriod, shouldRetry := retryer.Retry(err, attempt)
		if !shouldRetry {
			return err
		}
		attempt++
		time.Sleep(backoffPeriod)

	}
	return nil
}

// this function sends data and appends the responses to a queue to be checked asynchronously through a default stream with at least once functionality
func sendRequestDefault(ctx context.Context, data [][]byte, config **outputConfig) error {
	(*config).mutex.Lock()
	defer (*config).mutex.Unlock()

	appendResult, err := (*config).managedStream.AppendRows(ctx, data)
	if err != nil {
		return err
	}

	*(*config).appendResults = append(*(*config).appendResults, appendResult)
	return nil
}

// this function cases on the exactly/at-least once functionality and sends the data accordingly
func sendRequest(ctx context.Context, data [][]byte, config **outputConfig) error {
	if len(data) > 0 {
		if (*config).exactlyOnce {
			return sendRequestRetries(ctx, data, config)
		} else {
			return sendRequestDefault(ctx, data, config)
		}
	}
	return nil
}

// this is a test-only method that provides the instance count for configMap
func getInstanceCount() int {
	return len(configMap)
}

// this interface acts as a wrapper for the *managedwriter.Client type which the realManagedWriterClient struct implements
// with its actual methods.
type ManagedWriterClient interface {
	NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	Close() error
	BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error)
	CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
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

func (r *realManagedWriterClient) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	return r.currClient.BatchCommitWriteStreams(ctx, req, opts...)
}

func (r *realManagedWriterClient) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.CreateWriteStream(ctx, req, opts...)
}

// this function acts as a wrapper for the managedwriter.NewClient function in order to inject a mock interface, one can
// override the getClient method to return a different struct type (that still implements the managedwriterclient interface)
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

	//set exactly-once bool from config file param
	exactlyOnceVal, err := getConfigField(plugin, "Exactly_Once", exactlyOnceDefault)
	if err != nil {
		log.Printf("Invalid Exactly_Once parameter in configuration file: %s", err)
	}

	//optional num synchronous retries parameter
	numRetriesVal, err := getConfigField(plugin, "Num_Synchronous_Retries", numRetriesDefault)
	if err != nil {
		log.Printf("Invalid Num_Synchronous_Retries parameter in configuration file: %s", err)
	}

	//optional maxchunksize param
	maxChunkSize_init, err := getConfigField(plugin, "Max_Chunk_Size", chunkSizeLimit)
	if err != nil {
		log.Printf("Invalid Max_Chunk_Size parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}
	if maxChunkSize_init > chunkSizeLimit {
		log.Printf("Max_Chunk_Size was set to: %d, but a single call to AppendRows cannot exceed 9 MB. Defaulting to 9 MB", maxChunkSize_init)
		maxChunkSize_init = chunkSizeLimit
	}

	//optional max queue size params
	queueSize, err := getConfigField(plugin, "Max_Queue_Requests", queueRequestDefault)
	if err != nil {
		log.Printf("Invalid Max_Queue_Requests parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}
	queueByteSize, err := getConfigField(plugin, "Max_Queue_Bytes", queueByteDefault)
	if err != nil {
		log.Printf("Invalid Max_Queue_Bytes parameter in configuration file: %s", err)
		return output.FLB_ERROR
	}
	//create new client
	client, err := getClient(ms_ctx, projectID)
	if err != nil {
		log.Printf("Creating a new managed BigQuery Storage write client scoped to: %s failed in FLBPluginInit: %s", projectID, err)
		return output.FLB_ERROR
	}

	// streamname
	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	//use getDescriptors to get the message descriptor, and descriptor proto
	md, descriptor, err := getDescriptors(ms_ctx, client, projectID, datasetID, tableID)
	if err != nil {
		log.Printf("Getting message descriptor and descriptor proto for table: %s failed in FLBPluginInit: %s", tableReference, err)
		return output.FLB_ERROR
	}

	//set the stream type based on exactly once parameter
	var currStreamType managedwriter.StreamType
	var enableRetries bool
	if exactlyOnceVal {
		currStreamType = managedwriter.CommittedStream
	} else {
		currStreamType = managedwriter.DefaultStream
		enableRetries = true
	}

	// Create stream using NewManagedStream
	managedStream, err := client.NewManagedStream(ms_ctx,
		managedwriter.WithType(currStreamType),
		managedwriter.WithDestinationTable(tableReference),
		//use the descriptor proto when creating the new managed stream
		managedwriter.WithSchemaDescriptor(descriptor),
		managedwriter.EnableWriteRetries(enableRetries),
		managedwriter.WithMaxInflightBytes(queueByteSize),
		managedwriter.WithMaxInflightRequests(queueSize),
	)
	if err != nil {
		log.Printf("Creating a new managed stream with destination table: %s failed in FLBPluginInit: %s", tableReference, err)
		return output.FLB_ERROR
	}

	var res_temp []*managedwriter.AppendResult

	// Instantiates stream
	config := outputConfig{
		messageDescriptor: md,
		managedStream:     managedStream,
		client:            client,
		maxChunkSize:      maxChunkSize_init,
		appendResults:     &res_temp,
		exactlyOnce:       exactlyOnceVal,
		offsetCounter:     0,
		numRetries:        numRetriesVal,
	}

	configMap[configID] = &config

	// Creating FLB context for each output, enables multiinstancing
	config.mutex.Lock()
	output.FLBPluginSetContext(plugin, configID)
	configID = configID + 1
	config.mutex.Unlock()

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
	// Locate stream in map
	// Look up through reference
	config, ok := configMap[id]
	if !ok {
		log.Printf("Finding configuration for output instance with id: %d failed in FLBPluginFlushCtx", id)
		return output.FLB_ERROR
	}

	responseErr := checkResponses(ms_ctx, config.appendResults, false, &config.mutex, config.exactlyOnce)
	if responseErr != nil {
		log.Printf("Checking append responses for output instance with id: %d failed in FLBPluginFlushCtx: %s", id, responseErr)
		return output.FLB_ERROR
	}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	var binaryData [][]byte
	var currsize int
	//keeps track of the number of rows previously sent
	var rowCounter int64

	// Iterate Records
	for {
		// Extract Record
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		rowJSONMap := parseMap(record)

		//serialize data
		//transform each row of data into binary using the jsonToBinary function and the message descriptor from the getDescriptors function
		buf, err := jsonToBinary(config.messageDescriptor, rowJSONMap)
		if err != nil {
			log.Printf("Transforming records from JSON to binary data for output instance with id: %d failed in FLBPluginFlushCtx: %s", id, err)
			return output.FLB_ERROR
		}

		if (currsize + len(buf)) >= config.maxChunkSize {
			// Appending Rows
			err := sendRequest(ms_ctx, binaryData, &config)
			if err != nil {
				log.Printf("Appending data for output instance with id: %d failed in FLBPluginFlushCtx: %s", id, err)
				return output.FLB_ERROR
			}

			config.offsetCounter += rowCounter
			rowCounter = 0

			binaryData = nil
			currsize = 0

		}
		binaryData = append(binaryData, buf)
		//include the protobuf overhead to the currsize variable
		currsize += (len(buf) + 2)
		rowCounter++

	}
	// Appending Rows
	err := sendRequest(ms_ctx, binaryData, &config)
	if err != nil {
		log.Printf("Appending data for output instance with id: %d failed in FLBPluginFlushCtx: %s", id, err)
		return output.FLB_ERROR
	}

	config.offsetCounter += rowCounter

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

	// Locate stream in map
	config, ok := configMap[id]
	if !ok {
		log.Printf("Finding configuration for output instance with id: %d failed in FLBPluginExitCtx", id)
		return output.FLB_ERROR
	}

	responseErr := checkResponses(ms_ctx, config.appendResults, true, &config.mutex, config.exactlyOnce)
	if responseErr != nil {
		log.Printf("Checking append responses for output instance with id: %d failed in FLBPluginExitCtx: %s", id, responseErr)
		return output.FLB_ERROR
	}

	if config.managedStream != nil {
		if config.exactlyOnce {
			if _, err := config.managedStream.Finalize(ms_ctx); err != nil {
				log.Printf("Finalizing managed stream for output instance with id: %d failed in FLBPluginExit: %s", id, err)
			}
		}
		if err := config.managedStream.Close(); err != nil {
			log.Printf("Closing managed stream for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
			return output.FLB_ERROR
		}
	}

	if config.client != nil {
		if err := config.client.Close(); err != nil {
			log.Printf("Closing managed writer client for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
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
