package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
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

// Struct for each stream - one stream per output
type outputConfig struct {
	messageDescriptor protoreflect.MessageDescriptor
	managedStream     *managedwriter.ManagedStream
	client            *managedwriter.Client
	maxChunkSize      int
	appendResults     *[]*managedwriter.AppendResult
	mutex             sync.Mutex
}

var (
	err       error
	ms_ctx    = context.Background()
	configMap = make(map[int]*outputConfig)
	configID  = 0
)

// This function handles getting data on the schema of the table data is being written to. It uses GetWriteStream as well as adapt functions to get the relevant descriptors. The inputs for this function are the context, managed writer client,
// projectID, datasetID, and tableID. getDescriptors returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto(which sends the table schema to the stream when created with
// NewManagedStream as shown in line 54 and 63 of source.go).

func getDescriptors(curr_ctx context.Context, managed_writer_client *managedwriter.Client, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
	//create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	//create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	//call getwritestream to get data on the table
	table_data, err2 := managed_writer_client.GetWriteStream(curr_ctx, &req)
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

// this function is used for asynchronous WriteAPI response checking
// it takes in the relevant queue of responses as well as boolean that indicates whether we should block the AppendRows function
// and wait for the next response from WriteAPI
func checkResponses(curr_ctx context.Context, currQueuePointer *[]*managedwriter.AppendResult, waitForResponse bool, currMutex *sync.Mutex) int {
	(*currMutex).Lock()
	defer (*currMutex).Unlock()
	for len(*currQueuePointer) > 0 {
		queueHead := (*currQueuePointer)[0]
		if waitForResponse {
			recvOffset, err := queueHead.GetResult(curr_ctx)
			*currQueuePointer = (*currQueuePointer)[1:]
			if err != nil {
				log.Fatal("error in checking responses")
				return 1
			}
			log.Printf("Successfully appended data at offset %d.\n", recvOffset)
		} else {
			select {
			case <-queueHead.Ready():
				recvOffset, err := queueHead.GetResult(curr_ctx)
				*currQueuePointer = (*currQueuePointer)[1:]
				if err != nil {
					log.Fatal("error in checking responses")
					return 1
				}
				log.Printf("Successfully appended data at offset %d.\n", recvOffset)
			default:
				return 0
			}
		}

	}
	return 0
}

func sendRequest(ctx context.Context, data [][]byte, config **outputConfig) int {
	if len(data) > 0 {
		(*config).mutex.Lock()
		defer (*config).mutex.Unlock()
		stream, err := (*config).managedStream.AppendRows(ctx, data)
		if err != nil {
			log.Fatal("Stream failed at AppendRows: ", err)
			return 1
		}
		*(*config).appendResults = append(*(*config).appendResults, stream)
	}
	return 0
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
	//set projectID, datasetID, and tableID from config file params
	projectID := output.FLBPluginConfigKey(plugin, "ProjectID")
	datasetID := output.FLBPluginConfigKey(plugin, "DatasetID")
	tableID := output.FLBPluginConfigKey(plugin, "TableID")

	//optional maxchunksize param
	str := output.FLBPluginConfigKey(plugin, "Max_Chunk_Size")
	var maxChunkSize_init int
	if str != "" {
		maxChunkSize_init, err = strconv.Atoi(str)
		if err != nil {
			log.Printf("Invalid Max Chunk Size, defaulting to 9 MB:%v", err)
			maxChunkSize_init = 9 * 1024 * 1024
		}
		if maxChunkSize_init > 9*1024*1024 {
			log.Println("A single call to AppendRows cannot exceed 9 MB.")
			maxChunkSize_init = 9 * 1024 * 1024
		}
	}

	//optional max queue size params
	str1 := output.FLBPluginConfigKey(plugin, "Max_Queue_Requests")
	str2 := output.FLBPluginConfigKey(plugin, "Max_Queue_Bytes")
	var queueSize int
	var queueByteSize int
	if str1 == "" {
		queueSize = 1000
	} else {
		queueSize, err = strconv.Atoi(str1)
		if err != nil {
			log.Printf("Invalid Max Queue Requests, defaulting to 1000:%v", err)
			queueSize = 1000
		}
	}
	if str2 == "" {
		queueByteSize = (100 * 1024 * 1024)
	} else {
		queueByteSize, err = strconv.Atoi(str2)
		if err != nil {
			log.Printf("Invalid Max Queue MB, defaulting to 100 MB:%v", err)
			queueByteSize = (100 * 1024 * 1024)
		}
	}

	//create new client
	client, err := managedwriter.NewClient(ms_ctx, projectID)
	if err != nil {
		log.Fatal(err)
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
		log.Fatal("NewManagedStream: ", err)
		return output.FLB_ERROR
	}

	log.Printf("max byte size: %d, max requests: %d", queueByteSize, queueSize)

	var res_temp []*managedwriter.AppendResult

	// Instantiates stream
	config := outputConfig{
		messageDescriptor: md,
		managedStream:     managedStream,
		client:            client,
		maxChunkSize:      maxChunkSize_init,
		appendResults:     &res_temp,
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
	log.Printf("[multiinstance] Flush called for id: %d", id)

	// Locate stream in map
	// Look up through reference
	config, ok := configMap[id]
	if !ok {
		log.Printf("Skipping flush because config is not found for tag: %d.", id)
		return output.FLB_OK
	}

	responseErr := checkResponses(ms_ctx, config.appendResults, false, &config.mutex)
	if responseErr == 1 {
		log.Fatal("error in checking responses noticed in flush")
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

		row := parseMap(record)

		//serialize data
		//transform each row of data into binary using the json_to_binary function and the message descriptor from the getDescriptors function
		buf, err := json_to_binary(config.messageDescriptor, row)
		if err != nil {
			log.Fatal("converting from json to binary failed: ", err)
			return output.FLB_ERROR
		}

		if (currsize + len(buf)) > config.maxChunkSize {
			// Appending Rows
			res := sendRequest(ms_ctx, binaryData, &config)
			if res != 0 {
				return output.FLB_ERROR
			}

			binaryData = nil
			currsize = 0

		}
		binaryData = append(binaryData, buf)
		currsize += len(buf)

	}
	// Appending Rows
	res := sendRequest(ms_ctx, binaryData, &config)
	if res != 0 {
		return output.FLB_ERROR
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
		return output.FLB_ERROR
	}

	responseErr := checkResponses(ms_ctx, config.appendResults, true, &config.mutex)
	if responseErr == 1 {
		log.Fatal("error in checking responses noticed in flush")
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
