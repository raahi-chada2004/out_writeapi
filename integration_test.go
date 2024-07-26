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
	"context"
	"encoding/json"
	"log"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

const (
	logFileName    = "logfile.log"
	logFilePath    = "./" + logFileName
	configFilePath = "./fluent-bit.conf"
	numRowsData    = 10
)

// integration test validates the end-to-end fluentbit and bigquery pipeline
func TestPipeline(t *testing.T) {

	ctx := context.Background()

	//get projectID from environment
	projectID := os.Getenv("ProjectID")
	if projectID == "" {
		t.Fatal("Environment variable 'ProjectID' is required to run this test, but not set currently")
	}

	// Set up BigQuery client
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer client.Close()

	// Create a random dataset and table name
	datasetHash := randString()
	datasetID := "testdataset_" + datasetHash

	tableHash := randString()
	tableID := "testtable_" + tableHash

	// Create BigQuery dataset and table in an existing project
	tableSchema := bigquery.Schema{
		{Name: "StringField", Type: bigquery.StringFieldType},
		{Name: "BytesField", Type: bigquery.BytesFieldType},
		{Name: "IntegerField", Type: bigquery.IntegerFieldType},
		{Name: "FloatField", Type: bigquery.FloatFieldType},
		{Name: "NumericField", Type: bigquery.NumericFieldType},
		{Name: "BigNumericField", Type: bigquery.BigNumericFieldType},
		{Name: "BooleanField", Type: bigquery.BooleanFieldType},
		{Name: "TimestampField", Type: bigquery.TimestampFieldType},
		{Name: "DateField", Type: bigquery.DateFieldType},
		{Name: "TimeField", Type: bigquery.TimeFieldType},
		{Name: "DateTimeField", Type: bigquery.DateTimeFieldType},
		{Name: "GeographyField", Type: bigquery.GeographyFieldType},
		{Name: "RecordField", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "SubField1", Type: bigquery.StringFieldType},
			{Name: "SubField2", Type: bigquery.NumericFieldType},
		}},
		//when DateTime is the range element type, must send civil-time encoded int64 datetime data (as documented)
		{Name: "RangeField", Type: bigquery.RangeFieldType, RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateTimeFieldType}},
		{Name: "JSONField", Type: bigquery.JSONFieldType},
	}
	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		t.Fatalf("Failed to create BigQuery dataset %v", err)
	}
	table := dataset.Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: tableSchema}); err != nil {
		t.Fatalf("Failed to create BigQuery table: %v", err)
	}

	//Create config file with random table name
	if err := createConfigFile(projectID, datasetID, tableID, "false"); err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}

	// Create log file
	file, err := os.Create(logFilePath)
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	defer file.Close()

	// Start Fluent Bit with the config file
	FBcmd := exec.Command("fluent-bit", "-c", "fluent-bit.conf")
	if err := FBcmd.Start(); err != nil {
		t.Fatalf("Failed to start Fluent Bit: %v", err)
	}

	//Wait for fluent-bit connection to generate data; add delays before ending fluent-bit process
	time.Sleep(2 * time.Second)
	if err := generateData(numRowsData); err != nil {
		t.Fatalf("Failed to generate data: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Stop Fluent Bit
	if err := FBcmd.Process.Kill(); err != nil {
		t.Fatalf("Failed to stop Fluent Bit: %v", err)
	}

	// Verify data in BigQuery by querying
	queryMsg := "SELECT * FROM `" + projectID + "." + datasetID + "." + tableID + "`"
	BQquery := client.Query(queryMsg)
	BQdata, err := BQquery.Read(ctx)
	if err != nil {
		t.Fatalf("Failed to query data information BigQuery: %v", err)
	}

	rowCount := 0
	for {
		//Check that the data sent is correct
		var BQvalues []bigquery.Value
		err := BQdata.Next(&BQvalues)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read query results: %v", err)
		}

		//Verify the value of the data
		assert.Equal(t, "hello world", BQvalues[0])
		assert.Equal(t, []byte("hello bytes"), BQvalues[1])
		assert.Equal(t, int64(123), BQvalues[2])
		assert.Equal(t, float64(123.45), BQvalues[3])
		assert.Equal(t, big.NewRat(12345, 100), (BQvalues[4].(*big.Rat)))
		assert.Equal(t, big.NewRat(123456789123456789, 1000000000), BQvalues[5].(*big.Rat))
		assert.Equal(t, true, BQvalues[6])
		assert.Equal(t, time.Date(2024, time.July, 26, 6, 8, 46, 0, time.UTC), BQvalues[7])
		assert.Equal(t, civil.Date{Year: 2024, Month: 7, Day: 25}, BQvalues[8].(civil.Date))
		assert.Equal(t, civil.Time{Hour: 12, Minute: 34, Second: 56}, BQvalues[9].(civil.Time))
		assert.Equal(t, civil.DateTime{Date: civil.Date{Year: 2024, Month: 7, Day: 26}, Time: civil.Time{Hour: 12, Minute: 30, Second: 0, Nanosecond: 450000000}}, BQvalues[10].(civil.DateTime))
		assert.Equal(t, "POINT(1 2)", BQvalues[11].(string))
		assert.Equal(t, "sub field value", BQvalues[12].([]bigquery.Value)[0])
		assert.Equal(t, big.NewRat(456, 10), (BQvalues[12].([]bigquery.Value)[1]).(*big.Rat))
		assert.Equal(t, &bigquery.RangeValue{Start: civil.DateTime{Date: civil.Date{Year: 1987, Month: 1, Day: 23}, Time: civil.Time{Hour: 12, Minute: 34, Second: 56, Nanosecond: 789012000}}, End: civil.DateTime{Date: civil.Date{Year: 1987, Month: 1, Day: 23}, Time: civil.Time{Hour: 12, Minute: 34, Second: 56, Nanosecond: 789013000}}}, BQvalues[13])
		assert.Equal(t, "{\"age\":28,\"name\":\"Jane Doe\"}", BQvalues[14].(string))

		rowCount++
	}

	// Verify the number of rows
	assert.Equal(t, numRowsData, rowCount)

	// Clean up - delete the BigQuery dataset and its contents(includes generated table)
	if err := dataset.DeleteWithContents(ctx); err != nil {
		t.Fatalf("Failed to delete BigQuery table: %v", err)
	}

	// Clean up - delete the log file
	if err := os.Remove(logFilePath); err != nil {
		t.Fatalf("Failed to delete log file: %v", err)
	}

	// Clean up - delete the config file
	if err := os.Remove(configFilePath); err != nil {
		t.Fatalf("Failed to delete log file: %v", err)
	}
}

// generate a random string with length 10 (to act as a hash for the table name)
func randString() string {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	charset := "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM0123456789"
	str := make([]byte, 10)
	for i := range str {
		str[i] = charset[rng.Intn(len(charset))]
	}
	return string(str)
}

// template for the config file - dynamically update TableId field
const configTemplate = `
[SERVICE]
    Daemon          off
    Log_Level       error
    Parsers_File    ./jsonparser.conf
    plugins_file    ./plugins.conf
[INPUT]
    Name                               tail
    Path                               {{.CurrLogfilePath}}
    Parser                             json
    Tag                                hello_world
[OUTPUT]
    Name                               writeapi
    Match                              hello_world
    ProjectId                          {{.CurrProjectName}}
    DatasetId                          {{.CurrDatasetName}}
    TableId                            {{.CurrTableName}}
    Exactly_Once                       {{.CurrExactlyOnce}}
`

// struct for dynamically updating config file
type Config struct {
	CurrLogfilePath string
	CurrProjectName string
	CurrDatasetName string
	CurrTableName   string
	CurrExactlyOnce string
}

// function creates configuration file with the input as the TableId field
func createConfigFile(currProjectID string, currDatasetID string, currTableID string, currExactlyOnceVal string) error {
	//struct with the TableId
	config := Config{
		CurrLogfilePath: logFilePath,
		CurrProjectName: currProjectID,
		CurrDatasetName: currDatasetID,
		CurrTableName:   currTableID,
		CurrExactlyOnce: currExactlyOnceVal,
	}

	//create a new template with the format of configTemplate
	tmpl, err := template.New("currConfig").Parse(configTemplate)
	if err != nil {
		return err
	}

	//create the new file
	file, err := os.Create("./fluent-bit.conf")
	if err != nil {
		return err
	}
	defer file.Close()

	//return file with the given template and TableId
	return tmpl.Execute(file, config)
}

// Log entry template (corresponding to BQ table schema)
type log_entry struct {
	StringField     string  `json:"StringField"`
	BytesField      []byte  `json:"BytesField"`
	IntegerField    int64   `json:"IntegerField"`
	FloatField      float64 `json:"FloatField"`
	NumericField    string  `json:"NumericField"`
	BigNumericField string  `json:"BigNumericField"`
	BooleanField    bool    `json:"BooleanField"`
	TimestampField  int64   `json:"TimestampField"`
	DateField       int32   `json:"DateField"`
	TimeField       string  `json:"TimeField"`
	DateTimeField   string  `json:"DateTimeField"`
	GeographyField  string  `json:"GeographyField"`
	RecordField     struct {
		SubField1 string `json:"SubField1"`
		SubField2 string `json:"SubField2"`
	} `json:"RecordField"`
	RangeField struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"RangeField"`
	JSONField string `json:"JSONField"`
}

// data generation function
func generateData(numRows int) error {
	// open file
	file, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	logger := log.New(file, "", 0)

	// send json marshalled data
	for i := 0; i < numRows; i++ {
		jsonData := map[string]interface{}{
			"name": "Jane Doe",
			"age":  28,
		}
		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			return err
		}

		curr := log_entry{
			StringField:     "hello world",
			BytesField:      []byte("hello bytes"),
			IntegerField:    123,
			FloatField:      123.45,
			NumericField:    "123.45",
			BigNumericField: "123456789.123456789",
			BooleanField:    true,
			//milliseconds since unix epoch
			TimestampField: 1721974126000000,
			//days since unix epoch
			DateField:      19929,
			TimeField:      "12:34:56",
			DateTimeField:  "2024-07-26 12:30:00.45",
			GeographyField: "POINT(1 2)",
			RecordField: struct {
				SubField1 string `json:"SubField1"`
				SubField2 string `json:"SubField2"`
			}{
				SubField1: "sub field value",
				SubField2: "45.6",
			},
			//hardcoded civil-time encoded value
			RangeField: struct {
				Start int64 `json:"start"`
				End   int64 `json:"end"`
			}{
				Start: 139830307704277524,
				End:   139830307704277525,
			},
			JSONField: string(jsonBytes),
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			return err
		}
		logger.Println(string(entry))

		time.Sleep(time.Second)
	}

	return nil
}
