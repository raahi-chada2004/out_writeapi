package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
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
		{Name: "Message", Type: bigquery.StringFieldType},
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
	if err := createConfigFile(projectID, datasetID, tableID); err != nil {
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
	queryMsg := "SELECT Message FROM `" + projectID + "." + datasetID + "." + tableID + "`"
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

		assert.Equal(t, "hello world", BQvalues[0])
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
`

// struct for dynamically updating config file
type Config struct {
	CurrLogfilePath string
	CurrProjectName string
	CurrDatasetName string
	CurrTableName   string
}

// function creates configuration file with the input as the TableId field
func createConfigFile(currProjectID string, currDatasetID string, currTableID string) error {
	//struct with the TableId
	config := Config{
		CurrLogfilePath: logFilePath,
		CurrProjectName: currProjectID,
		CurrDatasetName: currDatasetID,
		CurrTableName:   currTableID,
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

//Log entry template (corresponding to BQ table schema)

type log_entry struct {
	Message string `json:"Message"`
}

// data generation function
func generateData(numRows int) error {
	//open file
	file, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		return err
	}

	//send json marshalled data
	for i := 0; i < numRows; i++ {
		curr := log_entry{
			Message: "hello world",
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			return err
		}
		logger.Println(string(entry))
	}

	return nil
}
