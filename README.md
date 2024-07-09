# Fluentbit WriteAPI Output Plugin

This README includes all the necessary information to use the WriteAPI output pluging. This plugin allows you to stream records into Google Cloud BigQuery. This implementation only supports the following:
- JSON file formats

## Creating a BigQuery Dataset and Table
Fluentbit does not create the dataset and table for your data, so you must create these ahead of time. 
- [Creating and using datasets](https://cloud.google.com/bigquery/docs/datasets)
- [Creating and using tables](https://cloud.google.com/bigquery/docs/tables)
For this output plugin, the fields in the table and JSON file must match the target table exactly.

## Installing Fluentbit
First, install the necessary build tools:
```
sudo apt-get update
sudo apt-get install git
sudo apt-get install make
sudo apt-get install gcc
sudo apt-get install cmake
sudo apt-get install build-essential
sudo apt-get install flex
sudo apt-get install bison
sudo apt install libyaml-dev
sudo apt install libssl-dev
sudo apt install pkg-config
```

Clone the git repository with `git clone  https://github.com/fluent/fluent-bit.git` and then build the repo:
```
cd build
cmake ../
make
sudo make install
```

## Setting Up the Plugin
Clone this repository into it's own directory. The binary file should be up to date, but you can make it again by running `go build -buildmode=c-shared -o out_writeapi.so out_writeapi.go`. This command generates a header file (`out_writeapi.h`) and the binary file (`out_writeapi.so`).

## Configuration File and Parameters
The WriteAPI Output Plugin enables a customer to send data to Google BigQuery without writing any code. Most of the work is done through the `config` file (named something like `examplefile.config`). The FluentBit `config` file should contain the following sections at the very least: `SERVICE`, `INPUT`, `OUTPUT`. The following is an example of a `SERVICE` section:
```
[SERVICE]
    Flush           1
    Daemon          off
    Log_Level       error
    Parsers_File    path/to/jsonparser.conf
    plugins_file    path/to/plugins.conf
```
The `Parsers_File` field points to the parsing of your input and the `plugins_file` field is the path to the plugin you wish to use. The paths here are the absolute paths to the files.

Here is an example of a `INPUT` section:
```
[INPUT]
    Name    tail
    Path    path/to/logfile.log
    Parser  json
    Tag     logfile1
```
This establishes an input with the name `tail` with a specified path which uses the `json` parser specified in the `SERVICES` section. The tag is the most important part to take note of here, as this will be used to find matches for relevant outputs. The paths here is the absolute path to the file. 

Here is an example of an `OUTPUT` section:
```
[OUTPUT]
    Name                               writeapi
    Match                              logfile*
    ProjectId                          sample_projectID
    DatasetId                          sample_datasetID
    TableId                            sample_tableID
    Format                             json_lines
    Max_Chunk_Size                     1048576
    Max_Queue_Requests                 100
    Max_Queue_Bytes                    52428800
```
This establishes an output with the name `writeapi` which matches any input with a tag of `logfile*`. The match uses regex, so the input from above would lead to this output. The next three lines describe the destination table in BigQuery. The format relates to how the file should be parsed and the three lines after relate to how you want the stream to send data.

The `Max_Chunk_Size` field takes in the number of bytes that the plugin will attempt to chunk data into before appending into the BigQuery Table. Fluent-Bit supports around a maximum of 2 MB of data within a single flush and we exercise a hard maximum of 9 MB (as BigQuery cannot handle appending data larger than this size). The `Max_Queue_Requests` and `Max_Queue_Bytes` fields describe the maximum number of requests/bytes of outstanding asynchrous responses that can be queued. When the first limit is reached, data appending will be blocked until enough responses are ready and the number of outstanding requests/bytes decreases.  

For more information, look to [Fluentbit Official Guide to a Config File](https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file)
