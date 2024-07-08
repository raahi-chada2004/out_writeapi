# Fluentbit WriteAPI Output Plugin

This README inclues all the necessary information to use the WriteAPI Pipeline. 

## Configuration File

Fluentbit Official Guide to a Config File: https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file

The WriteAPI Output Plugin enables a customer to send data to Google BigQuery without writing any code. Most of the work is done through the `config` file (named something like `examplefile.config`). The FluentBit `config` file should contain the following sections at the very least: `SERVICE`, `INPUT`, `OUTPUT`. The following is an example of a `SERVICE` section:
```
[SERVICE]
    Flush           1
    Daemon          off
    Log_Level       error
    Parsers_File    path/to/jsonparser.conf
    plugins_file    path/to/plugins.conf
```
The `Parsers_File` field points to the parsing of your input and the `plugins_file` field is the path to the plugin you wish to use. 

Here is an example of a `INPUT` section:
```
[INPUT]
    Name    tail
    Path    path/to/logfile.log
    Parser  json
    Tag     logfile1
```
This establishes an input with the name `tail` with a specified path which uses the `json` parser specified in the `SERVICES` section. The tag is the most important part to take note of here, as this will be used to find matches for relevant outputs. 

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