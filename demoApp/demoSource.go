package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type logEntry struct {
	Text string `json:"Text"`
}

func writeToFirstLog() {
	file, err := os.OpenFile("logfile1.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	logger := log.New(file, "", 0)

	for i := 0; i < 10; i++ {
		curr := logEntry{
			Text: "Hello World!",
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			continue
		}

		logger.Println(string(entry))
		time.Sleep(time.Second)
	}
}

func writeToSecondLog() {
	file, err := os.OpenFile("logfile2.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	logger := log.New(file, "", 0)

	for i := 0; i < 10; i++ {
		curr := logEntry{
			Text: fmt.Sprint(i),
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			continue
		}

		logger.Println(string(entry))
		time.Sleep(time.Second)
	}
}

func main() {
	go writeToFirstLog()
	go writeToSecondLog()

	// Prevent main from exiting immediately
	select {}
}
