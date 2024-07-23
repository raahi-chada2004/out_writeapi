package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type log_entry struct {
	Time string `json:"Time"`
	Text string `json:"Text"`
}

func main() {
	file, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}

	for {
		curr := log_entry{
			Time: time.Now().Format(time.RFC3339),
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
