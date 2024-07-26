package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type log_entry struct {
	Val string `json:"Val"`
}

func main() {
	file, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}

	for {
		jsonData := map[string]interface{}{
			"name": "Jane Doe",
			"age":  28,
		}
		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			break
		}
		curr := log_entry{
			Val: string(jsonBytes),
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
