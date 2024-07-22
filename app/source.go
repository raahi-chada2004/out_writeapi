package main

import (
	"encoding/json"
	"fmt"
	"log"
	_ "math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type log_entry struct {
	Time string `json:"Time"`
	Text string `json:"Text"`
}

func writeToFirstLog() {
	file, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	defer file.Close()

	currtime := time.NewTicker(time.Millisecond)
	defer currtime.Stop()

	datatime := time.NewTimer(60 * time.Second)
	defer datatime.Stop()

	tptime := time.NewTicker(time.Second)
	defer tptime.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	datagoing := false
	var currbytes int

	for {
		select {
		case <-currtime.C:
			if !datagoing {
				for i := 0; i < 2; i++ {
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
					currbytes += len(entry) + 1
				}
			}
		case <-datatime.C:
			datagoing = true
		case <-tptime.C:
			fmt.Printf("Current throughput: %.2f MB/s\n", float64(currbytes)/(1024*1024))
			currbytes = 0
		case <-stop:
			fmt.Println("ending")
			return
		}

	}
}

func writeToSecondLog() {
	file, err := os.OpenFile("logfile2.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	defer file.Close()

	currtime := time.NewTicker(time.Millisecond)
	defer currtime.Stop()

	datatime := time.NewTimer(60 * time.Second)
	defer datatime.Stop()

	tptime := time.NewTicker(time.Second)
	defer tptime.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	datagoing := false
	var currbytes int

	for {
		select {
		case <-currtime.C:
			if !datagoing {
				for i := 0; i < 2; i++ {
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
					currbytes += len(entry) + 1
				}
			}
		case <-datatime.C:
			datagoing = true
		case <-tptime.C:
			fmt.Printf("Current throughput: %.2f MB/s\n", float64(currbytes)/(1024*1024))
			currbytes = 0
		case <-stop:
			fmt.Println("ending")
			return
		}

	}
}

func writeToThirdLog() {
	file, err := os.OpenFile("logfile3.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(file, "", 0)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	defer file.Close()

	currtime := time.NewTicker(time.Millisecond)
	defer currtime.Stop()

	datatime := time.NewTimer(60 * time.Second)
	defer datatime.Stop()

	tptime := time.NewTicker(time.Second)
	defer tptime.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	datagoing := false
	var currbytes int

	for {
		select {
		case <-currtime.C:
			if !datagoing {
				for i := 0; i < 2; i++ {
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
					currbytes += len(entry) + 1
				}
			}
		case <-datatime.C:
			datagoing = true
		case <-tptime.C:
			fmt.Printf("Current throughput: %.2f MB/s\n", float64(currbytes)/(1024*1024))
			currbytes = 0
		case <-stop:
			fmt.Println("ending")
			return
		}

	}
}

func main() {
	go writeToFirstLog()
	go writeToSecondLog()
	go writeToThirdLog()

	// Prevent main from exiting immediately
	select {}
}
