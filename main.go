package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type KCTask struct {
	connector string
	taskId    int
}

type KCTaskResponse struct {
	Id    int    `json:"id"`
	State string `json:"state"`
}

type KCConnectorResponse struct {
	State string `json:"state"`
}

type KCConnectorStatusResponse struct {
	Connector KCConnectorResponse `json:"connector"`
	Tasks     []KCTaskResponse    `json:"tasks"`
}

func main() {
	kcUrlPtr := flag.String("host", "localhost:8083", "kafka connect host")
	durationBetweenCheck := flag.Int("duration", 15, "duration between check in minutes")
	flag.Parse()
	fmt.Println("KAFKA CONNECT URL " + *kcUrlPtr)
	fmt.Printf("DURATION BETWEEN CHECK %d mins", *durationBetweenCheck)

	var kcUrl = *kcUrlPtr
	if !strings.HasPrefix(kcUrl, "http") {
		kcUrl = "http://" + kcUrl
	}
	if strings.HasSuffix(kcUrl, "/") {
		kcUrl = strings.Trim(kcUrl, "/")
	}

	for true {
		connectors, err := getConnectorsList(kcUrl)
		if err == nil {
			for _, connector := range connectors {
				go handleKafkaConnector(connector, kcUrl)
			}
		} else {
			log.Println(err)
		}
		time.Sleep(time.Duration(*durationBetweenCheck) * time.Minute)
	}
}

func getConnectorsFromAPIResponse(responseData []byte) []string {
	var connectors []string
	json.Unmarshal(responseData, &connectors)
	return connectors
}

func handleKafkaConnector(connector string, kafkaConnectHost string) {
	fmt.Println("Handling " + connector)
	var toRestartTasks = getToRestartKCTasks(connector, kafkaConnectHost)
	for _, task := range toRestartTasks {
		go restartKCTask(task, kafkaConnectHost)
	}

}

func restartKCTask(task KCTask, kcUrl string) (*http.Response, error) {
	var url = kcUrl + "/connectors/" + task.connector + "/tasks/" + strconv.Itoa(task.taskId) + "/restart"
	fmt.Println(url)
	fmt.Printf("Going to restart "+task.connector+":%d", task.taskId)
	return http.Post(url, "application/json", bytes.NewReader([]byte{}))
}

func getToRestartKCTasks(connector string, kcUrl string) []KCTask {
	var statusUrl = kcUrl + "/connectors/" + connector + "/status"
	resp, err := http.Get(statusUrl)
	if err != nil {
		log.Println(err)
		return []KCTask{}
	} else {
		responseData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return []KCTask{}
		} else {
			var status KCConnectorStatusResponse
			json.Unmarshal(responseData, &status)
			var toRestartTasks []KCTask
			if status.Connector.State == "RUNNING" {
				for _, task := range status.Tasks {
					if task.State == "FAILED" {
						toRestartTasks = append(toRestartTasks, KCTask{connector, task.Id})
					}
				}
			}
			return toRestartTasks
		}

	}
}

func getConnectorsList(kcUrl string) ([]string, error) {
	url := kcUrl + "/connectors"
	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return nil, err
	} else {
		responseData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		return getConnectorsFromAPIResponse(responseData), nil
	}
}
