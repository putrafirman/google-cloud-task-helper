package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	e "putrafirman/google-cloud-task-helper/entity"
	s "putrafirman/google-cloud-task-helper/service"

	"github.com/google/uuid"
)

type IniModel struct {
	Nama  string `json:"name"`
	Score int    `json:"score"`
}

func main() {

	/*
		example default queue config:
		name:"**2"
		rate_limits:{max_dispatches_per_second:500  max_burst_size:100  max_concurrent_dispatches:1000}
		retry_config:{max_attempts:10  min_backoff:{nanos:100000000}  max_backoff:{seconds:3600} max_doublings:16}
		state:RUNNING
	*/

	/*
		Init Service
	*/

	taskService := s.NewTaskService("project-name", "asia-southeast1", "rg-live-task-dev-alpha2")
	ctx := context.Background()

	/*
		Example Creating New Queue and Handling
	*/
	respQueue, errQueue := taskService.CreateQueue(ctx, 10)
	if errQueue != nil {
		if strings.Contains(errQueue.Error(), "AlreadyExists") {
			fmt.Println("queue already created")
		}
		fmt.Println(errQueue)
	}

	fmt.Println(respQueue)
	fmt.Println("======")

	/*
		Example Create New Task inside Queue
	*/
	testStruct := IniModel{
		Nama:  "Putra",
		Score: 1,
	}
	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(testStruct)
	id := uuid.New()
	resp, err := taskService.CreateHTTPTask(ctx, "your-http-target", "coba-dari-local-schedule-"+id.String(), reqBodyBytes.Bytes(), e.HttpMethod_POST, time.Now())
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(resp)
	fmt.Println("======")

	/*
		Example List Task inside Queue
	*/
	respList, _ := taskService.ListTask(ctx)

	for _, task := range respList {
		fmt.Println(task)
		fmt.Println(task.ScheduleTime.AsTime().Format(time.RFC3339))
	}
}
