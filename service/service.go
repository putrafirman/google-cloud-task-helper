package service

import (
	"context"
	"fmt"
	"time"

	"putrafirman/google-cloud-task-helper/entity"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TaskService struct {
	ProjectID  string `json:"projectID"`
	LocationID string `json:"locationID"`
	QueueID    string `json:"queueID"`
}

func NewTaskService(
	projectID string,
	locationID string,
	queueID string,
) *TaskService {
	return &TaskService{
		ProjectID:  projectID,
		LocationID: locationID,
		QueueID:    queueID,
	}
}

// ListTask return task in queue
func (t *TaskService) ListTask(ctx context.Context) ([]*taskspb.Task, error) {

	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}
	defer client.Close()

	queuePath := fmt.Sprintf("projects/%s/locations/%s/queues/%s", t.ProjectID, t.LocationID, t.QueueID)

	req := &taskspb.ListTasksRequest{
		Parent: queuePath,
	}

	listTask := client.ListTasks(ctx, req)

	task, _, _ := listTask.InternalFetch(100, listTask.PageInfo().Token)

	return task, nil
}

// CreateHTTPTask creates a new task with a HTTP target then adds it to a Queue.
func (t *TaskService) CreateHTTPTask(ctx context.Context, url string, taskID string, body []byte, httpMethod entity.HttpMethod, time time.Time) (*taskspb.Task, error) {

	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}
	defer client.Close()

	queuePath := fmt.Sprintf("projects/%s/locations/%s/queues/%s", t.ProjectID, t.LocationID, t.QueueID)
	taskName := fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", t.ProjectID, t.LocationID, t.QueueID, taskID)

	req := &taskspb.CreateTaskRequest{
		Parent: queuePath,
		Task: &taskspb.Task{
			Name:       taskName,
			CreateTime: timestamppb.Now(),
			ScheduleTime: &timestamppb.Timestamp{
				Seconds: time.Unix(),
			},
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					HttpMethod: taskspb.HttpMethod(httpMethod.Int()),
					Url:        url,
				},
			},
		},
	}

	req.Task.GetHttpRequest().Body = body

	createdTask, err := client.CreateTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cloudtasks.CreateTask: %v", err)
	}

	return createdTask, nil
}

// CreateQueue creates a new queue with max attempt retry
func (t *TaskService) CreateQueue(ctx context.Context, maxAttempts int32) (*taskspb.Queue, error) {

	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}
	defer client.Close()

	queuePath := fmt.Sprintf("projects/%s/locations/%s", t.ProjectID, t.LocationID)
	queueName := fmt.Sprintf("projects/%s/locations/%s/queues/%s", t.ProjectID, t.LocationID, t.QueueID)

	req := &taskspb.CreateQueueRequest{
		Parent: queuePath,
		Queue: &taskspb.Queue{
			Name: queueName,
			RetryConfig: &taskspb.RetryConfig{
				MaxAttempts: maxAttempts,
			},
		},
	}

	createdQueue, err := client.CreateQueue(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cloudtasks.CreateQueue: %v", err)
	}

	return createdQueue, nil
}
