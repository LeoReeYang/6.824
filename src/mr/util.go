package mr

import (
	"fmt"
	"log"
	"time"
)

const (
	prefix = string("mr-")
)

const (
	MaxRunTime = 10 * time.Second
)

type TaskStatus int

const (
	Idle      TaskStatus = 0
	Assigned  TaskStatus = 1
	Completed TaskStatus = 2
)

type worker struct {
	id int32
}

type Task struct {
	file     string
	taskType ResponseType
	taskId   int32
	nReduce  int
	nFiles   int
}

type TaskStat struct {
	workerid int32
	state    TaskStatus
}

func (w *worker) Register() {
	arg := JoinRegister{}
	reply := JoinReply{}

	ok := call("Coordinator.Register", &arg, &reply)
	if !ok {
		log.Fatal("Register failed.")
	}
	w.id = reply.Id
}

func combineName(m, n int) string {
	return fmt.Sprintf("mr-%d-%d", m, n)
}

func SetTime() time.Time {
	return time.Now()
}
