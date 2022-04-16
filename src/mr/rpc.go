package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ResponseType string

const (
	MapTask    ResponseType = "MapTask"
	ReduceTask ResponseType = "ReduceTask"
	Wait       ResponseType = "Wait"
	TaskDone   ResponseType = "TaskDone"
)

type Response struct {
	task Task
}

type JoinRegister struct {
}
type JoinReply struct {
	Id int32
}

type TaskRequest struct {
	Workerid int32
}
type TaskReply struct {
	Task Task
}

type TaskFinish struct {
	Taskid   int32
	Workerid int32
	IsDone   bool
	Tasktype ResponseType
}
type TaskFinishReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
