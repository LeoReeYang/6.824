package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type phase int

const (
	MapTaskPhase    phase = 0
	ReduceTaskPhase phase = 1
)

type Coordinator struct {
	// Your definitions here.
	files []string

	mapNum    int
	reduceNum int

	workers   int32
	isDone    bool
	taskPhase phase

	//Record the status of the tasks

	// mapStatus    map[Task]TaskStat
	// reduceStatus map[Task]TaskStat

	// mapTasks    chan Task
	// reduceTasks chan Task

	MapTasks    []Task
	ReduceTasks []Task
	Status      []TaskStat

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CheckTimer(tid int32) {
	timer := time.NewTimer(MaxRunTime)
	<-timer.C

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.Status[tid].state != Completed {
		c.Status[tid].state = Idle
	}
}

func (c *Coordinator) TaskDispatch(args *TaskRequest, reply *TaskReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.Done() {
		reply.Task.taskType = TaskDone
		return nil
	} else if c.taskPhase == MapTaskPhase {
		for id := 0; id < c.mapNum; id++ {
			if c.Status[id].state == Idle {
				reply.Task = c.MapTasks[id]
				c.Status[id].state = Assigned
				c.Status[id].workerid = args.Workerid
				go c.CheckTimer(int32(id))
				return nil
			}
		}
		reply.Task.taskType = Wait
		return nil
	} else {
		for id := 0; id < c.reduceNum; id++ {
			if c.Status[id+c.mapNum].state == Idle {
				reply.Task = c.MapTasks[id]
				c.Status[id].state = Assigned
				c.Status[id].workerid = args.Workerid
				go c.CheckTimer(int32(id))
				return nil
			}
		}
		reply.Task.taskType = Wait
		return nil
	}

}

func (c *Coordinator) TaskFinish(arg *TaskFinish, reply *TaskFinishReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if arg.IsDone {
		c.Status[arg.Taskid].state = Completed

		if c.taskPhase == MapTaskPhase {
			for tid, status := range c.Status {
				if tid < c.mapNum && status.state != Completed {
					return nil
				}
			}
			c.taskPhase = ReduceTaskPhase
			return nil
		} else {
			for tid, status := range c.Status {
				if tid >= c.mapNum && status.state != Completed {
					return nil
				}
			}
			c.isDone = true
			return nil
		}

	} else {
		c.Status[arg.Taskid].state = Idle
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	ret := false

	// Your code here.

	if c.isDone {
		ret = true
	}

	return ret
}

func (c *Coordinator) Register(arg *JoinRegister, reply *JoinReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.Id = c.workers
	c.workers++
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.mapNum = len(files)
	c.reduceNum = nReduce
	c.workers = 0
	c.mutex = sync.Mutex{}

	c.MapTasks = make([]Task, c.mapNum)
	c.ReduceTasks = make([]Task, c.reduceNum)
	c.Status = make([]TaskStat, c.mapNum+c.reduceNum)

	for id := 0; id < c.mapNum; id++ {
		c.MapTasks[id] = Task{files[id], MapTask, int32(id), nReduce, c.mapNum}
		c.Status[id] = TaskStat{workerid: -1, state: Idle}
	}
	for id := 0; id < c.reduceNum; id++ {
		c.ReduceTasks[id] = Task{"", ReduceTask, int32(id), nReduce, c.mapNum}
		c.Status[id+c.mapNum] = TaskStat{workerid: -1, state: Idle}
	}

	c.taskPhase = MapTaskPhase
	// Your code here.

	c.server()
	return &c
}
