package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	worker := worker{}
	worker.Register()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		resp := RequestTask(worker.id)

		switch resp.Task.taskType {
		case MapTask:
			worker.DoMapTask(mapf, resp)
			break
		case ReduceTask:
			worker.DoRecuceTask(reducef, resp, worker.id)
			break
		case Wait:
			time.Sleep(5 * time.Second)
			break
		case TaskDone:
			log.Println("Tasks finished.")
			return
		default:
			panic("Invalide state.")
		}
	}
}

func (w *worker) DoMapTask(mapf func(string, string) []KeyValue, resp TaskReply) {
	intermediate := []KeyValue{}

	file, err := os.Open(resp.Task.file)
	if err != nil {
		FinishTask(w.id, resp.Task, false)
		log.Fatalf("cannot open %v", resp.Task.file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		FinishTask(w.id, resp.Task, false)
		log.Fatalf("cannot read %v", resp.Task.file)
	}
	file.Close()

	kva := mapf(resp.Task.file, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := resp.Task.nReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)

	for index := 0; index < nReduce; index++ {
		outFiles[index], err = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		if err != nil {
			FinishTask(w.id, resp.Task, false)
			log.Fatalf("temp file creat failed.")
		}
		fileEncs[index] = json.NewEncoder(outFiles[index])
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce

		file = outFiles[index]
		enc := fileEncs[index]
		err := enc.Encode(&kv)
		if err != nil {
			FinishTask(w.id, resp.Task, false)
			log.Printf("File %v Key %v Value %v Error: %v\n", resp.Task.file, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	for index, file := range outFiles {
		// outname := prefix + strconv.Itoa(int(resp.task.taskId)) + "-" + strconv.Itoa(index)
		outname := combineName(int(resp.Task.taskId), index)
		oldpath := filepath.Join(file.Name())

		err := os.Rename(oldpath, outname)
		if err != nil {
			FinishTask(w.id, resp.Task, false)
			log.Printf("Rename failed,%v", err.Error())
		}
		file.Close()
	}

	FinishTask(w.id, resp.Task, true)
}

func (w *worker) DoRecuceTask(reducef func(string, []string) string, resp TaskReply, workerid int32) {
	// intermediate := []KeyValue{}
	kvmaps := make(map[string][]string)
	// ans := make([]string, 0)

	for index := 0; index < resp.Task.nFiles; index++ {
		file, err := os.Open(combineName(int(resp.Task.taskId), int(workerid)))
		if err != nil {
			FinishTask(w.id, resp.Task, false)
			log.Fatal("ReducePhase open file failed")
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := kvmaps[kv.Key]; !ok {
				kvmaps[kv.Key] = make([]string, 0, 100)
			}
			kvmaps[kv.Key] = append(kvmaps[kv.Key], kv.Value)
			// intermediate = append(intermediate, kv)
		}
	}
	// file, err := os.Create("mr-out-" + strconv.Itoa(int(workerid)))
	// file, err := os.CreateTemp("mr-out-", "mr-out-*")
	file, err := ioutil.TempFile("mr-tmp", "mr-tmp-*")
	if err != nil {
		log.Fatalf("failed create temp reduce output file.")
	}
	for k, v := range kvmaps {
		// ans = append(ans, fmt.Sprintf("%v %v\n", k, reducef(k, v)))
		fmt.Fprintf(file, "%v %v\n", k, reducef(k, v))
	}
	oldpath := filepath.Join(file.Name())
	err = os.Rename(oldpath, "mr-out-"+strconv.Itoa(int(workerid)))
	if err != nil {
		log.Fatalf("Rename ReduceTask failed")
		FinishTask(w.id, resp.Task, false)
	}
	FinishTask(w.id, resp.Task, true)
}

func RequestTask(id int32) (r TaskReply) {
	arg := TaskRequest{Workerid: id}
	reply := TaskReply{}
	ok := call("Coordinator.TaskDispatch", &arg, &reply)

	if ok {
		return reply
	} else {
		log.Fatalf("RequestTask failed")
	}

	return reply
}

func FinishTask(wid int32, t Task, isdone bool) {
	args := TaskFinish{
		Workerid: wid,
		IsDone:   isdone,
		Tasktype: t.taskType,
	}
	reply := TaskFinishReply{}

	ok := call("TaskFinish", &args, &reply)
	if !ok {
		log.Fatalf("FinishTask failed.Master died...")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
