package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// initial request
	var workerId int
	var nReduce int
	var taskId int
	var taskType int
	var inputs []string
	var failed bool
	var output []string
	args := WorkerArgs{WorkerId: 0, RequestType: InitialRequest}
	reply := WorkerReply{}
	call("Coordinator.WorkerHandler", &args, &reply)
	for {
		//fmt.Println(reply)
		if reply.ExitMsg {
			break
		}
		failed = false
		workerId = reply.WorkerId
		nReduce = reply.NReduce
		taskId = reply.TaskId
		taskType = reply.TaskType
		inputs = reply.Input
		if taskType == TaskTypeMap {
			// 执行map操作
			intermediate := []KeyValue{}
			for _, filename := range inputs {
				file, err := os.Open(filename)
				if err != nil {
					file.Close()
					fail(workerId, FailedRequest, make([]string, 0), inputs, taskId, taskType, &reply)
					failed = true
					break
				}
				data, err := io.ReadAll(file)
				if err != nil {
					file.Close()
					fail(workerId, FailedRequest, make([]string, 0), inputs, taskId, taskType, &reply)
					failed = true
					break
				}
				//fmt.Printf("to map\n")
				kva := mapf(filename, string(data))
				intermediate = append(intermediate, kva...)
			}
			sort.Sort(ByKey(intermediate))
			files := []*os.File{}
			output = []string{}
			for i := 0; i < nReduce; i++ {
				oname := "mr-int-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(i)
				file, _ := os.Create(oname)
				file.Seek(0, 0)
				files = append(files, file)
				output = append(output, oname)
			}

			for _, kv := range intermediate {
				fmt.Fprintf(files[ihash(kv.Key)%nReduce], "%v %v\n", kv.Key, kv.Value)
			}
			if failed {
				continue
			}
			args = WorkerArgs{
				workerId,
				FinishedRequest,
				output,
				inputs,
				taskId,
				taskType,
			}
			call("Coordinator.WorkerHandler", &args, &reply)
		} else {
			// 执行reduce操作
			res := ByKey{}
			//for i := 0; i < nReduce; i++ {
			//	res = append(res, []KeyValue{})
			//}
			kvs := []KeyValue{}
			for _, filename := range inputs {
				file, err := os.Open(filename)
				if err != nil {
					file.Close()
					//fmt.Println(err, "shit")
					fail(workerId, FailedRequest, make([]string, 0), inputs, taskId, taskType, &reply)
					failed = true
					break
				}
				//data, err := io.ReadAll(file)
				//if err != nil {
				//	file.Close()
				//	fail(workerId, FailedRequest, make([]string, 0), inputs, taskId, taskType, &reply)
				//	failed = true
				//}
				var key string
				var value string
				for {
					if _, err = fmt.Fscanf(file, "%v %v\n", &key, &value); err != nil {
						//fmt.Println("in the read mr-int-* error ", err)
						break
					}
					kvs = append(kvs, KeyValue{key, value})
				}

			}
			sort.Sort(ByKey(kvs))
			//fmt.Println("ready to reduce!")
			if failed {
				continue
			}
			i := 0
			//fmt.Println("ready to reduce!2")
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)
				//fmt.Println("ready to reduce!3")
				res = append(res, KeyValue{kvs[i].Key, output})
				i = j
			}
			oname := "mr-out-" + strconv.Itoa(reply.ReduceNum)
			ofile, _ := os.Create(oname)
			for _, kv := range res {
				_, err := fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				if err != nil {
					//println("holy shit, err!!!!!", err)
				}
			}
			args = WorkerArgs{
				workerId,
				FinishedRequest,
				output,
				inputs,
				taskId,
				taskType,
			}
			call("Coordinator.WorkerHandler", &args, &reply)
		}
	}
}
func fail(workerId int, requestType int, output []string, input []string, taskId int, taskType int, reply *WorkerReply) {
	args := WorkerArgs{
		workerId,
		FailedRequest,
		make([]string, 0),
		input,
		taskId,
		taskType,
	}
	call("Coordinator.WorkerHandler", &args, reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
