package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

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

// Add your RPC definitions here.
type WorkerArgs struct {
	WorkerId    int
	RequestType int // request的类型

	// 下面的两个参数仅任务完成时存在且发送
	Output   []string // map任务生成的中间文件名或者reduce任务生成的文件名
	Input    []string
	TaskId   int // 对应的task id
	TaskType int
}

const (
	InitialRequest  = 0
	FinishedRequest = 1
	FailedRequest   = 2
)

type WorkerReply struct {
	WorkerId  int // 记录worker id
	TaskType  int // 任务的类型
	NReduce   int // reduce需要输出文件的数量
	TaskId    int // task的id
	Input     []string
	ReduceNum int
	ExitMsg   bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
