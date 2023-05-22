package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Coordinator 特别注意锁的获取顺序
type Coordinator struct {
	NReduce  int
	InputNum int
	Timeout  time.Duration

	NextWorkerId      int
	NextWorkerIdMutex sync.Mutex

	NextTaskId      int
	NextTaskIdMutex sync.Mutex

	TaskChan    chan Task
	TaskMsgChan chan TaskMsg

	MapResult      []string // 收到worker发来的结束消息后，task池里面的task就会删除然后放入MapResult
	MapResultMutex sync.Mutex

	WorkingTask      map[int]TaskStatus // 正在运行的task就放在这里面
	WorkingTaskMutex sync.Mutex

	//ReduceFinished      bool
	//ReduceFinishedMutex sync.Mutex

	ReduceTaskNum      int
	ReduceTaskNumMutex sync.Mutex

	WorkerNum      int
	WorkerNumMutex sync.Mutex

	WaitingNum      int
	WaitingNumMutex sync.Mutex
}
type Task struct {
	Input           []string
	CreateTask      bool
	TaskType        int
	TaskId          int
	ExcludeWorkerId int
	ReduceNum       int
	ExitTask        bool
}
type TaskMsg struct {
	Msg       int
	TaskId    int
	TaskType  int
	Input     []string
	Output    []string
	TimeStamp time.Time
	WorkerId  int
	ReduceNum int
}

const (
	CreateTaskMsg = 0
	FinishTaskMsg = 1
	UpdateTaskMsg = 2
	FailedTaskMsg = 3
)
const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
)

type TaskStatus struct {
	TaskId    int       // task的id
	TaskType  int       // task的类型
	Input     []string  // task的输入
	StartTime time.Time // task的开始时间
	WorkerId  int
	ReduceNum int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkerHandler(args *WorkerArgs, reply *WorkerReply) error {
	//fmt.Println("in it")
	c.incWaitingNum()
	defer c.decWaitingNum()
	if args.RequestType == InitialRequest {
		reply.WorkerId = c.getNextWorkerId()
		args.WorkerId = reply.WorkerId
		c.incWorkerNum()
	} else if args.RequestType == FinishedRequest {
		taskmsg := TaskMsg{
			FinishTaskMsg,
			args.TaskId,
			args.TaskType,
			args.Input,
			args.Output,
			time.Now(),
			args.WorkerId,
			0,
		}
		c.TaskMsgChan <- taskmsg
	} else if args.RequestType == FailedRequest {
		taskmsg := TaskMsg{
			FailedTaskMsg,
			args.TaskId,
			args.TaskType,
			args.Input,
			args.Output,
			time.Now(),
			args.WorkerId,
			0,
		}
		c.TaskMsgChan <- taskmsg
	}
	task := <-c.TaskChan
	if task.ExitTask {
		reply.ExitMsg = true
		c.decWorkerNum()
		return nil
	}
	for task.ExcludeWorkerId == args.WorkerId {
		c.TaskChan <- task
		time.Sleep(time.Duration(time.Millisecond))
		task = <-c.TaskChan
	}
	taskmsg := TaskMsg{}
	if task.CreateTask {
		taskmsg.Msg = CreateTaskMsg
	} else {
		taskmsg.Msg = UpdateTaskMsg
	}
	taskmsg.TaskId = task.TaskId
	taskmsg.TimeStamp = time.Now()
	taskmsg.Input = task.Input
	taskmsg.WorkerId = args.WorkerId
	taskmsg.TaskType = task.TaskType
	taskmsg.ReduceNum = task.ReduceNum
	c.TaskMsgChan <- taskmsg
	reply.TaskId = taskmsg.TaskId
	reply.TaskType = taskmsg.TaskType
	reply.Input = taskmsg.Input
	reply.NReduce = c.NReduce
	reply.WorkerId = args.WorkerId
	reply.ReduceNum = task.ReduceNum
	return nil
}
func (c *Coordinator) mapFinished() bool {
	c.MapResultMutex.Lock()
	defer c.MapResultMutex.Unlock()
	return len(c.MapResult) == c.InputNum
}
func (c *Coordinator) TaskStatusManager() {
	for {
		msg := <-c.TaskMsgChan
		switch msg.Msg {
		case CreateTaskMsg:
			{
				taskss := TaskStatus{
					msg.TaskId,
					msg.TaskType,
					msg.Input,
					msg.TimeStamp,
					msg.WorkerId,
					msg.ReduceNum,
				}
				c.WorkingTaskMutex.Lock()
				c.WorkingTask[taskss.TaskId] = taskss
				c.WorkingTaskMutex.Unlock()
			}
		case FinishTaskMsg:
			{
				c.WorkingTaskMutex.Lock()
				if _, ok := c.WorkingTask[msg.TaskId]; ok {
					// 如果存在，那么表示这个task结束了，直接删除
					if msg.TaskType == TaskTypeMap {
						c.MapResultMutex.Lock()
						c.MapResult = append(c.MapResult, msg.Output...)
						//fmt.Printf("%v %v", len(c.MapResult), c.InputNum)
						if len(c.MapResult) == c.InputNum*c.NReduce {
							// 表示所有map任务都已经完成，可以加入reduce任务了
							reduceTask := make([][]string, c.NReduce)

							for _, mres := range c.MapResult {
								var idx int
								var taskId int
								var workerId int
								fmt.Sscanf(mres, "mr-int-%d-%d-%d", &taskId, &workerId, &idx)
								reduceTask[idx] = append(reduceTask[idx], mres)
							}
							for i := 0; i < c.NReduce; i++ {
								task := Task{
									reduceTask[i],
									true,
									TaskTypeReduce,
									c.getNextTaskId(),
									0,
									i,
									false,
								}
								c.TaskChan <- task
							}
						}
						c.MapResultMutex.Unlock()
					} else {
						c.ReduceTaskNumMutex.Lock()
						c.ReduceTaskNum -= 1
						c.ReduceTaskNumMutex.Unlock()
					}
					delete(c.WorkingTask, msg.TaskId)
				}
				c.WorkingTaskMutex.Unlock()
				if c.getReduceTaskNum() == 0 {
					task := Task{ExitTask: true}
					for {
						//进入收尾阶段，啥都不干了
						c.TaskChan <- task
					}
				}
			}
		case UpdateTaskMsg:
			{
				c.WorkingTaskMutex.Lock()
				if _, ok := c.WorkingTask[msg.TaskId]; ok {
					c.WorkingTask[msg.TaskId] = TaskStatus{
						msg.TaskId,
						msg.TaskType,
						msg.Input,
						msg.TimeStamp,
						msg.WorkerId,
						msg.ReduceNum,
					}
				}
				c.WorkingTaskMutex.Unlock()
			}
		case FailedTaskMsg:
			{
				c.WorkingTaskMutex.Lock()
				if _, ok := c.WorkingTask[msg.TaskId]; ok {
					c.TaskChan <- Task{
						msg.Input,
						false,
						msg.TaskType,
						msg.TaskId,
						msg.WorkerId,
						c.WorkingTask[msg.TaskId].ReduceNum,
						false,
					}
				}
				c.WorkingTaskMutex.Unlock()
			}
		}
	}
}
func (c *Coordinator) CheckTimeout() {
	for {
		c.WorkingTaskMutex.Lock()
		now := time.Now()
		for key, _ := range c.WorkingTask {
			if now.Sub(c.WorkingTask[key].StartTime) >= c.Timeout {
				// 超时了
				task := Task{
					c.WorkingTask[key].Input,
					false,
					c.WorkingTask[key].TaskType,
					c.WorkingTask[key].TaskId,
					c.WorkingTask[key].WorkerId,
					c.WorkingTask[key].ReduceNum,
					false,
				}
				c.TaskChan <- task
			}
		}
		c.WorkingTaskMutex.Unlock()
		time.Sleep(c.Timeout)
	}
}
func (c *Coordinator) getNextTaskId() int {
	c.NextTaskIdMutex.Lock()
	defer c.NextTaskIdMutex.Unlock()
	res := c.NextTaskId
	c.NextTaskId += 1
	return res
}
func (c *Coordinator) getNextWorkerId() int {
	c.NextWorkerIdMutex.Lock()
	defer c.NextWorkerIdMutex.Unlock()
	res := c.NextWorkerId
	c.NextWorkerId += 1
	return res
}
func (c *Coordinator) incWorkerNum() {
	c.WorkerNumMutex.Lock()
	defer c.WorkerNumMutex.Unlock()
	c.WorkerNum += 1
}
func (c *Coordinator) decWorkerNum() {
	c.WorkerNumMutex.Lock()
	defer c.WorkerNumMutex.Unlock()
	c.WorkerNum -= 1
}
func (c *Coordinator) getReduceTaskNum() int {
	c.ReduceTaskNumMutex.Lock()
	defer c.ReduceTaskNumMutex.Unlock()
	return c.ReduceTaskNum
}
func (c *Coordinator) incWaitingNum() {
	c.WaitingNumMutex.Lock()
	defer c.WaitingNumMutex.Unlock()
	c.WaitingNum += 1
}
func (c *Coordinator) decWaitingNum() {
	c.WaitingNumMutex.Lock()
	defer c.WaitingNumMutex.Unlock()
	c.WaitingNum -= 1
}
func (c *Coordinator) getWaitingNum() int {
	c.WaitingNumMutex.Lock()
	defer c.WaitingNumMutex.Unlock()
	return c.WaitingNum
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 原子操作，放置多次自增

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	chk1 := false
	chk2 := false
	c.ReduceTaskNumMutex.Lock()
	chk1 = c.ReduceTaskNum == 0
	//fmt.Printf("%v ", c.ReduceTaskNum)
	c.ReduceTaskNumMutex.Unlock()

	chk2 = c.getWaitingNum() == 0
	return chk1 && chk2
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.NReduce = nReduce
	c.InputNum = len(files)
	c.NextTaskId = 1
	c.NextWorkerId = 1
	c.TaskChan = make(chan Task, len(files)+1)
	c.TaskMsgChan = make(chan TaskMsg, len(files)+1)
	c.Timeout = time.Duration(time.Second * 10)
	for i := 0; i < len(files); i++ {
		input := []string{files[i]}
		task := Task{
			input,
			true,
			TaskTypeMap,
			c.getNextTaskId(),
			0,
			0,
			false,
		}
		//fmt.Printf("%v\n", task)
		c.TaskChan <- task
	}
	//for i := 0; i < nReduce; i++ {
	//	filename := "mr-int-" + strconv.Itoa(i)
	//	file, _ := os.Create(filename)
	//	file.Close()
	//}
	c.MapResult = []string{}
	c.WorkingTask = make(map[int]TaskStatus)
	c.WorkerNum = 0
	c.ReduceTaskNum = nReduce
	go c.CheckTimeout()
	go c.TaskStatusManager()
	c.WaitingNum = 0
	c.server()
	return &c
}
