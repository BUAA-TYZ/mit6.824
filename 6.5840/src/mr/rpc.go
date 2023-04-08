package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.

// worker()向coordinator发送一个rpc来获取任务
// 最先想到要一个filename来进行任务，以及任务类型是map还是reduce
// 还要有一个任务序号，一个nreduce
type Task struct {
	Tasktype TaskType
	TaskId   int
	Nreduce  int
	Filename []string
}

// rpc 需要传入参数，但实际上获取任务不需要参数故用一个空的结构体
type TaskArgs struct{}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
