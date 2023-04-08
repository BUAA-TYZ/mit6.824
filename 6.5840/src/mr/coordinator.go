package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CoordinatorType int

const (
	MapState CoordinatorType = iota
	ReduceState
	Alldone
)

type State int

const (
	waiting State = iota
	working
	finished
)

type TaskMetaInfo struct {
	state   State
	taskAdr *Task
}
type TaskMetaholder struct {
	MetaMap map[int]*TaskMetaInfo
}
type Coordinator struct {
	// Your definitions here.
	taskMetaholder   TaskMetaholder
	Filelist         []string //所有的文件名
	MapQueue         chan *Task
	ReduceQueue      chan *Task
	Nreduce          int
	CoordinatorState CoordinatorType
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	//为防止多个线程同时拉取任务，这里加锁
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()
	switch c.CoordinatorState {
	case MapState:
		{
			//如果管道没有任务了，则说明所有任务都在被执行
			if len(c.MapQueue) == 0 {
				reply.Tasktype = WaitingTask
				//当探查到waitingtask coordinator就应该检查一次所有任务状态
				if c.checkdone() {
					c.tonextphase()
				}
			} else {
				*reply = *<-c.MapQueue
				c.taskMetaholder.MetaMap[reply.TaskId].state = working //改变元数据任务状态

			}
		}
	case ReduceState:
		{
			if len(c.ReduceQueue) == 0 {
				reply.Tasktype = WaitingTask
				//当探查到waitingtask coordinator就应该检查一次所有任务状态
				if c.checkdone() {
					c.tonextphase()
				}
			} else {
				*reply = *<-c.ReduceQueue
				c.taskMetaholder.MetaMap[reply.TaskId].state = working //改变元数据任务状态
			}
		}
	case Alldone:
		reply.Tasktype = ExitTask
	}
	return nil
}

func (c *Coordinator) checkdone() bool {
	var mapdonenum, reducedonenum int
	for _, v := range c.taskMetaholder.MetaMap {
		if v.taskAdr.Tasktype == MapTask {
			if v.state == finished {
				mapdonenum++
			}
		} else if v.taskAdr.Tasktype == ReduceTask {
			if v.state == finished {
				reducedonenum++
			}
		}
	}
	if c.CoordinatorState == MapState && mapdonenum >= len(c.Filelist) {
		return true
	}
	if c.CoordinatorState == ReduceState && reducedonenum >= c.Nreduce {
		return true
	}
	return false
}

func (c *Coordinator) MarkFinish(task, nouse2 *Task) error {
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()
	switch task.Tasktype {
	case MapTask:
		meta, ok := c.taskMetaholder.MetaMap[task.TaskId]
		if ok && meta.state == working {
			meta.state = finished
		} else {
			fmt.Println("task has already been finished")
		}
	case ReduceTask:
		meta, ok := c.taskMetaholder.MetaMap[task.TaskId]
		if ok && meta.state == working {
			meta.state = finished
		} else {
			fmt.Println("task has already been finished")
		}
	}
	return nil
}

func (m *Coordinator) Checkcrush() {
	// if a worker dies, then reput the task into the channel
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()
	timer := make([]int, len(m.Filelist)+m.Nreduce)
	for {
		if m.CoordinatorState == Alldone {
			break
		} else if m.CoordinatorState == MapState {
			for i := 0; i < len(m.Filelist); i++ {
				_, ok := m.taskMetaholder.MetaMap[i]
				if !ok {
					continue
				}
				if m.taskMetaholder.MetaMap[i].state == working {
					timer[i]++
				}
				if timer[i] >= 10 {
					//if timeout, put back the task and give up the worker
					m.taskMetaholder.MetaMap[i].state = waiting
					timer[i] = 0
					m.MapQueue <- m.taskMetaholder.MetaMap[i].taskAdr
				}
			}
		} else {
			for i := len(m.Filelist); i < len(timer); i++ {
				_, ok := m.taskMetaholder.MetaMap[i]
				if !ok {
					continue
				}
				if m.taskMetaholder.MetaMap[i].state == working {
					timer[i]++
				}
				if timer[i] >= 10 {
					//if timeout, put back the task and give up the worker
					m.taskMetaholder.MetaMap[i].state = waiting
					timer[i] = 0
					m.ReduceQueue <- m.taskMetaholder.MetaMap[i].taskAdr
				}
			}
		}
		time.Sleep(time.Second)
	}

}

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
	ret := false

	// Your code here.
	if c.CoordinatorState == Alldone {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Filelist:    files,
		Nreduce:     nReduce,
		MapQueue:    make(chan *Task, len(files)),
		ReduceQueue: make(chan *Task, nReduce),
		taskMetaholder: TaskMetaholder{
			make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.MakeMapTasks(c.Filelist)

	c.server()
	return &c
}

var UniqueId int

func (c *Coordinator) MakeMapTasks(files []string) {
	for i := 0; i < len(files); i++ {
		task := Task{
			MapTask,
			UniqueId,
			c.Nreduce,
			append([]string(nil), files[i]),
		}
		//记录源数据
		taskMetaInfo := TaskMetaInfo{
			waiting,
			&task,
		}
		c.taskMetaholder.MetaMap[UniqueId] = &taskMetaInfo
		fmt.Println("make a map task", &task)
		c.MapQueue <- &task
		UniqueId++
	}

}
func (c *Coordinator) MakeReduceTasks() {
	for i := 0; i < c.Nreduce; i++ {
		UniqueId++
		task := Task{
			ReduceTask,
			UniqueId,
			c.Nreduce,
			selectreducename(i),
		}
		//记录源数据
		taskMetaInfo := TaskMetaInfo{
			waiting,
			&task,
		}
		c.taskMetaholder.MetaMap[UniqueId] = &taskMetaInfo
		fmt.Println("make a reduce task", &task)
		c.ReduceQueue <- &task
	}
}
func selectreducename(i int) (pool []string) {
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			pool = append(pool, file.Name())
		}
	}
	return
}
func (c *Coordinator) tonextphase() {
	//状态转移
	if c.CoordinatorState == MapState {
		c.CoordinatorState = ReduceState
		c.MakeReduceTasks()
	} else if c.CoordinatorState == ReduceState {
		c.CoordinatorState = Alldone
	} else {
		fmt.Println("There is something wrong!!")
	}
}
