package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerState int

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetTask() Task {
	//调用Coordinator.PollTask方法来获取任务
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("fails to GetTask")
	}
	return reply
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	//向coordinator要任务
	flag := true
	for flag {
		task := GetTask()
		switch task.Tasktype {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				calldone(&task)
			}
		case WaitingTask:
			{
				fmt.Println("The task is not done")
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				calldone(&task)
			}
		case ExitTask:
			{
				fmt.Println("Task about :[", task.TaskId, "] finish")
				flag = false
			}
		}
	}

}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var InterKeyValue []KeyValue
	file, err := os.Open(task.Filename[0])
	//fmt.Println("open ", task.Filename[0])
	if err != nil {
		log.Fatalf("can't open file:%v", file)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read file:%v", file)
	}
	file.Close()
	filename := task.Filename[0]
	InterKeyValue = mapf(filename, string(content))

	nr := task.Nreduce
	Storekv := make([][]KeyValue, nr)
	//下面要用ihash将Map任务分成nreduce块
	for _, kv := range InterKeyValue {
		//往相同的哈系值中加入键值对
		Storekv[ihash(kv.Key)%nr] = append(Storekv[ihash(kv.Key)%nr], kv)
	}

	for i := 0; i < nr; i++ {
		osname := "mr-tmp-" + strconv.Itoa(task.TaskId) + strconv.Itoa(i)
		newfile, _ := os.Create(osname)
		enc := json.NewEncoder(newfile)
		for _, kv := range Storekv[i] {
			enc.Encode(kv)
		}
		newfile.Close()
	}
}
func DoReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := shuffle(task.Filename)
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("fail to create tempfile", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		pool := []string{}
		for k := i; k < j; k++ {
			pool = append(pool, intermediate[k].Value)
		}
		res := reducef(intermediate[i].Key, pool)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, res)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), fn)
}

// shuffle将相同的键排序到一起
func shuffle(files []string) (kva []KeyValue) {
	for _, filee := range files {
		file, _ := os.Open(filee)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return
}
func calldone(task *Task) {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkFinish", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed!")
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
