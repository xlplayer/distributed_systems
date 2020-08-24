package mr

import "encoding/json"
import "fmt"
import "strings"
import "log"
import "os"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

type worker struct {
	// Your definitions here.
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
	id int
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func (w *worker) register(){
	args := RegArgs{}
	reply := RegReply{}
	call("Master.WorkerReg",&args, &reply)
	w.id = reply.WorkerId
	fmt.Printf("worker id: %v\n", w.id)
}

func (w *worker) GetTask() Task {
	args := GetTaskArgs{WorkerId:w.id}
	reply := GetTaskReply{}
	if call("Master.GetTask",&args, &reply) == false{
		fmt.Printf("master exit!\n")
		os.Exit(0)
	}
	task := reply.Task
	//fmt.Printf("%v\n",task)

	return task
}

func (w *worker) DoTask(task Task){
	switch task.TaskType {
	case "map":
		w.DoMapTask(task)
	case "reduce":
		w.DoReduceTask(task)
	default:
		panic("TaskType wrong!")
	}
}


func (w *worker) DoMapTask(task Task){
	file,_ := os.Open(task.FileName)
	content,_ := ioutil.ReadAll(file)
	file.Close()
	kvs := w.mapf(task.FileName, string(content))

	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, kvs := range reduces {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskId, idx)
		f,_:= os.Create(filename)
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			enc.Encode(&kv);
		}
	}
	args := ReportTaskArgs{WorkerId:w.id, TaskId:task.TaskId, Info:"mapfinish"}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
}


func (w *worker) DoReduceTask(task Task){
	maps := make(map[string][]string)
	for idx := 0; idx < task.FileNum; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, task.TaskId)
		file, err := os.Open(filename)
		if !os.IsNotExist(err) {
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				if _, ok := maps[kv.Key]; !ok {
					maps[kv.Key] = make([]string, 0, 100)
				}
				maps[kv.Key] = append(maps[kv.Key], kv.Value)
			}
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	filename :=  fmt.Sprintf("mr-out-%d", task.TaskId)
	ioutil.WriteFile(filename, []byte(strings.Join(res, "")), 0600)

	args := ReportTaskArgs{WorkerId:w.id, TaskId:task.TaskId, Info:"reducefinish"}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
}

func (w *worker) run(){
	for{
		task := w.GetTask()
		w.DoTask(task)
	}
}
//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
