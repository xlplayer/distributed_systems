package mr

import "time"
//import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Master struct {
	// Your definitions here.
	nReduce int
	files []string
	taskDoing []Task
	taskToDochan chan Task
	taskstage string
	workerid int
	leftmap int
	leftreduce int
	finish bool
	mu sync.Mutex
}

type Task struct {
	NReduce int
	TaskId int
	TaskType string
	TaskStatus string 
	FileName string
	FileNum int
	WorkderId int
	StartTime time.Time
}
// Your code here -- RPC handlers for the worker to call.
func (m *Master) Init(files []string, nReduce int){
	m.files = files
	m.nReduce = nReduce
	if len(files)>nReduce {
		m.taskToDochan = make(chan Task,len(files))
	}else{
		m.taskToDochan = make(chan Task,nReduce)
	}
	m.leftmap = len(files)
	m.leftreduce = nReduce
	m.finish = false
	m.taskstage = "map"
	m.taskDoing = make([]Task, len(files))
	for i:=0;i<len(files);i++{
		m.taskToDochan<-Task{
			NReduce:m.nReduce,
			TaskId:i,
			TaskType:"map",
			TaskStatus:"idle",
			FileName:files[i],
			FileNum:len(files),
			WorkderId:-1,
			StartTime:time.Now(),
		}
	}
}

func (m *Master) WorkerReg(args *RegArgs, reply *RegReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.WorkerId = m.workerid
	m.workerid ++
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	t := <- m.taskToDochan
	if t.TaskType == "map"{
		t.TaskStatus = "mapping"
	}else if t.TaskType == "reduce"{
		t.TaskStatus = "reducing"
	}
	t.WorkderId = args.WorkerId
	t.StartTime = time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskDoing[t.TaskId] = t
	reply.Task = t
	//fmt.Printf("%v",reply.Task)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	workerid := args.WorkerId
	taskid := args.TaskId
	info := args.Info
	m.mu.Lock()
	defer m.mu.Unlock()
	//fmt.Printf("%v\n",*args)
	if m.taskDoing[taskid].WorkderId == workerid {
		if info == "mapfinish" && m.taskDoing[taskid].TaskType == "map" && m.taskstage == "map"{
			m.taskDoing[taskid].TaskStatus = "mapfinish"
			m.leftmap--
			if m.leftmap <= 0{
				go m.ReduceInit()
			}
		}else if info == "reducefinish" && m.taskDoing[taskid].TaskType == "reduce" && m.taskstage == "reduce"{
			m.taskDoing[taskid].TaskStatus = "reducefinish"
			m.leftreduce--
			
			if m.leftreduce<=0{
				flag := true
				for _,t := range m.taskDoing{
					if t.TaskStatus != "reducefinish"{
						flag = false
						break
					}
				}
				if flag{
					m.finish = true
				}
			}
		}
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m* Master) ReduceInit(){
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leftmap<=0 && m.taskstage == "map"{
		for _,t := range m.taskDoing{
			if t.TaskStatus != "mapfinish"{
				return
			}
		}
		m.taskstage = "reduce"
		m.taskDoing = make([]Task, m.nReduce)
		for i:=0;i<m.nReduce;i++{
			m.taskToDochan<-Task{
				TaskId:i,
				WorkderId:-1,
				TaskType:"reduce",
				TaskStatus:"idle",
				FileName:"",
				FileNum:len(m.files),
				StartTime:time.Now(),
				NReduce:m.nReduce,
			}
		}
	}
}

func (m* Master) Checker(){
	for{
		m.mu.Lock()
		for _,task := range m.taskDoing{
			if task.TaskStatus == "mapping" || task.TaskStatus == "reducing"{
				if time.Now().Sub(task.StartTime) > time.Second*10{
					task.TaskStatus = "idle"
					task.WorkderId = -1
					m.taskToDochan<-task
				}
			}
		}
		m.mu.Unlock()
		time.Sleep(time.Second)
	}
}
//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool{
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.finish

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Init(files, nReduce)
	go m.Checker()
	m.server()
	return &m
}
