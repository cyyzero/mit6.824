package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	TaskUnrunning TaskStatus = iota
	TaskRunning
	TaskFinished
)

type Coordinator struct {
	Files []string
	NReduce int
	IntermediateFiles  [][]string
	MapStatus, ReduceStatus []TaskStatus
	CurMapIdx, CurReduceIdx int // used for round-robin
	MapMutex, ReduceMutex, IntermediateFilesMutex sync.Mutex
	done chan struct{}
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

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	mapID, reduceID := c.allocUnRunningTask()
	if mapID != -1 {
		reply.Type = MapType
		reply.ID = mapID
		reply.NReduce = c.NReduce
		reply.Filenames = []string{c.Files[mapID]}
		go c.runTaskTimeout(MapType, mapID)
		return nil
	}
	if reduceID != -1 {
		reply.Type = ReduceType
		reply.ID = reduceID
		reply.NMap = len(c.Files)
		// Do not need to lock because reduce tasks are all after map tasks
		reply.Filenames = c.IntermediateFiles[reduceID]
		go c.runTaskTimeout(ReduceType, reduceID)
		return nil
	}
	if c.isMapTaskAllFinished() && c.isReduceTaskAllFinished() {
		reply.Type = ExitType
		return nil
	}
	// mapID == -1 && reduceID == -1, means all tasks are running or finished
	reply.Type = SleepType
	return nil
}

func (c* Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	t := args.Type
	id := args.ID
	switch t {
	case MapType:
		if id < len(c.MapStatus) {
			c.MapMutex.Lock()
			c.MapStatus[id] = TaskFinished
			c.MapMutex.Unlock()
			c.appendIntermediateFiles(args.IntermediateFiles)
			log.Printf("map task %d is finished", id)
		}
	case ReduceType:
		if id < len(c.ReduceStatus) {
			c.ReduceMutex.Lock()
			c.ReduceStatus[id] = TaskFinished
			c.ReduceMutex.Unlock()
			log.Printf("reduce task %d is finished", id)
		}
	}
	if c.isMapTaskAllFinished() && c.isReduceTaskAllFinished() {
		fmt.Println(c.MapStatus, c.ReduceStatus)
		c.done <- struct{}{}
		// c.finishAll = true
	}
	return nil
}

// return whether all map tasks is finished
func (c *Coordinator) isMapTaskAllFinished() bool {
	c.MapMutex.Lock()
	defer c.MapMutex.Unlock()
	for _, state := range c.MapStatus {
		if state != TaskFinished {
			return false
		}
	}
	return true
}

// return whether all reduce tasks is fnished
func (c *Coordinator) isReduceTaskAllFinished() bool {
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	for _, state := range c.ReduceStatus {
		if state != TaskFinished {
			return false
		}
	}
	return true
}

// return unrunning task id
func (c *Coordinator) allocUnRunningTask() (int, int) {
	mapID, reduceID := -1, -1
	mapID = c.getUnRunningMapId()
	if mapID != -1 {
		return mapID, reduceID
	}
	if !c.isMapTaskAllFinished() {
		return mapID, reduceID
	}
	reduceID = c.getUnRunningReduceId()
	return mapID, reduceID
}

// get an index of UnRunning map task
func (c *Coordinator) getUnRunningMapId() int {
	c.MapMutex.Lock()
	defer c.MapMutex.Unlock()
	for i := range c.MapStatus {
		idx := (i + c.CurMapIdx) % len(c.MapStatus)
		if c.MapStatus[idx] == TaskUnrunning {
			c.CurMapIdx = idx
			c.MapStatus[idx] = TaskRunning
			return idx
		}
	}
	return -1
}

// get an index of UnRunning Reduce task
func (c *Coordinator) getUnRunningReduceId() int {
	c.ReduceMutex.Lock()
	defer c.ReduceMutex.Unlock()
	for i := range c.ReduceStatus {
		idx := (i + c.CurReduceIdx) % len(c.ReduceStatus)
		if c.ReduceStatus[idx] == TaskUnrunning {
			c.CurReduceIdx = idx
			c.ReduceStatus[idx] = TaskRunning
			return idx
		}
	}
	return -1
}

func (c *Coordinator) appendIntermediateFiles(files []string) {
	c.IntermediateFilesMutex.Lock()
	for i := range c.IntermediateFiles {
		if i >= len(files) {
			break
		}
		c.IntermediateFiles[i] = append(c.IntermediateFiles[i], files[i])
	}
	c.IntermediateFilesMutex.Unlock()
}

// wait 
func (c *Coordinator) runTaskTimeout(t TaskType, id int) {
	timeout := time.After(10 * time.Second)
	<-timeout
	switch t {
	case MapType: {
		c.MapMutex.Lock()
		if c.MapStatus[id] != TaskFinished {
			c.MapStatus[id] = TaskUnrunning
			log.Printf("map task %d is not finished, mark it state UnRunning", id)
		}
		c.MapMutex.Unlock()
	}
	case ReduceType: {
		c.ReduceMutex.Lock()
		if c.ReduceStatus[id] != TaskFinished {
			c.ReduceStatus[id] = TaskUnrunning
			log.Printf("reduce task %d is not finished, mark it state UnRunning", id)
		}
		c.ReduceMutex.Unlock()
	}
	default:
		log.Fatal("unknown TaskType %V", t)
	}
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
	ret := true
	<-c.done
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Files = files
	c.NReduce = nReduce
	c.IntermediateFiles = make([][]string, nReduce)
	c.MapStatus = make([]TaskStatus, len(files))
	c.ReduceStatus = make([]TaskStatus, nReduce)
	c.done = make(chan struct{}, 1)
	c.server()
	return &c
}
