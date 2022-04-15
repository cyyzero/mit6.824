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
	"syscall"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		requestTask(mapf, reducef)
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

// request master for a task
func requestTask(mapf func(string, string) []KeyValue,
				 reducef func(string, []string) string) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		switch reply.Type {
		case MapType:
			err := doMap(reply.ID, reply.NReduce, reply.Filename, mapf)
			if err == nil {
				notifyTaskFinished(reply.Type, reply.ID)
			}
		case ReduceType:
			err := doReduce(reply.ID, reply.NMap, reducef)
			if err == nil {
				notifyTaskFinished(reply.Type, reply.ID)
			}
		case UnknownType:
			fmt.Println("there is no unrunning tasks")
			time.Sleep(1 * time.Second)
		default:
			log.Fatalf("unknown taks type %v", reply.Type)
		}
	} else {
		log.Fatal("call Coordinator.RequestTask failed");
	}
}

// read file content
func readFileContent(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", err
	}
	return string(content), nil
}

// save intermediate key/values in file mr-X-Y
func saveIntermediateKVs(kvs []KeyValue, taskID int, nReduce int) {
	kvsBucket := make([][]KeyValue, nReduce)
	for i := range kvs {
		idx := ihash(kvs[i].Key) % nReduce
		kvsBucket[idx] = append(kvsBucket[idx], kvs[i])
	}
	for i := 0; i < nReduce; i++ {
		if (len(kvsBucket[i]) == 0) {
			continue
		}
		file, err := os.CreateTemp("", "")
		if err != nil {
			log.Fatal("can't create temp file")
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvsBucket[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encoding failed %e", err)
			}
		}
		file.Close()
		newName := fmt.Sprintf("mr-%d-%d", taskID, i)
		err = os.Rename(file.Name(), newName)
		if err != nil {
			log.Fatalf("rename from %v to %v failed", file.Name(), newName)
		}
	}
}

// do map work
func doMap(taskID int, nReduce int, filename string, mapf func(string, string) []KeyValue) error {
	content, err := readFileContent(filename)
	if err != nil {
		return err
	}
	kvs := mapf(filename, content)
	saveIntermediateKVs(kvs, taskID, nReduce)
	return nil
}

// load intermediate key/values
func loadIntermediateKVs(taskID int, nMap int) []KeyValue {
	kvs := []KeyValue{}
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, taskID)
		file, err := os.Open(fileName)
		if err != nil {
			if e, ok := err.(*os.PathError); !ok || e.Err != syscall.ENOENT {
				log.Fatalf("file %v open failed %v", fileName, err)
			}
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	return kvs
}

// 
func saveFinalKVs(fileName string, kvs []KeyValue, reducef func(string, []string) string) {
	ofile, err := os.CreateTemp("", "")
	if err != nil {
		log.Fatalf("create tempfile failed, %e", err)
	}
	defer ofile.Close()
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
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
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	os.Rename(ofile.Name(), fileName)
}

// do reduce work
func doReduce(taskID int, nMap int, reducef func(string, []string) string) error {
	kvs := loadIntermediateKVs(taskID, nMap)
	sort.Sort(ByKey(kvs))
	saveFinalKVs(fmt.Sprintf("mr-out-%d", taskID), kvs, reducef)
	return nil
}

// notify master a task is finish
func notifyTaskFinished(t TaskType, id int) {
	args := FinishTaskArgs{t, id}
	reply := FinishTaskReply{}

	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		log.Fatal("call Coordinator.FinishTask failed")
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
