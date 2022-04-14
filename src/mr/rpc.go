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
//

const (
	MapType TaskType = iota
	ReduceType
	UnknownType
)

type TaskType int

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Type TaskType
	ID int
	NReduce int
	Filename string
	NMap int
}

type FinishTaskArgs struct {
	Type TaskType
	ID int
}

type FinishTaskReply struct {
	RequestTaskReply
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
