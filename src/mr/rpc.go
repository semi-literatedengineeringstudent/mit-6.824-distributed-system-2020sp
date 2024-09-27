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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RpcArgs struct {
	WorkerState int // either idle or complete when finish a task
	TaskType int // either none (if idle) or map or reduce (once completed)
	TaskNumber int //number corresponding to the task
	Committed bool
}

type RpcReply struct {
	TaskType int // the task type assigned from master to worker, none if all tasks has either in_process or completed state, if a task is un_scheduled, 
	TaskNumber int
	Filename string
	NReduce int
	ShouldCommit bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
