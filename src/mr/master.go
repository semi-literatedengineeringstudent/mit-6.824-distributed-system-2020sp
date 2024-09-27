package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"


// Your definitions here.
type Master struct {
	mapDone map[int]int
	reduceDone map[int]int
	mapScheduledTime map[int] time.Time
	reduceScheduledTime map[int] time.Time
	remainingMap int
	remainingReduce int
	filenames []string
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func(m *Master) scheduleTask(reply *RpcReply, taskType int, taskNumber int) {
	
	if taskType == map_task {
		reply.TaskType = map_task
		reply.TaskNumber = taskNumber
		reply.Filename = m.filenames[taskNumber]
		reply.NReduce = len(m.reduceDone)
	
		m.mapDone[taskNumber] = task_in_progress
		m.mapScheduledTime[taskNumber] = time.Now()
		log.Printf("Map task %d scheduled at %s", taskNumber, m.mapScheduledTime[taskNumber].String())

	} else if taskType == reduce_task {
		reply.TaskType = reduce_task
		reply.TaskNumber = taskNumber
		reply.Filename = ""
		reply.NReduce = len(m.reduceDone)

		m.reduceDone[taskNumber] = task_in_progress
		m.reduceScheduledTime[taskNumber] = time.Now()
		log.Printf("Reduce task %d scheduled at %s", taskNumber, m.reduceScheduledTime[taskNumber].String())


	} else if taskType == none_task {
		reply.TaskType = none_task
		reply.TaskNumber = taskNumber
		reply.Filename = ""
		reply.NReduce = len(m.reduceDone)
	} else {
		reply.TaskType = exit_task
		reply.TaskNumber = taskNumber
		reply.Filename = ""
		reply.NReduce = len(m.reduceDone)
	}
	
	return
}

func (m *Master) shouldSchedule(taskType int, taskNumber int) bool {
	doneToCheck := m.mapDone
	timeToCheck := m.mapScheduledTime
	if taskType == reduce_task {
		doneToCheck = m.reduceDone
		timeToCheck = m.reduceScheduledTime
	}
	
	if doneToCheck[taskNumber] == task_unscheduled {
		return true
	} else if doneToCheck[taskNumber] == task_in_progress {
		timeToCheck := (timeToCheck[taskNumber]).Add(time.Duration(master_wait_time_second) * time.Second)	
		currentTime := time.Now()
		if currentTime.After(timeToCheck) {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
	
}

func (m *Master) RpcHandler(args *RpcArgs, reply *RpcReply) error {
	m.mu.Lock();
	defer m.mu.Unlock();
	if args.WorkerState == worker_idle {
		if m.remainingMap != 0 {
			for i, _ := range m.mapDone {
				if m.shouldSchedule(map_task, i) {
					m.scheduleTask(reply, map_task, i)
					return nil
				}
			}
		} else{
			for j, _ := range m.reduceDone {
				if m.shouldSchedule(reduce_task, j) {
					m.scheduleTask(reply, reduce_task, j)
					return nil
				}
			}
		}
		if m.remainingMap == 0 && m.remainingReduce == 0 {
			m.scheduleTask(reply, exit_task, 0)
			log.Printf("every thing is done, every one can exit")
		} else {
			m.scheduleTask(reply, none_task, 0)
			log.Printf("No task is scheduled for this idle worker")
		}
	} else {
		completedTaskType := args.TaskType
		completeTaskNumber := args.TaskNumber
		if m.remainingMap == 0 && m.remainingReduce == 0 {
			m.scheduleTask(reply, exit_task, 0)
			log.Printf("every thing is done, every one can exit")
		} else {
			if completedTaskType == map_task {
				if !args.Committed {
					if m.mapDone[completeTaskNumber] != task_completed {
						reply.ShouldCommit = true
					} else {
						reply.ShouldCommit = false
					}
					
				} else {
					m.mapDone[completeTaskNumber] = task_completed
					m.remainingMap = m.remainingMap - 1
					m.scheduleTask(reply, none_task, 0)
				}
			} else {
				if !args.Committed {
					if m.reduceDone[completeTaskNumber] != task_completed {
						reply.ShouldCommit = true
					} else {
						reply.ShouldCommit = false
					}
					
				} else {
					m.reduceDone[completeTaskNumber] = task_completed
					m.remainingReduce = m.remainingReduce - 1
					m.scheduleTask(reply, none_task, 0)
				}
			}
		}

	}
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.remainingMap == 0 && m.remainingReduce == 0 {
		ret = true
		log.Printf("task done, close master")
		defer os.Exit(0)
	}
	
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
	m.mapDone = make(map[int]int)
	m.reduceDone = make(map[int]int)
	m.mapScheduledTime = make(map[int]time.Time)
	m.reduceScheduledTime = make(map[int]time.Time)
	for i := 0; i < len(files); i++ {
		m.mapDone[i] = task_unscheduled
		m.mapScheduledTime[i] = time.Now()
	}

	for j := 0; j < nReduce; j++ {
		m.reduceDone[j] = task_unscheduled
		m.reduceScheduledTime[j] = time.Now()
	}
	m.remainingMap = len(files)
	m.remainingReduce = nReduce

	m.filenames = files

	m.server()
	return &m
}
