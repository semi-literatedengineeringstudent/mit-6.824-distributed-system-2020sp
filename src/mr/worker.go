package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "strconv"

import "sort"

import "io/ioutil"

import "time"

import "os"

import "encoding/json"

import "path/filepath"


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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	pid := os.Getpid()

	for {
		args := RpcArgs{}
		args.WorkerState = worker_idle
		args.TaskType = none_task
		args.TaskNumber = -1

		reply := RpcReply{}

		receivedReply := call("Master.RpcHandler", &args, &reply)
		if !receivedReply {
			log.Printf("master is dead, worker %d out", pid)
			os.Exit(0) 
		}
	
		if reply.TaskType == map_task {
			log.Printf("worker %d receives map task %d", pid, reply.TaskNumber)
			perfromMapTask(reply.TaskNumber, reply.Filename, reply.NReduce, mapf)
			time.Sleep(time.Duration(worker_request_interval_millisecond) * time.Millisecond)

		} else if reply.TaskType == reduce_task {
			log.Printf("worker %d receives reduce task %d", pid, reply.TaskNumber)
			performReduceTask(reply.TaskNumber, reducef)
			time.Sleep(time.Duration(worker_request_interval_millisecond) * time.Millisecond)

		} else if reply.TaskType == none_task{
			time.Sleep(time.Duration(worker_request_interval_millisecond) * time.Millisecond)
		} else {
			log.Printf("master is done, worker %d out", pid)
			os.Exit(0)
		}
	}
}

func perfromMapTask(taskNumber int, fileName string, nReduce int, mapf func(string, string) []KeyValue){
	intermediate := []KeyValue{}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	
	bucket := make(map[int][]KeyValue)
	for i := 0; i < nReduce; i++ {
		bucket[i] = []KeyValue{}
	}
	for _, kv := range intermediate {
		reduceGroup := ihash(kv.Key) % nReduce
		bucket[reduceGroup] = append(bucket[reduceGroup], kv)
	}
	tempFiles := []*os.File{}
	filesToWrite := []string{}
	for j := 0; j < nReduce; j++{
		fileToWrite := "mr" + "-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(j) + ".json"
		tempFile, errTempCreate := ioutil.TempFile("", "example-*.json")
		if errTempCreate != nil {
			log.Fatalf("cannot open or create temp intermediate json file for %v", fileToWrite)
		}
		enc := json.NewEncoder(tempFile)

		for _, kv := range bucket[j] {
			errEncode := enc.Encode(&kv)
			if errEncode != nil {
				log.Fatalf("cannot append key value pair to json file (%v, %v)", kv.Key, kv.Value)
			}
		}
		errCloseBeforeEnlist := tempFile.Close()
		if errCloseBeforeEnlist != nil {
			log.Fatalf("Error closing temporary file %v that is candidate of %v before putting into list:", tempFile.Name(), fileToWrite)
		}
		tempFiles = append(tempFiles, tempFile)
		filesToWrite = append(filesToWrite, fileToWrite)
	}
	commitIfInComplete(tempFiles, filesToWrite, map_task, taskNumber)

	return
}

func performReduceTask(taskNumber int, reducef func(string, []string) string) {
	reduceTaskNumber := strconv.Itoa(taskNumber)
	fileLists, err := findFiles(".",  reduceTaskNumber)
	if err != nil {
		log.Fatalf("cannot grab files for reduce task %v", taskNumber)
	}

	intermediate := []KeyValue{}
	for _, file := range fileLists {
		mapFileToRead, errReadFile := os.Open(file)
		if errReadFile != nil {
			log.Fatalf("cannot grab the file %v for reduce task %v", file, taskNumber)
		}
		dec := json.NewDecoder(mapFileToRead)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		errCloseFile := mapFileToRead.Close()
		if errCloseFile!= nil {
			log.Fatalf("cannot close the file %v for reduce task %v", file, taskNumber)
		}
	}
	sort.Sort(ByKey(intermediate))

	fileToWrite := "mr-out-" +  strconv.Itoa(taskNumber)

	tempFile, errTempCreate := ioutil.TempFile("", "example-*.json")
	if errTempCreate != nil {
		log.Fatalf("cannot create temporary file for reduce task %v", taskNumber)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	errCloseBeforeEnlist := tempFile.Close()
	if errCloseBeforeEnlist != nil {
		log.Fatalf("Error closing temporary file %v that is candidate of %v before putting into list:", tempFile.Name(), fileToWrite)
	}
	tempFiles := []*os.File{}
	filesToWrite := []string{}
	tempFiles = append(tempFiles, tempFile)
	filesToWrite = append(filesToWrite, fileToWrite)

	commitIfInComplete(tempFiles, filesToWrite, reduce_task, taskNumber)

	return
}

func commitIfInComplete(tempFiles []*os.File, filesToWrite []string, taskType int, taskNumber int){
	pid := os.Getpid()

	argsBeforeCommit := RpcArgs{}
	argsBeforeCommit.WorkerState = worker_complete
	argsBeforeCommit.TaskType = taskType
	argsBeforeCommit.TaskNumber = taskNumber
	argsBeforeCommit.Committed = false

	replyBeforeCommit  := RpcReply{}

	receivedReplyBeforeCommit := call("Master.RpcHandler", &argsBeforeCommit , &replyBeforeCommit)
	if !receivedReplyBeforeCommit  {
		log.Printf("master is dead, worker %d out", pid)
		os.Exit(0) 
	}
	if replyBeforeCommit.TaskType == exit_task {
		log.Printf("master is done with work, worker %d out", pid)
		os.Exit(0) 
	}

	if replyBeforeCommit.ShouldCommit{
		for i, tempFile := range tempFiles {
			defer tempFile.Close()
			fileToWrite :=  filesToWrite[i]
			errRename := os.Rename(tempFile.Name(), fileToWrite)
			if errRename != nil {
				log.Fatalf("cannot rename tempFile %v to %v when commiting", tempFile.Name(), fileToWrite)
			}
			
		}

		argsAfterCommit := RpcArgs{}
		argsAfterCommit.WorkerState = worker_complete
		argsAfterCommit.TaskType = taskType
		argsAfterCommit.TaskNumber = taskNumber
		argsAfterCommit.Committed = true
	
		replyAfterCommit := RpcReply{}

		receivedReplyAfterCommit := call("Master.RpcHandler", &argsAfterCommit, &replyAfterCommit)
		if !receivedReplyAfterCommit {
			log.Printf("master is dead, worker %d out", pid)
			os.Exit(0) 
		}
		if replyAfterCommit.TaskType == exit_task {
			log.Printf("master is done with work, worker %d out", pid)
			os.Exit(0) 
		}
		
	} else {
		for i, tempFile := range tempFiles {
			defer tempFile.Close()
			fileToWrite := filesToWrite[i]
			errRemovingIfAlreadyCommitted := os.Remove(tempFile.Name())
			if errRemovingIfAlreadyCommitted  != nil {
				log.Fatalf("Error removing temporary file %v for %v given target already commited", tempFile.Name(), fileToWrite)
			}
			
		}
		if taskType == map_task {
			log.Printf("no need to commit map task %d by worker %d ", taskNumber, pid)
		} else {
			log.Printf("no need to commit reduce task %d by worker %d ", taskNumber, pid)
		}
		
	}

	return
}

func findFiles(dir, match string) ([]string, error) {
	// Use filepath.Glob to get files matching the first part of the pattern
	pattern := "mr-*-" + match + ".json"
	files, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return nil, err
	}

	return files, nil
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
