package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"

	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Operation   string

	Serial_Number int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	processQueue map[int64]bool // store the serial numbers for requests that have not been processed
	processedReplied map[int64]*StoredReply // store the replies that has been processed

	db map[string]string
}

type StoredReply struct {
	Err Err
	Value string
}

func (kv *KVServer) tryInitRequestQueue() {
	if kv.processQueue == nil {
		kv.processQueue = make(map[int64]bool)
	}
	return
}

func (kv *KVServer) tryDeleteRequestQueue() {
	if kv.processQueue != nil {
		kv.processQueue = nil
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	serial_number := args.Serial_Number

	//log.printf("This server %d has received Get request with key %d and serial number %d", ck.me, key, serial_number)

	if kv.killed() {
		reply.Err = ErrServerKilled
		//log.printf("This server %d has been killed", ck.me)
		return
	} 

	for i := 0; i < len(args.PrevRequests); i++ {
		serialNumberProbe := args.PrevRequests[i]
		_, okCachedProbe := kv.processedReplied[serialNumberProbe]
		if okCachedProbe {
			delete(kv.processedReplied, serialNumberProbe)
		}
	}
	// removed reply to previous rpc already finished

	term, isLeader := kv.rf.GetState()

	if !isLeader {
		//log.printf("This server %d has received Get request with key %d and serial number %d but is not leader, re route to leader %d of term %d", ck.me, key, serial_number, reply.CurrentLeaderId, reply.CurrentLeaderTerm)
		kv.tryDeleteRequestQueue()
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId, reply.CurrentLeaderTerm = kv.rf.GetCurrentLeaderIdAndTerm() 
		return
	} else {
		kv.tryInitRequestQueue()

		cachedReply, okCached := kv.processedReplied[serial_number]
		if okCached {
			reply.Err = cachedReply.Err
			reply.Value = cachedReply.Value

			reply.CurrentLeaderId = kv.me
			reply.CurrentLeaderTerm = term
			
			return
		} else {
			_, okInQueue := kv.processQueue[serial_number]
			if !okInQueue {
				opToRaft := Op{}

				opToRaft.Key = key
	
				opToRaft.Operation = "Get"
				opToRaft.Serial_Number = serial_number
				_, _, isLeader := kv.rf.Start(opToRaft)
				if !isLeader {
					kv.tryDeleteRequestQueue()
					reply.Err = ErrWrongLeader
					reply.CurrentLeaderId, reply.CurrentLeaderTerm = kv.rf.GetCurrentLeaderIdAndTerm() 
					return
				} else {
					kv.processQueue[serial_number] = true
					kv.mu.Unlock()
				}
			}

			for {
				kv.mu.Lock()
				if kv.killed() {
					reply.Err = ErrServerKilled
					return
				} 

				term, isLeader = kv.rf.GetState()
				if !isLeader {
					kv.tryDeleteRequestQueue()
					reply.Err = ErrWrongLeader
					reply.CurrentLeaderId, reply.CurrentLeaderTerm = kv.rf.GetCurrentLeaderIdAndTerm() 
					return
				} else {
					cachedReply, okCached = kv.processedReplied[serial_number]
					if okCached {
						reply.Err = cachedReply.Err
						reply.Value = cachedReply.Value
	
						reply.CurrentLeaderId = kv.me
						reply.CurrentLeaderTerm = term
						return
					} 
				}
				kv.mu.Unlock()
				time.Sleep(time.Duration(kvserver_loop_wait_time_millisecond) * time.Millisecond)
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.killed() {
		reply.Err = ErrServerKilled
		return
	} 

	for i := 0; i < len(args.PrevRequests); i++ {
		serialNumberProbe := args.PrevRequests[i]
		_, okCachedProbe := kv.processedReplied[serialNumberProbe]
		if okCachedProbe {
			delete(kv.processedReplied, serialNumberProbe)
		}
	}
	// removed reply to previous rpc already finished

	term, isLeader := kv.rf.GetState()
	
	if !isLeader {
		kv.tryDeleteRequestQueue()
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId, reply.CurrentLeaderTerm = kv.rf.GetCurrentLeaderIdAndTerm()
		return
	} else {
		kv.tryInitRequestQueue()
		key := args.Key
		value := args.Value
		op := args.Op
		serial_number := args.Serial_Number

		cachedReply, okCached := kv.processedReplied[serial_number]
		if okCached {
			reply.Err = cachedReply.Err
			return
		} else {
			_, okInQueue := kv.processQueue[serial_number]
			if !okInQueue {
				opToRaft := Op{}

				opToRaft.Key = key
				opToRaft.Value = value
				opToRaft.Operation = op
				opToRaft.Serial_Number = serial_number
				_, _, isLeader := kv.rf.Start(opToRaft)
				if !isLeader {
					kv.tryDeleteRequestQueue()
					reply.Err = ErrWrongLeader
					reply.CurrentLeaderId, reply.CurrentLeaderTerm = kv.rf.GetCurrentLeaderIdAndTerm()
					return
				} else {
					kv.processQueue[serial_number] = true
					kv.mu.Unlock()
				}
			}

			for {
				kv.mu.Lock()

				if kv.killed(){
					reply.Err = ErrServerKilled
					return
				}

				term, isLeader = kv.rf.GetState()
				if !isLeader {
					kv.tryDeleteRequestQueue()

					reply.Err = ErrWrongLeader
					reply.CurrentLeaderId, reply.CurrentLeaderTerm = kv.rf.GetCurrentLeaderIdAndTerm()
					return
				} else {
					cachedReply, okCached = kv.processedReplied[serial_number]
					if okCached {
						reply.Err = cachedReply.Err
	
						reply.CurrentLeaderId = kv.me
						reply.CurrentLeaderTerm = term
						return
					} 
				}
				kv.mu.Unlock()
				time.Sleep(time.Duration(kvserver_loop_wait_time_millisecond) * time.Millisecond)
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but itkv.rf.getCurrentLeaderId() may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// kv server changes its database state according to committed commands
// as well as handle and cache requests related to the committed commands
func (kv *KVServer) handleRequest(applyMessage raft.ApplyMsg) {
	operation := applyMessage.Command.(Op)

	key := operation.Key
	value := operation.Value
	op := operation.Operation
	serial_number := operation.Serial_Number

	_, ok := kv.processedReplied[serial_number]
	if ok {
		// it is possible that this server has received the request from heart beat in previous term
		// but has not commited, then before commit, this leader receives duplicate request from
		// client, which will also be added to queue since the request is not commited and is not 
		// in process queue of current leader
		// we need to handle this edge case by checking if this request has already been handled or not

		return
	}

	replyToStore := StoredReply{}

	if op == "Get" {
		dbvalue, ok:= kv.db[key]
		if ok {
			replyToStore.Err = OK
			replyToStore.Value = dbvalue
		} else {
			replyToStore.Err = ErrNoKey
		}
	} else if (op == "Put") {
		kv.db[key] = value
		replyToStore.Err = OK
	} else {
		dbvalue, ok:= kv.db[key]
		if ok {
			kv.db[key] = dbvalue + value
		} else {
			kv.db[key] =  value
		}
		replyToStore.Err = OK
	}
	kv.processedReplied[serial_number] = &replyToStore // cache the response in case of handling retry
	if kv.processQueue != nil {
		_, ok := kv.processQueue[serial_number]
		if ok {
			delete(kv.processQueue, serial_number) // remove request from processQueue if this server is a leader that handles the request process
		}
	}

	return
}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.processedReplied = make(map[int64]*StoredReply)
	kv.processQueue = nil

	kv.db = make(map[string]string)

	go func(kv *KVServer) {		
		for applyMessage := range kv.applyCh {
			kv.mu.Lock()
			if kv.killed(){
				kv.mu.Unlock()
				return
			} else {
				_, isLeader := kv.rf.GetState()
				if isLeader {
					kv.tryInitRequestQueue()
				} else {
					kv.tryDeleteRequestQueue()
				}
				
				kv.handleRequest(applyMessage)
				kv.mu.Unlock()
			}
		}
	}(kv)

	return kv
}