package kvraft

import (
	"../labgob"
	"bytes"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"

	"time"

	"math"
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
	Sequence_Number int // index of op the client submit, i means ith operation 
	// (indicating client has received respone for all ops from 1 to i-1)

	Client_Serial_Number int64 // serial_number for client who initate this op, used in conjunction with Sequence_Number
	// for duplication detection

	Key   string
	Value string
	Operation   string
}

type Client struct {
	Received_Sequence_Number int // highest sequence number of op whose response has been received by the client, 
	// so all op with sequence from 1 to Received_Sequence can be deleted since client already has the sequence

	Last_Processed_Sequence_Number int // the sequence number of last operation executed by server,
	// all op with seq_num <= Last_Processed_Sequence_Number should not be executed since they have been executed already  by This kvserver

	Cached_Response map[int] *StoredReply
}

type SnapshotCommand struct {
	LastIncludedIndex int // the index of command for last operation that is executed by the server
	LastIncludedTerm int // the term of command for last operation that is executed by the server

	DbState map[string]string
	Clients_Info map[int64]*Client //map from client serial number to its state pertaining cached responses
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	clients_Info map[int64]*Client //map from client serial number to its state pertaining cached responses
	db map[string]string

	lastIncludedIndex int
	lastIncludedTerm int

	operationBuffer []Op // stores the operations whose index is not 
	// one above lastIncludedIndex, indicating that the raft is outdated and the raft should be waiting for snapshot from the leader
	indexBuffer []int // stores index corresponding to operations in operationBuffer
	termBuffer []int // stores term corresponding to operations in operarionBuffer
}

type StoredReply struct {
	Err string
	Value string
}

func (kv *KVServer) tryInitSnapShot() {


	_, isLeader := kv.rf.GetState()

	if !isLeader {
		return
	} 

	if kv.maxraftstate == -1 {
		return
	}

	LastIncludedIndex := kv.lastIncludedIndex
	LastIncludedTerm := kv.lastIncludedTerm
	//log.Printf("KvServer %d init snapshot with LastIncludedIndex %d, LastIncludedTerm %d", kv.me, LastIncludedIndex, LastIncludedTerm)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clients_Info)
	e.Encode(kv.db)

	SnapShotByte := w.Bytes()

	kv.rf.InitInstallSnapshot(LastIncludedIndex, LastIncludedTerm, SnapShotByte)

	
	return


}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key

	Client_Serial_Number := args.Client_Serial_Number
	Sequence_Number := args.Sequence_Number

	client_Info_This, ok := kv.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This kvserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		client_To_Add.Last_Processed_Sequence_Number = default_sentinel_index // well This kvserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		kv.clients_Info[Client_Serial_Number] = &client_To_Add
		client_Info_This = kv.clients_Info[Client_Serial_Number]

	} else {
		// we have this client on file, we can simply delete all requests with 
		// sequence number < sequence number of current request
		for seq_Num, _ := range client_Info_This.Cached_Response {
			if seq_Num < Sequence_Number {
				delete(client_Info_This.Cached_Response, seq_Num)
			}
		}
		// we know all requests up to Sequence_Number - 1 has been received by the client so we need to update Received sequence number as well
		client_Info_This.Received_Sequence_Number = int(math.Max(float64(client_Info_This.Received_Sequence_Number), float64(Sequence_Number - 1)))
		// due to asychronous network, it is possible that the older request arrives This kvserver as result of re routing, but This kvserver already 
		// receives snapshot from previous leader that has handled this request
	}
	

	//log.Printf("This kvserver %d has received Get request with key %s and serial number %d from clerk %d", kv.me, key, Sequence_Number, Client_Serial_Number)

	if kv.killed() {
		reply.Err = ErrServerKilled
		//log.Printf("This kvserver %d has been killed", kv.me)
		return
	} 

	// removed reply to previous rpc already finished

	term, isLeader, currentLeaderId, serverRole := kv.rf.GetStateWTF()

	if !isLeader {
		//log.Printf("This kvserver %d (term %d) has received Get request with key %s and serial number %d but is not leader, re route to leader %d of term %d", kv.me, term, key, Sequence_Number, currentLeaderId, term)
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId = currentLeaderId
		reply.CurrentLeaderTerm = term
		reply.ServerRole = serverRole
		return
	} else {

		Client_Received_Sequence_Number := client_Info_This.Received_Sequence_Number
		Client_Last_Processed_Sequence_Number := client_Info_This.Last_Processed_Sequence_Number

		if Sequence_Number <= Client_Received_Sequence_Number {
			// dude the client has already received reply, so that reply is just staled and we don't need to do 
			// anything about it
			return
		} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
			// good, that means cached reply is still in the dictionary
			cachedReply := client_Info_This.Cached_Response[Sequence_Number]

			reply.Err = cachedReply.Err
			reply.Value = cachedReply.Value

			reply.CurrentLeaderId = kv.me
			reply.CurrentLeaderTerm = term

			//log.Printf("This kvserver %d (term %d) has cached result for Get request with key %s, client: %d, seq_num: %d", kv.me, term, key, Client_Serial_Number, Sequence_Number)
			
			return
		} else {
	
			opToRaft := Op{}

			opToRaft.Sequence_Number = Sequence_Number
			opToRaft.Client_Serial_Number = Client_Serial_Number

			opToRaft.Key = key
			opToRaft.Operation = "Get"

			//_, _, isLeader := kv.rf.Start(opToRaft)
	
			currentLeaderId, index, term, isLeader := kv.rf.StartQuick(opToRaft)

	
			if index == invalid_index {
				reply.Err = ErrServerKilled
				return
			}
			if !isLeader {
				//log.Printf("This kvserver %d (term %d) has cached result for Get request with key %s and serial number %d but is not leader, re route to leader %d of term %d", kv.me, term, key, Sequence_Number, currentLeaderId, term)
				reply.Err = ErrWrongLeader
				reply.CurrentLeaderId, reply.CurrentLeaderTerm = currentLeaderId, term 
				return
			} else {
				/*if kv.maxraftstate != -1 {
				
					snapShotSize := kv.rf.GetRaftStateSize()
				
					if snapShotSize >= kv.maxraftstate {
						//log.Printf("kvserver %d make snapshot in Get with LastIncludeIndex %d and LastIncludeTerm %d", kv.me, kv.LastIncludedIndex, kv.LastIncludedTerm)
						kv.tryInitSnapShot()
					}
				}*/
				//log.Printf("This kvserver %d (term %d) does not have cached result for Get request with key %s and serial number %d and is a  leader, now enqueue", kv.me, term, key, Sequence_Number)
				kv.mu.Unlock()
			}
			
			for {
				//log.Printf("Kvserver get before lock")
				kv.mu.Lock()
				//log.Printf("Kvserver %d (term %d) get wtf", kv.me, term)
				//log.Printf("Kvserver %d (term %d) get locked", kv.me, term)
				if kv.killed() {
					//log.Printf("This kvserver %d (term %d) has been killed", kv.me, term)
					reply.Err = ErrServerKilled
					return
				} 
				//log.Printf("Kvserver %d (term %d) Get GetStateWtf init", kv.me, term)

				term, isLeader, currentLeaderId, serverRole = kv.rf.GetStateWTF()
		
				//log.Printf("Kvserver %d (term %d) Get GetStateWtf finished", kv.me, term)
				if !isLeader {
					//log.Printf("This kvserver %d (term %d) has received Get request with key %s and serial number %d but is not leader, re route to leader %d of term %d", kv.me, term, key, Sequence_Number, currentLeaderId, term)
				
					reply.Err = ErrWrongLeader
					reply.CurrentLeaderId = currentLeaderId
					reply.CurrentLeaderTerm = term
					reply.ServerRole = serverRole
					return
				} else {
					client_Info_This = kv.clients_Info[Client_Serial_Number]

					Client_Received_Sequence_Number = client_Info_This.Received_Sequence_Number
					Client_Last_Processed_Sequence_Number = client_Info_This.Last_Processed_Sequence_Number
					//log.Printf("Kvserver %d (term %d), for client %d, Get task with sequence number %d, Client_Received_Sequence_Number %d, Client_Last_Processed_Sequence_Number %d", kv.me, term, Client_Serial_Number, Sequence_Number, Client_Received_Sequence_Number, Client_Last_Processed_Sequence_Number)
					if Sequence_Number <= Client_Received_Sequence_Number {
						// dude the client has already received reply, so that reply is just staled and we don't need to do 
						// anything about it
						//log.Printf("Kvserver %d (term %d) wtf2", kv.me, term)
						return
					} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
						// good, that means cached reply is still in the dictionary
						cachedReply := client_Info_This.Cached_Response[Sequence_Number]
			
						reply.Err = cachedReply.Err
						reply.Value = cachedReply.Value
			
						reply.CurrentLeaderId = kv.me
						reply.CurrentLeaderTerm = term
			
						//log.Printf("This kvserver %d (term %d) has cached result for Get request with key %s, client: %d, seq_num: %d", kv.me, term, key, Client_Serial_Number, Sequence_Number)
						
						return
					} else {
						//log.Printf("This kvserver %d (term %d) does not have cached result for Get request with key %s, client: %d, seq_num: %d, keep waiting...", kv.me, term, key, Client_Serial_Number, Sequence_Number)
						//log.Printf("Kvserver %d (term %d) get Unlocked", kv.me, term)
						kv.mu.Unlock()
					}
				}
				
				time.Sleep(time.Duration(kvserver_loop_wait_time_millisecond) * time.Millisecond)
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	op := args.Op

	Client_Serial_Number := args.Client_Serial_Number
	Sequence_Number := args.Sequence_Number

	client_Info_This, ok := kv.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This kvserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		client_To_Add.Last_Processed_Sequence_Number = default_sentinel_index // well This kvserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		kv.clients_Info[Client_Serial_Number] = &client_To_Add
		client_Info_This = kv.clients_Info[Client_Serial_Number]

	} else {
		// we have this client on file, we can simply delete all requests with 
		// sequence number < sequence number of current request

		//log.Printf("number of cached response for client %d before deletion is %d", Client_Serial_Number, len(client_Info_This.Cached_Response))

		for seq_Num, _ := range client_Info_This.Cached_Response {
			if seq_Num < Sequence_Number {
				delete(client_Info_This.Cached_Response, seq_Num)
			}
		}
		//log.Printf("number of cached response for client %d after deletion is %d", Client_Serial_Number, len(client_Info_This.Cached_Response))
		// we know all requests up to Sequence_Number - 1 has been received by the client so we need to update Received sequence number as well
		client_Info_This.Received_Sequence_Number = int(math.Max(float64(client_Info_This.Received_Sequence_Number), float64(Sequence_Number - 1)))
		// due to asychronous network, it is possible that the older request arrives This kvserver as result of re routing, but This kvserver already 
		// receives snapshot from previous leader that has handled this request
	}
	

	//log.Printf("This kvserver %d has received Get request with key %s and serial number %d from clerk %d", kv.me, key, Sequence_Number, Client_Serial_Number)

	if kv.killed() {
		reply.Err = ErrServerKilled
		//log.Printf("This kvserver %d has been killed", kv.me)
		return
	} 

	// removed reply to previous rpc already finished
	term, isLeader, currentLeaderId, serverRole := kv.rf.GetStateWTF()

	if !isLeader {
		//log.Printf("This kvserver %d (term %d) has received Get request with key %s and serial number %d but is not leader, re route to leader %d of term %d", kv.me, term, key, Sequence_Number, currentLeaderId, term)
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId = currentLeaderId
		reply.CurrentLeaderTerm = term
		reply.ServerRole = serverRole
		return
	} else {
		Client_Received_Sequence_Number := client_Info_This.Received_Sequence_Number
		Client_Last_Processed_Sequence_Number := client_Info_This.Last_Processed_Sequence_Number

		if Sequence_Number <= Client_Received_Sequence_Number {
			// dude the client has already received reply, so that reply is just staled and we don't need to do 
			// anything about it
			return
		} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
			// good, that means cached reply is still in the dictionary
			cachedReply := client_Info_This.Cached_Response[Sequence_Number]

			reply.Err = cachedReply.Err

			reply.CurrentLeaderId = kv.me
			reply.CurrentLeaderTerm = term

			//log.Printf("This kvserver %d (term %d) has cached result for Get request with key %s, client: %d, seq_num: %d", kv.me, term, key, Client_Serial_Number, Sequence_Number)
			
			return
		} else {
	
			opToRaft := Op{}

			opToRaft.Sequence_Number = Sequence_Number
			opToRaft.Client_Serial_Number = Client_Serial_Number

			opToRaft.Key = key
			opToRaft.Value = value
			opToRaft.Operation = op

			//_, _, isLeader := kv.rf.Start(opToRaft)
	
			currentLeaderId, index, term, isLeader := kv.rf.StartQuick(opToRaft)
	

			if index == invalid_index {
				reply.Err = ErrServerKilled
				return
			}
			if !isLeader {
				//log.Printf("This kvserver %d (term %d) has cached result for Get request with key %s and serial number %d but is not leader, re route to leader %d of term %d", kv.me, term, key, Sequence_Number, currentLeaderId, term)
				reply.Err = ErrWrongLeader
				reply.CurrentLeaderId, reply.CurrentLeaderTerm = currentLeaderId, term
				return
			} else {
				/*if kv.maxraftstate != -1 {
				
					snapShotSize := kv.rf.GetRaftStateSize()
				
					if snapShotSize >= kv.maxraftstate {
						//log.Printf("kvserver %d make snapshot in PutAppend with LastIncludeIndex %d and LastIncludeTerm %d", kv.me, kv.LastIncludedIndex, kv.LastIncludedTerm)
						kv.tryInitSnapShot()
					}
				}*/
				//log.Printf("This kvserver %d (term %d) does not have cached result for Get request with key %s and serial number %d but is not leader, now enqueue", kv.me, term, key, Sequence_Number)
				kv.mu.Unlock()
			}
			
			for {

				//log.Printf("Kvserver before lock")
				kv.mu.Lock()
				//log.Printf("Kvserver %d putappend locked ", kv.me)
				if kv.killed() {
					//log.Printf("This kvserver %d has been killed", kv.me)
					reply.Err = ErrServerKilled
					return
				} 
				//log.Printf("Kvserver %d putappend GetStateWtf init", kv.me)

			
				term, isLeader, currentLeaderId, serverRole = kv.rf.GetStateWTF()
			

				//log.Printf("Kvserver %d (term %d) putappend GetStateWtf finished", kv.me, term)
				if !isLeader {
					//log.Printf("This kvserver %d (term %d) has received Get request with key %s and serial number %d but is not leader, re route to leader %d of term %d", kv.me, term, key, Sequence_Number, currentLeaderId, term)
		
					reply.Err = ErrWrongLeader
					reply.CurrentLeaderId = currentLeaderId
					reply.CurrentLeaderTerm = term
					reply.ServerRole = serverRole
					return
				} else {
					client_Info_This = kv.clients_Info[Client_Serial_Number]

					Client_Received_Sequence_Number = client_Info_This.Received_Sequence_Number
					Client_Last_Processed_Sequence_Number = client_Info_This.Last_Processed_Sequence_Number

					//log.Printf("Kvserver %d (term %d), for client %d, putappend task with sequence number %d, Client_Received_Sequence_Number %d, Client_Last_Processed_Sequence_Number %d", kv.me, term, Client_Serial_Number, Sequence_Number, Client_Received_Sequence_Number, Client_Last_Processed_Sequence_Number)

					if Sequence_Number <= Client_Received_Sequence_Number {
						// dude the client has already received reply, so that reply is just staled and we don't need to do 
						// anything about it
						//log.Printf("Kvserver %d (term %d) wtf2", kv.me, term)
						return
					} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
						// good, that means cached reply is still in the dictionary
						cachedReply := client_Info_This.Cached_Response[Sequence_Number]
			
						reply.Err = cachedReply.Err
			
						reply.CurrentLeaderId = kv.me
						reply.CurrentLeaderTerm = term
			
						//log.Printf("This kvserver %d (term %d) has cached result for Get request with key %s, client: %d, seq_num: %d", kv.me, term, key, Client_Serial_Number, Sequence_Number)
						
						return
					} else {
						//log.Printf("This kvserver %d (term %d) does not cached result for Get request with key %s, client: %d, seq_num: %d, keep waiting...", kv.me, term, key, Client_Serial_Number, Sequence_Number)
						//log.Printf("Kvserver %d (term %d) putappend Unlocked", kv.me, term)
						kv.mu.Unlock()
					}
				}
				
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
// about this, but it may be convenient (for example)
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
func (kv *KVServer) emptyOperationBuffer() {
	if (len(kv.operationBuffer) == 0) {
		//nothing in the buffer...
		return
	} 
	opBufferLowerBound := kv.indexBuffer[0]
	opBufferUpperBound := kv.indexBuffer[len(kv.indexBuffer) - 1]
	if (opBufferUpperBound <= kv.lastIncludedIndex) {
		kv.operationBuffer = make([]Op, 0)
		kv.indexBuffer = make([]int, 0)
		kv.termBuffer = make([]int, 0)

		//everything in the buffer has already been applied...
		return
	}

	if (opBufferLowerBound > kv.lastIncludedIndex + 1) {
		//there is a gap between current state machine index and opBuffer index, so we wait for snapshot to fill the gap
		//log.Printf("Kvserver %d, opbufferLowerBound %d, opbufferUpperBound %d, lastIncludeIndex %d, there is a gap, wait for snapshot to fill the gap", kv.me, opBufferLowerBound, opBufferUpperBound, kv.lastIncludedIndex)
		return
	}
	for i := 0; i < len(kv.operationBuffer); i++ {
		commandIndex := kv.indexBuffer[i]
		commandTerm := kv.termBuffer[i]
		if (commandIndex == kv.lastIncludedIndex + 1) {
			operation := kv.operationBuffer[i]
			//log.Printf("Kvserver %d, having LastIncludeIndex %d, applies operation with commandIndex %d, commandTerm %d from emptyOperationBuffer", kv.me, kv.lastIncludedIndex, commandIndex, commandTerm)
			kv.applyOperation(operation)
			kv.lastIncludedIndex = commandIndex
			kv.lastIncludedTerm = commandTerm
		} else {
			//log.Printf("Kvserver %d, having LastIncludeIndex %d, cannot apply operation with commandIndex %d, commandTerm %d from emptyOperationBuffer", kv.me, kv.lastIncludedIndex, commandIndex, commandTerm)
		}
	}
	kv.operationBuffer = make([]Op, 0)
	kv.indexBuffer = make([]int, 0)
	kv.termBuffer = make([]int, 0)
	return
}
func(kv *KVServer) applyOperation(operation Op) {

	Sequence_Number := operation.Sequence_Number
	Client_Serial_Number := operation.Client_Serial_Number

	key := operation.Key
	value := operation.Value
	op := operation.Operation

	client_Info_This, ok := kv.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This kvserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		// but in this case, this is certainly command for op with seq_num 1
		client_To_Add.Last_Processed_Sequence_Number = default_sentinel_index // well This kvserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		kv.clients_Info[Client_Serial_Number] = &client_To_Add

		client_Info_This = kv.clients_Info[Client_Serial_Number] 

	}
	last_Processed_Sequence_Number := client_Info_This.Last_Processed_Sequence_Number
	if Sequence_Number <= last_Processed_Sequence_Number {
		// if sequence number for this op is <= seq num of last op for this client the
		// server has processed, we do not want to re peat execution
		return
	}

	if Sequence_Number != last_Processed_Sequence_Number + 1 {
		// to ensure linearizability
		// only process a request if current request's sequence number is 1 above previous op done on current client
		return
	}



	replyToStore := StoredReply{}

	if op == "Get" {
		dbvalue, ok:= kv.db[key]
		if ok {
			//log.Printf("This kvserver %d is caching result for Get request with key %s and serial number %d, cached value is %s", kv.me, key, Sequence_Number, dbvalue)
			replyToStore.Err = OK
			replyToStore.Value = dbvalue
		} else {
			//log.Printf("This kvserver %d is caching result for Get request with key %s and serial number %d, there is no key so return ErrNoKey", kv.me, key, Sequence_Number)
			replyToStore.Err = ErrNoKey
		}
	} else if (op == "Put") {
		//log.Printf("This kvserver %d is caching result for Put request with key %s and serial number %d, cached value is %s", kv.me, key, Sequence_Number, value)
		kv.db[key] = value
		replyToStore.Err = OK
		replyToStore.Value = empty_string
	} else {
		
		dbvalue, ok:= kv.db[key]
		if ok {
			kv.db[key] = dbvalue + value
			//log.Printf("This kvserver %d is caching result for Append request with key %s and serial number %d, cached value is %s", kv.me, key, Sequence_Number, dbvalue + value)
		} else {
			kv.db[key] =  value
			//log.Printf("This kvserver %d is caching result for Append request with key %s and serial number %d, cached value is %s", kv.me, key, Sequence_Number, value)
		}
		replyToStore.Err = OK
		replyToStore.Value = empty_string
	}
	kv.clients_Info[Client_Serial_Number].Cached_Response[Sequence_Number] = &replyToStore // cache the response in case of handling retry
	kv.clients_Info[Client_Serial_Number].Last_Processed_Sequence_Number = Sequence_Number	

}
// kv server changes its database state according to committed commands
// as well as handle and cache requests related to the committed commands
func (kv *KVServer) handleRequest(applyMessage raft.ApplyMsg) {

	if applyMessage.CommandValid {
		commandIndex := applyMessage.CommandIndex
		commandTerm := applyMessage.CommandTerm
		operation := applyMessage.Command.(Op)
		if (commandIndex == kv.lastIncludedIndex + 1) {
			//log.Printf("Kvserver %d applies operation with commandIndex %d from handleRequest with LastIncludeIndex %d, LastIncludeTerm %d", kv.me, commandIndex, kv.lastIncludedIndex, kv.lastIncludedTerm)
			kv.lastIncludedIndex = commandIndex
			kv.lastIncludedTerm = commandTerm
			kv.applyOperation(operation)
		} else {
			//log.Printf("Kvserver %d put operation with LastIncludeIndex %d, LastIncludeTerm %d into buffer", kv.me, kv.lastIncludedIndex, kv.lastIncludedTerm)
			kv.operationBuffer = append(kv.operationBuffer, operation)
			kv.indexBuffer = append(kv.indexBuffer, commandIndex)
			kv.termBuffer = append(kv.termBuffer, commandTerm)
		}
		return
	} else {
		LastIncludedIndex := applyMessage.LastIncludedIndex
		LastIncludedTerm := applyMessage.LastIncludedTerm
		if (LastIncludedIndex > kv.lastIncludedIndex) {
			r := bytes.NewBuffer(applyMessage.SnapShotByte)
			d := labgob.NewDecoder(r)

			var clients_Info map[int64]*Client //map from client serial number to its state pertaining cached responses
			var db map[string]string

			if d.Decode(&clients_Info) != nil ||
			   d.Decode(&db) != nil {
				//log.Printf("Kvserver %d could not read snapshot from raft. There is error in reading.", kv.me)
				return
			} else {
				//log.Printf("Kvserver %d, having LastIncludeIndex %d and lastIncludeTerm %d, reads snapshot from raft with LastIncludeIndex %d, LastIncludeTerm %d.", kv.me, kv.lastIncludedIndex, kv.lastIncludedIndex, LastIncludedIndex, LastIncludedTerm)
				kv.lastIncludedIndex = LastIncludedIndex
				kv.lastIncludedTerm = LastIncludedTerm
				kv.clients_Info = clients_Info
				kv.db = db
				
				kv.emptyOperationBuffer()
			}

		}

	}
	
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

	//labgob.Register(Client{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeWithSnapshot(servers, me, persister, kv.applyCh, maxraftstate)

	// You may need initialization code here.

	if maxraftstate != -1 {
		lastIncludedIndex, lastIncludedTerm, snapShotByte := kv.rf.SendSnapShotToKvServer() 
		r := bytes.NewBuffer(snapShotByte)
		d := labgob.NewDecoder(r)

		var clients_Info map[int64]*Client //map from client serial number to its state pertaining cached responses
		var db map[string]string

		if d.Decode(&clients_Info) != nil ||
			d.Decode(&db) != nil {
			//log.Printf("could not read snapshot from raft for This kvserver %d. There is error in reading.", kv.me)
			kv.lastIncludedIndex = default_sentinel_index
			kv.lastIncludedTerm = default_start_term

			kv.clients_Info = make(map[int64]*Client)
			kv.db = make(map[string]string)
			
		} else {
			kv.lastIncludedIndex = lastIncludedIndex
			kv.lastIncludedTerm = lastIncludedTerm
			kv.clients_Info = clients_Info
			kv.db = db
			kv.emptyOperationBuffer()
		}

	} else {
		kv.lastIncludedIndex = default_sentinel_index
		kv.lastIncludedTerm = default_start_term

		kv.clients_Info = make(map[int64]*Client)
		kv.db = make(map[string]string)
	}

	kv.operationBuffer = make([]Op, 0)
	kv.indexBuffer = make([]int, 0)
	kv.termBuffer = make([]int, 0)

	go func(kv *KVServer) {		
		for applyMessage := range kv.applyCh {
			kv.mu.Lock()
			//log.Printf("kvserver %d Locked", kv.me)
			if kv.killed(){
				kv.mu.Unlock()
				return
			} else {
				
				_, isLeader := kv.rf.GetState()
				
				if isLeader {
					if kv.maxraftstate != -1 {
					
						snapShotSize := kv.rf.GetRaftStateSize()
					
						if snapShotSize >= kv.maxraftstate {
							//log.Printf("kvserver %d make snapshot in StartKVServer with LastIncludeIndex %d and LastIncludeTerm %d", kv.me, kv.lastIncludedIndex, kv.lastIncludedTerm)
							kv.tryInitSnapShot()
						}
					}
				}
				kv.handleRequest(applyMessage)
				//log.Printf("kvserver %d finished handling request", kv.me)
				//log.Printf("kvserver %d unlocked", kv.me)
				kv.mu.Unlock()
				
			}
		}
	}(kv)

	return kv
}


