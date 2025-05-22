package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "sync/atomic"
import "../labgob"

import "math"

import "time"

//import "log"

import "strconv"

import "sort"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	dead    int32 // set by Kill()

	configs []Config // indexed by config num

	clients_Info map[int64]*Client //map from client serial number to its state pertaining cached responses
}


type Op struct {
	// Your data here.
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Sequence_Number int // index of op the client submit, i means ith operation 
	// (indicating client has received respone for all ops from 1 to i-1)

	Client_Serial_Number int64 // serial_number for client who initate this op, used in conjunction with Sequence_Number
	// for duplication detection

	Servers map[int][]string // GIDs and corresponding server list that should join(be added) the most recent configs, new GID -> servers mappings used for Join()

	GIDs []int // GIDs that should leave the most recent configs, used for Leave()

	Shard int // shard we point to GID, used for Move()
	GID   int // GID the Shard is pointed to, used for Move()

	Num int // version number of the config the clients attempts to see, used for Query()

	Operation string // describe what type of operation we try to perform with the current command
}

type Client struct {
	Received_Sequence_Number int // highest sequence number of op whose response has been received by the client, 
	// so all op with sequence from 1 to Received_Sequence can be deleted since client already has the sequence

	Last_Processed_Sequence_Number int // the sequence number of last operation executed by server,
	// all op with seq_num <= Last_Processed_Sequence_Number should not be executed since they have been executed already  by This smserver

	Cached_Response map[int] *StoredReply
}

type StoredReply struct {
	Err Err
	ConfigToReturn Config
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := "Join"
	
	servers := make(map[int][]string)
	for gid, server := range args.Servers {
		servers[gid] = server
	}
	Client_Serial_Number := args.Client_Serial_Number
	Sequence_Number := args.Sequence_Number

	//log.Printf("Shardmaster %d receives Join request from client %d with sequence_number %d", sm.me, Client_Serial_Number, Sequence_Number)

	client_Info_This, ok := sm.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This smserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		client_To_Add.Last_Processed_Sequence_Number = 0 // well This smserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		sm.clients_Info[Client_Serial_Number] = &client_To_Add
		client_Info_This = sm.clients_Info[Client_Serial_Number]

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
		// due to asychronous network, it is possible that the older request arrives This smserver as result of re routing, but This smserver already 
		// receives snapshot from previous leader that has handled this request
	}
	

	////log.Printf("This smserver %d has received Join request with sequence number %d from clerk %d", sm.me, Sequence_Number, Client_Serial_Number)

	if sm.killed() {
		reply.WrongLeader = true
		//log.Printf("This smserver %d has been killed", sm.me)
		return
	} 

	// removed reply to previous rpc already finished
	//log.Printf("server %d raft Join lock 1", sm.me)
	_, isLeader := sm.rf.GetState()
	//log.Printf("server %d raft Join unlock 1", sm.me)

	if !isLeader {
		//log.Printf("This smserver %d has received Join request from client %d with sequence number %d but is not leader", sm.me, Client_Serial_Number, Sequence_Number)
		reply.WrongLeader = true
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

			reply.WrongLeader = false
			reply.Err = cachedReply.Err

			//log.Printf("This smserver %d has cached result for Join request  client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
			
			return
		} else {
	
			opToRaft := Op{}

			opToRaft.Sequence_Number = Sequence_Number
			opToRaft.Client_Serial_Number = Client_Serial_Number

			opToRaft.Servers = servers
			opToRaft.Operation = op
			
			//log.Printf("server %d raft start lock", sm.me)
			_, _, _, isLeader := sm.rf.StartQuick(opToRaft)
			//log.Printf("Shard master %d starts Join agreement at index %d", sm.me, index)

			//log.Printf("server %d raft start unlock 1", sm.me)
	
			if !isLeader {
				//log.Printf("This smserver %d has cached result for Join request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
				reply.WrongLeader = true
				
				return
			} else {
				//log.Printf("This smserver %d does not have cached result for Join request client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
				sm.mu.Unlock()
			}
			
			for {
				////log.Printf("smserver before lock")
				sm.mu.Lock()
				////log.Printf("smserver %d putappend locked ", sm.me)
				if sm.killed() {
					//log.Printf("This smserver %d has been killed", sm.me)
					reply.WrongLeader = true
					return
				} 
				////log.Printf("smserver %d putappend GetStateWtf init", sm.me)

				//log.Printf("server %d raft Join lock 2", sm.me)
				_, isLeader := sm.rf.GetState()
				//log.Printf("server %d raft Join unlock 2", sm.me)
			
				////log.Printf("smserver %d (term %d) putappend GetStateWtf finished", sm.me, term)
				if !isLeader {
					//log.Printf("This smserver %d has cached result for Join request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
		
					reply.WrongLeader = true
					return
				} else {
					client_Info_This = sm.clients_Info[Client_Serial_Number]

					Client_Received_Sequence_Number = client_Info_This.Received_Sequence_Number
					Client_Last_Processed_Sequence_Number = client_Info_This.Last_Processed_Sequence_Number

					////log.Printf("smserver %d (term %d), for client %d, putappend task with sequence number %d, Client_Received_Sequence_Number %d, Client_Last_Processed_Sequence_Number %d", sm.me, term, Client_Serial_Number, Sequence_Number, Client_Received_Sequence_Number, Client_Last_Processed_Sequence_Number)

					if Sequence_Number <= Client_Received_Sequence_Number {
						// dude the client has already received reply, so that reply is just staled and we don't need to do 
						// anything about it
						////log.Printf("smserver %d (term %d) wtf2", sm.me, term)
						return
					} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
						// good, that means cached reply is still in the dictionary
						cachedReply := client_Info_This.Cached_Response[Sequence_Number]
			
						reply.WrongLeader = false
						reply.Err = cachedReply.Err
			
						//log.Printf("This smserver %d has cached result for Join request with client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
						
						return
					} else {
						//log.Printf("This smserver %d does not have cached result for Join request with client: %d, seq_num: %d, keep waiting", sm.me, Client_Serial_Number, Sequence_Number)
						////log.Printf("smserver %d (term %d) putappend Unlocked", sm.me, term)
						sm.mu.Unlock()
					}
				}
				
				time.Sleep(time.Duration(5) * time.Millisecond)
			}
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := "Leave"

	gids := args.GIDs

	Client_Serial_Number := args.Client_Serial_Number
	Sequence_Number := args.Sequence_Number

	//log.Printf("Shardmaster %d receives Leave request from client %d with sequence_number %d", sm.me, Client_Serial_Number, Sequence_Number)

	client_Info_This, ok := sm.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This smserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		client_To_Add.Last_Processed_Sequence_Number = 0 // well This smserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		sm.clients_Info[Client_Serial_Number] = &client_To_Add
		client_Info_This = sm.clients_Info[Client_Serial_Number]

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
		// due to asychronous network, it is possible that the older request arrives This smserver as result of re routing, but This smserver already 
		// receives snapshot from previous leader that has handled this request
	}
	

	////log.Printf("This smserver %d has received Join request with sequence number %d from clerk %d", sm.me, Sequence_Number, Client_Serial_Number)

	if sm.killed() {
		reply.WrongLeader = true
		//log.Printf("This smserver %d has been killed", sm.me)
		return
	} 

	// removed reply to previous rpc already finished
	//log.Printf("server %d raft Leave lock 1", sm.me)
	_, isLeader := sm.rf.GetState()
	//log.Printf("server %d raft Leave unlock 1", sm.me)

	if !isLeader {
		//log.Printf("This smserver %d has received Leave request from client %d with sequence number %d but is not leader", sm.me, Client_Serial_Number, Sequence_Number)
		reply.WrongLeader = true
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

			reply.WrongLeader = false
			reply.Err = cachedReply.Err

			//log.Printf("This smserver %d has cached result for Leave request  client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
			
			return
		} else {
	
			opToRaft := Op{}

			opToRaft.Sequence_Number = Sequence_Number
			opToRaft.Client_Serial_Number = Client_Serial_Number

			opToRaft.GIDs = gids
			opToRaft.Operation = op
			//log.Printf("server %d raft Leave start lock ", sm.me)
			_, _, _, isLeader := sm.rf.StartQuick(opToRaft)
			//log.Printf("Shard master %d starts Leave agreement at index %d", sm.me, index)
			//log.Printf("server %d raft Leave start unlock ", sm.me)
	
			if !isLeader {
				//log.Printf("This smserver %d has cached result for Leave request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
				reply.WrongLeader = true
				
				return
			} else {
				//log.Printf("This smserver %d does not have cached result for Leave request client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
				sm.mu.Unlock()
			}
			
			for {
				////log.Printf("smserver before lock")
				sm.mu.Lock()
				////log.Printf("smserver %d putappend locked ", sm.me)
				if sm.killed() {
					//log.Printf("This smserver %d has been killed", sm.me)
					reply.WrongLeader = true
					return
				} 
				////log.Printf("smserver %d putappend GetStateWtf init", sm.me)

				//log.Printf("server %d raft Leave lock 2 ", sm.me)
				_, isLeader := sm.rf.GetState()
				//log.Printf("server %d raft Leave unlock 2 ", sm.me)
			
				////log.Printf("smserver %d (term %d) putappend GetStateWtf finished", sm.me, term)
				if !isLeader {
					//log.Printf("This smserver %d has cached result for Leave request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
		
					reply.WrongLeader = true
					return
				} else {
					client_Info_This = sm.clients_Info[Client_Serial_Number]

					Client_Received_Sequence_Number = client_Info_This.Received_Sequence_Number
					Client_Last_Processed_Sequence_Number = client_Info_This.Last_Processed_Sequence_Number

					////log.Printf("smserver %d (term %d), for client %d, putappend task with sequence number %d, Client_Received_Sequence_Number %d, Client_Last_Processed_Sequence_Number %d", sm.me, term, Client_Serial_Number, Sequence_Number, Client_Received_Sequence_Number, Client_Last_Processed_Sequence_Number)

					if Sequence_Number <= Client_Received_Sequence_Number {
						// dude the client has already received reply, so that reply is just staled and we don't need to do 
						// anything about it
						//log.Printf("smserver %d  wtf2", sm.me)

						return
					} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
						// good, that means cached reply is still in the dictionary
						cachedReply := client_Info_This.Cached_Response[Sequence_Number]
			
						reply.WrongLeader = false
						reply.Err = cachedReply.Err
			
						//log.Printf("This smserver %d has cached result for Leave request with client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
						
						return
					} else {
						//log.Printf("This smserver %d does not have cached result for Leave request with client: %d, seq_num: %d, keep waiting", sm.me, Client_Serial_Number, Sequence_Number)
						////log.Printf("smserver %d (term %d) putappend Unlocked", sm.me, term)
						sm.mu.Unlock()
					}
				}
				
				time.Sleep(time.Duration(5) * time.Millisecond)
			}
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := "Move"

	shard := args.Shard
	gid := args.GID
	Client_Serial_Number := args.Client_Serial_Number
	Sequence_Number := args.Sequence_Number

	//log.Printf("Shardmaster %d receives Move request from client %d with sequence_number %d", sm.me, Client_Serial_Number, Sequence_Number)

	client_Info_This, ok := sm.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This smserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		client_To_Add.Last_Processed_Sequence_Number = 0 // well This smserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		sm.clients_Info[Client_Serial_Number] = &client_To_Add
		client_Info_This = sm.clients_Info[Client_Serial_Number]

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
		// due to asychronous network, it is possible that the older request arrives This smserver as result of re routing, but This smserver already 
		// receives snapshot from previous leader that has handled this request
	}
	

	////log.Printf("This smserver %d has received Join request with sequence number %d from clerk %d", sm.me, Sequence_Number, Client_Serial_Number)

	if sm.killed() {
		reply.WrongLeader = true
		//log.Printf("This smserver %d has been killed", sm.me)
		return
	} 

	// removed reply to previous rpc already finished
	//log.Printf("server %d raft Move lock 1 ", sm.me)
	_, isLeader := sm.rf.GetState()
	//log.Printf("server %d raft Move unlock 1 ", sm.me)

	if !isLeader {
		//log.Printf("This smserver %d has received Move request from client %d with sequence number %d but is not leader", sm.me, Client_Serial_Number, Sequence_Number)
		reply.WrongLeader = true
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

			reply.WrongLeader = false
			reply.Err = cachedReply.Err

			//log.Printf("This smserver %d has cached result for Move request  client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
			
			return
		} else {
	
			opToRaft := Op{}

			opToRaft.Sequence_Number = Sequence_Number
			opToRaft.Client_Serial_Number = Client_Serial_Number

			opToRaft.Shard = shard
			opToRaft.GID = gid
			opToRaft.Operation = op
			//log.Printf("server %d raft Move start lock 1 ", sm.me)
			_, _, _, isLeader := sm.rf.StartQuick(opToRaft)
			//log.Printf("Shard master %d starts Move agreement at index %d", sm.me, index)
			//log.Printf("server %d raft Move start unlock 1 ", sm.me)
	
			if !isLeader {
				//log.Printf("This smserver %d has cached result for Move request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
				reply.WrongLeader = true
				
				return
			} else {
				//log.Printf("This smserver %d does not have cached result for Move request client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
				sm.mu.Unlock()
			}
			
			for {
				////log.Printf("smserver before lock")
				sm.mu.Lock()
				////log.Printf("smserver %d putappend locked ", sm.me)
				if sm.killed() {
					//log.Printf("This smserver %d has been killed", sm.me)
					reply.WrongLeader = true
					return
				} 
				////log.Printf("smserver %d putappend GetStateWtf init", sm.me)

				//log.Printf("server %d raft Move lock 2", sm.me)
				_, isLeader := sm.rf.GetState()
				//log.Printf("server %d raft Move unlock 2", sm.me)
			
				////log.Printf("smserver %d (term %d) putappend GetStateWtf finished", sm.me, term)
				if !isLeader {
					//log.Printf("This smserver %d has cached result for Move request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
		
					reply.WrongLeader = true
					return
				} else {
					client_Info_This = sm.clients_Info[Client_Serial_Number]

					Client_Received_Sequence_Number = client_Info_This.Received_Sequence_Number
					Client_Last_Processed_Sequence_Number = client_Info_This.Last_Processed_Sequence_Number

					////log.Printf("smserver %d (term %d), for client %d, putappend task with sequence number %d, Client_Received_Sequence_Number %d, Client_Last_Processed_Sequence_Number %d", sm.me, term, Client_Serial_Number, Sequence_Number, Client_Received_Sequence_Number, Client_Last_Processed_Sequence_Number)

					if Sequence_Number <= Client_Received_Sequence_Number {
						// dude the client has already received reply, so that reply is just staled and we don't need to do 
						// anything about it
						////log.Printf("smserver %d (term %d) wtf2", sm.me, term)
						return
					} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
						// good, that means cached reply is still in the dictionary
						cachedReply := client_Info_This.Cached_Response[Sequence_Number]
			
						reply.WrongLeader = false
						reply.Err = cachedReply.Err
			
						//log.Printf("This smserver %d has cached result for Move request with client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
						
						return
					} else {
						//log.Printf("This smserver %d does not have cached result for Move request with client: %d, seq_num: %d, keep waiting", sm.me, Client_Serial_Number, Sequence_Number)
						////log.Printf("smserver %d (term %d) putappend Unlocked", sm.me, term)
						sm.mu.Unlock()
					}
				}
				
				time.Sleep(time.Duration(5) * time.Millisecond)
			}
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := "Query"
	
	num := args.Num
	Client_Serial_Number := args.Client_Serial_Number
	Sequence_Number := args.Sequence_Number

	//log.Printf("Shardmaster %d receives Query request from client %d with sequence_number %d", sm.me, Client_Serial_Number, Sequence_Number)

	client_Info_This, ok := sm.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This smserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		client_To_Add.Last_Processed_Sequence_Number = 0 // well This smserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		sm.clients_Info[Client_Serial_Number] = &client_To_Add
		client_Info_This = sm.clients_Info[Client_Serial_Number]

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
		// due to asychronous network, it is possible that the older request arrives This smserver as result of re routing, but This smserver already 
		// receives snapshot from previous leader that has handled this request
	}
	

	////log.Printf("This smserver %d has received Join request with sequence number %d from clerk %d", sm.me, Sequence_Number, Client_Serial_Number)

	if sm.killed() {
		reply.WrongLeader = true
		//log.Printf("This smserver %d has been killed", sm.me)
		return
	} 

	// removed reply to previous rpc already finished
	//log.Printf("server %d raft Query lock 1", sm.me)
	_, isLeader := sm.rf.GetState()
	//log.Printf("server %d raft Query unlock 2", sm.me)

	if !isLeader {
		//log.Printf("This smserver %d has received Query request from client %d with sequence number %d but is not leader", sm.me, Client_Serial_Number, Sequence_Number)
		reply.WrongLeader = true
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

			reply.WrongLeader = false
			reply.Err = cachedReply.Err
			reply.Config = cachedReply.ConfigToReturn

			//log.Printf("This smserver %d has cached result for Query request from  client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
			
			return
		} else {
	
			opToRaft := Op{}

			opToRaft.Sequence_Number = Sequence_Number
			opToRaft.Client_Serial_Number = Client_Serial_Number

			opToRaft.Num = num
			opToRaft.Operation = op
			
			//log.Printf("server %d raft Query start lock", sm.me)
			_, _, _, isLeader := sm.rf.StartQuick(opToRaft)
			//log.Printf("Shard master %d starts Query agreement at index %d", sm.me, index)
			//log.Printf("server %d raft Query start unlock", sm.me)
	
			if !isLeader {
				//log.Printf("This smserver %d has cached result for Query request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
				reply.WrongLeader = true
				
				return
			} else {
				//log.Printf("This smserver %d does not have cached result for Query request client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
				sm.mu.Unlock()
			}
			
			for {
				////log.Printf("smserver before lock")
				sm.mu.Lock()
				////log.Printf("smserver %d putappend locked ", sm.me)
				if sm.killed() {
					//log.Printf("This smserver %d has been killed", sm.me)
					reply.WrongLeader = true
					return
				} 
				////log.Printf("smserver %d putappend GetStateWtf init", sm.me)

				//log.Printf("server %d raft Query lock 2", sm.me)
				_, isLeader := sm.rf.GetState()
				//log.Printf("server %d raft Query unlock 2", sm.me)
			
				////log.Printf("smserver %d (term %d) putappend GetStateWtf finished", sm.me, term)
				if !isLeader {
					//log.Printf("This smserver %d has cached result for Query request client: %d, seq_num: %d but is not a leader", sm.me, Client_Serial_Number, Sequence_Number)
		
					reply.WrongLeader = true
					return
				} else {
					client_Info_This = sm.clients_Info[Client_Serial_Number]

					Client_Received_Sequence_Number = client_Info_This.Received_Sequence_Number
					Client_Last_Processed_Sequence_Number = client_Info_This.Last_Processed_Sequence_Number

					////log.Printf("smserver %d (term %d), for client %d, putappend task with sequence number %d, Client_Received_Sequence_Number %d, Client_Last_Processed_Sequence_Number %d", sm.me, term, Client_Serial_Number, Sequence_Number, Client_Received_Sequence_Number, Client_Last_Processed_Sequence_Number)

					if Sequence_Number <= Client_Received_Sequence_Number {
						// dude the client has already received reply, so that reply is just staled and we don't need to do 
						// anything about it
						////log.Printf("smserver %d (term %d) wtf2", sm.me, term)
						return
					} else if Sequence_Number <= Client_Last_Processed_Sequence_Number {
						// good, that means cached reply is still in the dictionary
						cachedReply := client_Info_This.Cached_Response[Sequence_Number]
			
						reply.WrongLeader = false
						reply.Err = cachedReply.Err
						reply.Config = cachedReply.ConfigToReturn
			
						//log.Printf("This smserver %d has cached result for Query request with client: %d, seq_num: %d", sm.me, Client_Serial_Number, Sequence_Number)
						
						return
					} else {
						//log.Printf("This smserver %d does not have cached result for Query request with client: %d, seq_num: %d, keep waiting", sm.me, Client_Serial_Number, Sequence_Number)
						////log.Printf("smserver %d (term %d) putappend Unlocked", sm.me, term)
						sm.mu.Unlock()
					}
				}
				
				time.Sleep(time.Duration(5) * time.Millisecond)
			}
		}
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func(sm *ShardMaster) applyOperation(operation Op) {

	Sequence_Number := operation.Sequence_Number
	Client_Serial_Number := operation.Client_Serial_Number

	op := operation.Operation

	client_Info_This, ok := sm.clients_Info[Client_Serial_Number]
	if !ok {
		// means this leader is the first leader that has received request from this client
		// and This smserver has not processed any request for this client and no other server has
		// (it others do, )
		client_To_Add := Client{}
		client_To_Add.Received_Sequence_Number = Sequence_Number - 1 //this client must have received all
		// requests before sequence number or it will not fire this request
		// but in this case, this is certainly command for op with seq_num 1
		client_To_Add.Last_Processed_Sequence_Number = 0 // well This smserver has
		// not execute any operation on this client yet, so we need to wait raft send command
		// so that we can eventually apply operations until the server is at least up to date 
		// as Received_Sequence_Number
		client_To_Add.Cached_Response = make(map[int]*StoredReply)
		// save all responses from Received_Sequence_Number + 1 (since we are not sure if client has received
		// previous response or not, we don't delete until future rpc indicate we can)
		sm.clients_Info[Client_Serial_Number] = &client_To_Add

		client_Info_This = sm.clients_Info[Client_Serial_Number] 

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

	latestConfig := sm.configs[len(sm.configs) - 1]
	latestConfigNum := latestConfig.Num
	latestConfigShards := latestConfig.Shards
	latestConfigGroups := latestConfig.Groups
	if op == "Join" {
		//log.Printf("Shard master %d handling Join request from Client %d with seq_num %d, updating config to version %d", sm.me, Client_Serial_Number, Sequence_Number, latestConfigNum + 1)
		
		serversToJoin := operation.Servers

		newConfigNum := latestConfigNum + 1
		newConfigShards := latestConfigShards
		newConfigGroups := make(map[int][]string)

		groupsBeforeJoinList := make([]int, 0)
		for gid, servers := range latestConfigGroups {
			newConfigGroups[gid] = servers
			groupsBeforeJoinList = append(groupsBeforeJoinList, gid)
		}
		// sort groups before join in ascending order
		sort.Sort(sort.IntSlice(groupsBeforeJoinList))

		groupsBeforeJoin := ""
		for _, gid := range groupsBeforeJoinList {
			groupsBeforeJoin = groupsBeforeJoin + " " + strconv.Itoa(gid)
		}
		//log.Printf("On ShardMaster %d, version %d, Groups before join are : %s",sm.me, latestConfigNum, groupsBeforeJoin)

		groupsToJoinList := make([]int, 0)
		for gid, servers := range serversToJoin {
			
			newConfigGroups[gid] = servers
			_, ok := latestConfigGroups[gid]
			if !ok {
				// if the gid we want to join is already in the config, then we only change its corresponding servers
				// if not, we know is it a new group we need to add
				groupsToJoinList = append(groupsToJoinList, gid)
			}
		}
		sort.Sort(sort.IntSlice(groupsToJoinList))

		groupsToJoin := ""
		for _, gid := range groupsToJoinList {
			groupsToJoin = groupsToJoin + " " + strconv.Itoa(gid)
		}
		
		//log.Printf("On ShardMaster %d, version %d, Groups to join are : %s",sm.me, latestConfigNum, groupsToJoin)

		if len(latestConfigGroups) == 0 {
			// meaning all shards have not been appointed to any group, we have no group in previous config 
			for i := 0; i < NShards; i++ {
				// simply assign shards to new groups in round robin fashion
				newConfigShards[i] = groupsToJoinList[i % len(groupsToJoinList)]
			}

			newConfig := Config{}
			newConfig.Num = newConfigNum
			newConfig.Shards = newConfigShards
			newConfig.Groups = newConfigGroups
			sm.configs = append(sm.configs, newConfig)

		} else if len(groupsToJoinList) == 0 {
			// like there is no newGid we need to add; they already in group and we just change their correspondiong server
			// so no assignment changed is needed
			newConfig := Config{}
			newConfig.Num = newConfigNum
			newConfig.Shards = newConfigShards
			newConfig.Groups = newConfigGroups
			sm.configs = append(sm.configs, newConfig)
		} else {
			// make a map from gid to the list of shards that belong to it
			mapGidsToShards := make(map[int][]int)

			for shard, gid := range latestConfigShards {

				gidShardList, ok := mapGidsToShards[gid]

				if !ok {
					mapGidsToShards[gid] = make([]int, 0)
				}
				mapGidsToShards[gid] = append(gidShardList, shard)
			}

			for _, newGid := range groupsToJoinList {
				mapGidsToShards[newGid] = make([]int, 0)
			}



			numOfGroup := len(newConfigGroups)

			totalGroup := append(groupsBeforeJoinList, groupsToJoinList...)
			sort.Sort(sort.IntSlice(totalGroup))

			//log.Printf("On ShardMaster %d, version %d, shard assignment before Join: ",sm.me, latestConfigNum)

			for _, gid := range totalGroup {
				shardList := mapGidsToShards[gid]
				shardString := ""
				for _, shard := range shardList {
					shardString = shardString + " " + strconv.Itoa(shard)
				}
				//log.Printf("On gid %d, shards are: %s", gid, shardString)
			}



			// max number of shards a group could have to maintain shard assignment balance
			// for example, if NShards = 5, and we have 2 groups, the max number will be ceil(5/2) = 3
			// min will be floor(5/2) = 2
			// if NShards = 10 and we have 12 groups (which could happen if we Join too much without Leave)
			// max will be 1 and min will be 1


			maxNumberOfShardInGroup := int(math.Ceil(float64(NShards) / float64(numOfGroup)))

			minNumberOfShardInGroup := int(math.Floor(float64(NShards) / float64(numOfGroup)))

			for {
				////log.Printf("NShards is %d, nunOfGroup is %d", NShards, numOfGroup)
				////log.Printf("maxNumberOfShardInGroup is %d, minNumberOfShardInGroup is %d", maxNumberOfShardInGroup, minNumberOfShardInGroup)
				////log.Printf("Shardmaster %d looping", sm.me)
				groupToRemoveFrom := -1

				for i := 0; i < len(totalGroup); i++ {
					gid := totalGroup[i]
					shardList := mapGidsToShards[gid]
					if len(shardList) > maxNumberOfShardInGroup {
						// to ensure minimal transfer, we only move shards from group whose
						// size is bigger then max group size in new replica groups set
						groupToRemoveFrom = gid
						break
					}
				}

				groupToMoveTo := -1

				for i := 0; i < len(totalGroup); i++ {
					gid := totalGroup[i]
					shardList := mapGidsToShards[gid]
					if len(shardList) < minNumberOfShardInGroup {
						// to ensure minimal transfer, we only move shards to group whose
						// size is less than min group size in new replica groups set
						groupToMoveTo = gid
						break
					}
				}

				////log.Printf("first, groupToRemoveFrom is %d, and groupToMoveTo is %d",groupToRemoveFrom, groupToMoveTo)



				if groupToRemoveFrom == -1  && groupToMoveTo == -1 {
					// if all groups have size less than or equal to max group size and larger then or equal to min group size
					// no assignment necessary
					// just break
					break
				} else if groupToRemoveFrom != -1 && groupToMoveTo == -1 {
					// there exists a group with size too large, but there is no group too small
					// we move one from the large group to the group with minimum number of shards assigned
					// it does not matter if the migration is to the new server or not 
					// since removing shard from a group that way too large 
					// is necessary for rebalancing 

					minLength := 257 // magical number from test I just saw...

					for i := 0; i < len(totalGroup); i++ {
						gid := totalGroup[i]
						shardList := mapGidsToShards[gid]
						if len(shardList) < minLength {
							// we attempt to move shard to new gids
							// if a new gid has shard list size < minimum number of shards we can add to group
							// we add new shard to that group, regardless of it being newGid or not
							groupToMoveTo = gid
							minLength = len(shardList)

						}
					}

				} else if groupToRemoveFrom == -1 && groupToMoveTo != -1 {
					// there exists a group with size too small, but there is no group too large
					// we move one to the small group from the group with maximum number of shards assigned
					// it does not matter if the migration is to the new server or not 
					// since moving shard to a group that is way too small
					// is necessary for rebalancing 

					maxLength := 0 

					for i := 0; i < len(totalGroup); i++ {
						gid := totalGroup[i]
						shardList := mapGidsToShards[gid]
						if len(shardList) > maxLength {
							// we attempt to move shard to new gids
							// if a new gid has shard list size < minimum number of shards we can add to group
							// we add new shard to that group, regardless of it being newGid or not
							groupToRemoveFrom = gid
							maxLength = len(shardList)
						}
					}

				} else {
					groupToRemoveFrom = groupToRemoveFrom
					groupToMoveTo = groupToMoveTo

				}

				////log.Printf("groupToRemoveFrom is %d, and groupToMoveTo is %d",groupToRemoveFrom, groupToMoveTo)
				// get list from which we get a shard to move to new group
				shardListFromGroupToRemoveFrom := mapGidsToShards[groupToRemoveFrom]

				// use last shard in the list to move to new group
				shardToMoveToNewGroupTo := shardListFromGroupToRemoveFrom[len(shardListFromGroupToRemoveFrom) - 1]

				// remove last shard from the group we remove from 
				mapGidsToShards[groupToRemoveFrom] = shardListFromGroupToRemoveFrom [: len(shardListFromGroupToRemoveFrom) - 1]

				// get list to which we move the shard
				shardListFromGroupToRemoveTo := mapGidsToShards[groupToMoveTo]

				// move it to the group
				mapGidsToShards[groupToMoveTo] = append(shardListFromGroupToRemoveTo, shardToMoveToNewGroupTo)

			}

			// map shard to gid, we know NShards > number of gids, so will be a many to one mapping
			for gid, shardList := range mapGidsToShards {
				for _, shard := range shardList {
					newConfigShards[shard] = gid
				}
			}

			newConfig := Config{}
			newConfig.Num = newConfigNum
			newConfig.Shards = newConfigShards
			newConfig.Groups = newConfigGroups
			sm.configs = append(sm.configs, newConfig)

			//log.Printf("On ShardMaster %d, version %d, shard assignment after Join :",sm.me, newConfigNum)

			for _, gid := range totalGroup {
				shardList := mapGidsToShards[gid]
				shardString := ""
				for _, shard := range shardList {
					shardString = shardString + " " + strconv.Itoa(shard)
				}
				//log.Printf("On gid %d, shards are: %s", gid, shardString)
			}
		}
		groupsAfterJoinList := make([]int, 0)
		for gid, _ := range sm.configs[len(sm.configs) - 1].Groups {
			groupsAfterJoinList = append(groupsAfterJoinList, gid)
		}

		sort.Sort(sort.IntSlice(groupsAfterJoinList))

		groupsAfterJoin := ""
		for _, gid := range groupsAfterJoinList {
			groupsAfterJoin = groupsAfterJoin + " " + strconv.Itoa(gid)
		}

		//log.Printf("On ShardMaster %d, version %d, Groups after join are : %s",sm.me, newConfigNum, groupsAfterJoin)

		replyToStore.Err = OK
	
	} else if op == "Leave" {
		//log.Printf("Shard master %d handling Leave request from Client %d with seq_num %d, updating config to version %d", sm.me, Client_Serial_Number, Sequence_Number, latestConfigNum + 1)
		
		gidsToLeave := operation.GIDs

		newConfigNum := latestConfigNum + 1
		newConfigShards := latestConfigShards
		newConfigGroups := make(map[int][]string)

		
		groupsBeforeLeaveList := make([]int, 0)
		for gid, servers := range latestConfigGroups {
			newConfigGroups[gid] = servers
			groupsBeforeLeaveList = append(groupsBeforeLeaveList, gid)
		}
		sort.Sort(sort.IntSlice(groupsBeforeLeaveList))

		groupsBeforeLeave := ""
		for _, gid := range groupsBeforeLeaveList {
			groupsBeforeLeave = groupsBeforeLeave + " " + strconv.Itoa(gid)
		}

		//log.Printf("On ShardMaster %d, version %d, Groups before Leave are : %s",sm.me, latestConfigNum, groupsBeforeLeave)

		if len(latestConfigGroups) == 0 {
			// meaning there is no group in the config in the first place
			// then nothing we need to do
			//log.Printf("On ShardMaster %d, version %d, there is no group in latest config, so nothing to do",sm.me, latestConfigNum)
			newConfig := Config{}
			newConfig.Num = newConfigNum
			newConfig.Shards = newConfigShards
			newConfig.Groups = newConfigGroups
			sm.configs = append(sm.configs, newConfig)
		} else {
			mapGidsToShards := make(map[int][]int)

			for gid, _ := range latestConfigGroups {
				mapGidsToShards[gid] = make([]int, 0)
			}

			for shard, gid := range latestConfigShards {

				mapGidsToShards[gid] = append(mapGidsToShards[gid], shard)
			}

			//log.Printf("On ShardMaster %d, version %d, shard assignment before Leave :",sm.me, latestConfigNum)

			for _, gid := range groupsBeforeLeaveList  {
				shardList := mapGidsToShards[gid]
				shardString := ""
				for _, shard := range shardList {
					shardString = shardString + " " + strconv.Itoa(shard)
				}
				//log.Printf("On gid %d, shards are: %s", gid, shardString)
			}

			groupsToLeaveList := make([]int, 0)
			shardsToAllocate := make([]int, 0)

			//log.Printf("number of GIDs before delete is %d", len(mapGidsToShards))
			for _, gidToLeave := range gidsToLeave {
				
				shardsToMove, ok := mapGidsToShards[gidToLeave]

				groupsToLeaveList = append(groupsToLeaveList, gidToLeave)

				if ok {
					// it is possible the server we ask to leave is not in the config in the first place

					// add shards attached to GID we want to delete to shardsToAllocate for re allocation
					shardsToAllocate = append(shardsToAllocate, shardsToMove...)
					delete(newConfigGroups, gidToLeave)
					delete(mapGidsToShards, gidToLeave)
				}
				
			}
			//log.Printf("number of GIDs after delete is %d", len(mapGidsToShards))

			sort.Sort(sort.IntSlice(groupsToLeaveList))

			groupsToLeave := ""
			for _, gid := range groupsToLeaveList {
				groupsToLeave  = groupsToLeave  + " " + strconv.Itoa(gid)
			}


			//log.Printf("On ShardMaster %d, version %d, Groups to leave are : %s",sm.me, latestConfigNum, groupsToLeave)

			if len(shardsToAllocate) == 0 {
				// either none of groups we ask to leave is in the config
				// or groups that are in latest config has no shard assigned to
				// then there is no shard reassignment needed
				// but we do need to ensure we remove the gids in Leave arguement that exists in latest config to
				// get the new config
				//log.Printf("None of the group we try to leave exists in config, so assignment will not change, but still attempt to remove gids in args from the groups")
				newConfig := Config{}
				newConfig.Num = newConfigNum
				newConfig.Shards = newConfigShards
				newConfig.Groups = newConfigGroups

				for _, gidToLeave := range gidsToLeave {
					_, ok := newConfig.Groups[gidToLeave]
					if ok {
						delete(newConfig.Groups, gidToLeave)
					}
				}
				sm.configs = append(sm.configs, newConfig)
			} else if len(mapGidsToShards) == 0 {
				// means there is no more groups...
				//log.Printf("We just removed every group in the lates config, so literally there is no group shards can be assigned to...")
				newConfig := Config{}
				newConfig.Num = newConfigNum
				
				newConfig.Groups = make(map[int][]string)
				sm.configs = append(sm.configs, newConfig)

			} else {
				// there is some shard we need to move

				groupsAfterLeaveList := make([]int, 0)
				for gid, _ := range mapGidsToShards {
					groupsAfterLeaveList = append(groupsAfterLeaveList, gid)
				}
				sort.Sort(sort.IntSlice(groupsAfterLeaveList))

				groupsAfterLeave := ""
				for _, gid := range groupsAfterLeaveList {
					groupsAfterLeave = groupsAfterLeave + " " + strconv.Itoa(gid)
				}


				//log.Printf("On ShardMaster %d, version %d, GroupsAfterLeave are : %s",sm.me, latestConfigNum, groupsAfterLeave)

				sort.Sort(sort.IntSlice(shardsToAllocate))

				for len(shardsToAllocate) != 0 {
					// while there is still some shards to move

					minLength := 257 // magical number from test I just saw...
					groupToMoveTo := -1
					for i := 0; i < len(groupsAfterLeaveList); i++ {
						gid := groupsAfterLeaveList[i]
						shardList := mapGidsToShards[gid]
						//log.Printf("On gid %d, number of shard is: %d, and currentMin is %d", gid, len(shardList), minLength)

						if len(shardList) < minLength {
							// we attempt to move shard to new gids
							// if a new gid has shard list size < minimum number of shards we can add to group
							// we add new shard to that group, regardless of it being newGid or not
							groupToMoveTo = gid
							minLength = len(shardList)
						}
					}

					// use first shard in the sorted shardsToAllocate list to move to new group
					shardToMoveToNewGroupTo := shardsToAllocate[0]

					// remove the first shard from shardsToAllocate 
					shardsToAllocate = shardsToAllocate[1: len(shardsToAllocate)]

					// get list to which we move the shard
					shardListFromGroupToRemoveTo := mapGidsToShards[groupToMoveTo]

					// move the shard to the group
					mapGidsToShards[groupToMoveTo] = append(shardListFromGroupToRemoveTo, shardToMoveToNewGroupTo)
				}

				
				for gid, shardList := range mapGidsToShards {
					for _, shard := range shardList {
						newConfigShards[shard] = gid
					}
				}

				newConfig := Config{}
				newConfig.Num = newConfigNum
				newConfig.Shards = newConfigShards
				newConfig.Groups = newConfigGroups
				sm.configs = append(sm.configs, newConfig)

			}

			groupsAfterLeaveList := make([]int, 0)
			for gid, _ := range sm.configs[len(sm.configs) - 1].Groups {
				groupsAfterLeaveList = append(groupsAfterLeaveList, gid)
			}

			sort.Sort(sort.IntSlice(groupsAfterLeaveList))

			groupsAfterLeave := ""
			for _, gid := range groupsAfterLeaveList {
				groupsAfterLeave = groupsAfterLeave + " " + strconv.Itoa(gid)
			}

			//log.Printf("On ShardMaster %d, version %d, Groups after Leave are : %s",sm.me, newConfigNum, groupsAfterLeave)

			//log.Printf("On ShardMaster %d, version %d, shard assignment after Leave :",sm.me, newConfigNum)

			for _, gid := range groupsAfterLeaveList {
				shardList := mapGidsToShards[gid]
				shardString := ""
				for _, shard := range shardList {
					shardString = shardString + " " + strconv.Itoa(shard)
				}
				//log.Printf("On gid %d, shards are: %s", gid, shardString)
			}

		}

		


		replyToStore.Err = OK
		
	} else if op == "Move"{
		
		shardToMove := operation.Shard
		gidToMoveTo := operation.GID

		//log.Printf("Shard master %d handling Move request from Client %d with seq_num %d, move shard %d to group %d updating config to version %d", sm.me, Client_Serial_Number, Sequence_Number, shardToMove, gidToMoveTo, latestConfigNum + 1)

		newConfigNum := latestConfigNum + 1
		newConfigShards := latestConfigShards
		newConfigGroups := make(map[int][]string)

		for gid, servers := range latestConfigGroups {
			newConfigGroups[gid] = servers
		}

		_, ok := latestConfigGroups[gidToMoveTo]

		if !ok {
			// like there is no such group in the latest config...
			//log.Printf("Group %d does not exist, so we cannot move shard %d to it", gidToMoveTo, shardToMove)
			newConfig := Config{}
			newConfig.Num = newConfigNum
			newConfig.Shards = newConfigShards
			newConfig.Groups = newConfigGroups
			sm.configs = append(sm.configs, newConfig)

		} else {
			// there is such a group in the latest config
			//log.Printf("Group %d exists, move shard %d to it", gidToMoveTo, shardToMove)
			newConfigShards[shardToMove] = gidToMoveTo

			newConfig := Config{}
			newConfig.Num = newConfigNum
			newConfig.Shards = newConfigShards
			newConfig.Groups = newConfigGroups
			sm.configs = append(sm.configs, newConfig)
		}

		replyToStore.Err = OK

	} else {
		
		configNum := operation.Num

		maxConfigNum := len(sm.configs) - 1

		//log.Printf("Shard master %d handling Query request from Client %d with seq_num %d, querying %d, current latest num is %d", sm.me, Client_Serial_Number, Sequence_Number, configNum, maxConfigNum)

		configIndex := -1
		if configNum == -1 || configNum >= maxConfigNum {
			configIndex = len(sm.configs) - 1
		} else {
			configIndex = configNum
		}
		configToReturn := Config{}

		configToReturn.Num = sm.configs[configIndex].Num
		configToReturn.Shards = sm.configs[configIndex].Shards
		configToReturn.Groups = make(map[int][]string)

		for gid, servers := range sm.configs[configIndex].Groups {
			configToReturn.Groups[gid] = servers
		}

		replyToStore.ConfigToReturn = configToReturn
		replyToStore.Err = OK
	}
	sm.clients_Info[Client_Serial_Number].Cached_Response[Sequence_Number] = &replyToStore // cache the response in case of handling retry
	sm.clients_Info[Client_Serial_Number].Last_Processed_Sequence_Number = Sequence_Number	

	//log.Printf("shard master %d finishes handling %s request from client %d with sequence number %d", sm.me, op, Client_Serial_Number, Sequence_Number)

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	sm.clients_Info = make(map[int64]*Client)

	go func(sm *ShardMaster) {		
		for applyMessage := range sm.applyCh {
			sm.mu.Lock()
			//log.Printf("smserver %d Locked", sm.me)
			if sm.killed(){
				sm.mu.Unlock()
				return
			} else {

				operation := applyMessage.Command.(Op)
				
				sm.applyOperation(operation)
				//log.Printf("smserver %d finished handling request", sm.me)
				//log.Printf("smserver %d unlocked", sm.me)
				sm.mu.Unlock()
				
			}
		}
	}(sm)

	return sm
}