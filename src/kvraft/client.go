package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

//import "log"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	numberOfServers int

	currentLeaderId int
	currentLeaderTerm int

	prevRequests []int64
}

func (ck *Clerk)randServer() int {

	// Generate a random number between 0 and numberOfServers - 1
	serverIndex, _ := rand.Int(rand.Reader, big.NewInt(int64(ck.numberOfServers)))

	return int(serverIndex.Int64())
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.numberOfServers = len(servers)

	ck.currentLeaderId = invalid_leader
	ck.currentLeaderTerm = invalid_term

	ck.prevRequests = make([]int64, 0)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}

	args.Key = key
	args.Serial_Number = nrand()
	args.PrevRequests = make([]int64, 0)
	for i := 0; i < len(ck.prevRequests); i++ {
		args.PrevRequests = append(args.PrevRequests, ck.prevRequests[i])
	}

	//log.Printf("initiate Get request with key %s and serial number %d", key, args.Serial_Number)

	reply := GetReply{}

	for {
		leaderId := ck.currentLeaderId

		if (leaderId  == invalid_leader) {
			leaderId = ck.randServer()
		} 

		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			error := reply.Err
			if (error == OK) {
				//log.Printf("Get request with key %s and serial number %d is successful, get %s", key, args.Serial_Number, reply.Value)
				ck.prevRequests = append(ck.prevRequests, args.Serial_Number)
				return reply.Value
			} else if (error == ErrNoKey) {
				//log.Printf("No key, Get request with key %s and serial number %d has failed", key, args.Serial_Number)
				return empty_string
			} else if (error == ErrServerKilled) {
				//log.Printf("server with id %d of term %d has been killed, Get request with key %s and serial number %d is unsuccessful, retry with random server",leaderId,  ck.currentLeaderTerm, key, args.Serial_Number)
				ck.currentLeaderId = invalid_leader
			} else {
				
				if (reply.CurrentLeaderTerm > ck.currentLeaderTerm) {
					ck.currentLeaderId = reply.CurrentLeaderId
					ck.currentLeaderTerm = reply.CurrentLeaderTerm
					//log.Printf("server with id %d of term %d has been lost leadership/or is not leader, Get request with key %s and serial number %d is unsuccessful, retry with new leader server of id %d and term %d",leaderId,  ck.currentLeaderTerm, key, args.Serial_Number, reply.CurrentLeaderId, reply.CurrentLeaderTerm)
				} else if (reply.CurrentLeaderTerm == ck.currentLeaderTerm){
					if (reply.CurrentLeaderId == ck.currentLeaderId) {
						ck.currentLeaderId = invalid_leader
						//log.Printf("server with id %d of term %d has been lost leadership/or is not leader, Get request with key %s and serial number %d is unsuccessful, leader did not realize lose of leadership, retry with random server",leaderId,  ck.currentLeaderTerm, key, args.Serial_Number)
					} else {
						ck.currentLeaderId = reply.CurrentLeaderId
					}
				} else {
					ck.currentLeaderId = invalid_leader
				}
			}
		} else {
			//log.Printf("did not receive reply from server with id %d of term %d, Get request with key %s and serial number %d is unsuccessful possibily due to network partitiob, retry with same server",leaderId,  ck.currentLeaderTerm, key, args.Serial_Number)
			ck.currentLeaderId = ck.randServer()
		}
	}

	return empty_string
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{}

	args.Key = key
	args.Value = value
	args.Op = op

	args.Serial_Number = nrand()
	args.PrevRequests = make([]int64, 0)
	for i := 0; i < len(ck.prevRequests); i++ {
		args.PrevRequests = append(args.PrevRequests, ck.prevRequests[i])
	}

	//log.Printf("initiate %s request with (key %s, value %s) and serial number %d", op, key, value, args.Serial_Number)

	reply := PutAppendReply{}

	for {
		leaderId := ck.currentLeaderId
		
		if (leaderId  == invalid_leader) {
			leaderId = ck.randServer()
		} 

		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			error := reply.Err
			if (error == OK) {
				//log.Printf("%s request with (key %s, value %s) and serial number %d is successful", op, key, value, args.Serial_Number)
				ck.prevRequests = append(ck.prevRequests, args.Serial_Number)
				return 
			} else if (error == ErrServerKilled) {
				//log.Printf("server with id %d of term %d has been killed, %s request with (key %s, value %s) and serial number %d is unsuccessful, retry with random server",leaderId,  ck.currentLeaderTerm, op, key, value, args.Serial_Number)
				ck.currentLeaderId = invalid_leader
			} else {
				if (reply.CurrentLeaderTerm > ck.currentLeaderTerm) {
					//log.Printf("server with id %d of term %d has been lost leadership/or not a leader, %s request with (key %s, value %s) and serial number %d is unsuccessful, retry with new leader server of id %d and term %d",leaderId,  ck.currentLeaderTerm, op, key, value, args.Serial_Number, reply.CurrentLeaderId, reply.CurrentLeaderTerm)
					ck.currentLeaderId = reply.CurrentLeaderId
					ck.currentLeaderTerm = reply.CurrentLeaderTerm
				} else if (reply.CurrentLeaderTerm == ck.currentLeaderTerm){
					if (reply.CurrentLeaderId == ck.currentLeaderId) {
						ck.currentLeaderId = invalid_leader
						//log.Printf("server with id %d of term %d has been lost leadership/or is not leader, Get request with key %s and serial number %d is unsuccessful, leader did not realize lose of leadership, retry with random server",leaderId,  ck.currentLeaderTerm, key, args.Serial_Number)
					} else {
						ck.currentLeaderId = reply.CurrentLeaderId
					}
				} else {
					ck.currentLeaderId = invalid_leader
				}
			}
		} else {
			//log.Printf("did not receive reply from server with id %d of term %d, %s request with (key %s, value %s) and serial number %d is unsuccessful, retry with random server",leaderId,  ck.currentLeaderTerm, op, key, value, args.Serial_Number)
			ck.currentLeaderId = ck.randServer()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
