package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

//import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	Client_Serial_Number int64 // serial number with which we uniquely identify the client

	numberOfServers int

	currentLeaderId int
	currentLeaderTerm int

	Sequence_Number int 

	RPC_Count int
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

	ck.Client_Serial_Number = nrand()

	ck.numberOfServers = len(servers)

	ck.currentLeaderId = invalid_leader
	//ck.currentLeaderTerm = invalid_term

	ck.Sequence_Number = default_sentinel_index
	ck.RPC_Count = 0

	//log.Printf("make clerk with serial number %d", ck.Client_Serial_Number)

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
	
	args.Client_Serial_Number = ck.Client_Serial_Number

	args.Received_Sequence_Number = ck.Sequence_Number
	ck.Sequence_Number = ck.Sequence_Number + 1
	args.Sequence_Number = ck.Sequence_Number

	reply := GetReply{}
	//log.Printf("Client %d 's get request init with (key %s) and sequence number %d", ck.Client_Serial_Number, key, args.Sequence_Number)

	for {
		leaderId := ck.currentLeaderId
		//log.Printf("leaderId is %d", leaderId)
		if (leaderId  == invalid_leader) {
			leaderId = ck.randServer()
			//log.Printf("For client %d, Get request with key %s and sequence number%d re-route to random server %d", ck.Client_Serial_Number, key, args.Sequence_Number, leaderId)
		} 

		ck.RPC_Count = ck.RPC_Count + 1
		//log.Printf("RPC sent by clerk %d is %d, to %d", ck.Client_Serial_Number, ck.RPC_Count, leaderId)

		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		//log.Printf("clerk %d received response from %d", ck.Client_Serial_Number, leaderId)
		if ok {
			err := reply.Err
			if (err == OK) {
				//log.Printf("For client %d, Get request with key %s and sequence number %d is successful, get %s", ck.Client_Serial_Number, key, args.Sequence_Number, reply.Value)
				ck.currentLeaderId = leaderId
				return reply.Value
			} else if (err == ErrNoKey) {
				//log.Printf("No key, for client %d Get request with key %s and sequence number %d has failed", ck.Client_Serial_Number, key, args.Sequence_Number)
				ck.currentLeaderId = leaderId
				return empty_string
			} else if (err == ErrServerKilled) {
				//log.Printf("server with id %d of term %d has been killed, for client %d Get request with key %s and sequence number %d is unsuccessful, retry with random server",leaderId,  ck.currentLeaderTerm, ck.Client_Serial_Number, key, args.Sequence_Number)
				ck.currentLeaderId = invalid_leader
			} else {
				ck.currentLeaderId = invalid_leader
				//log.Printf("server with id %d of term %d has been lost leadership/or is not leader, role is %s, for client %d Get request with key %s and sequence number %d is unsuccessful, retry with random server", leaderId, reply.CurrentLeaderTerm, role, args.Client_Serial_Number, key, args.Sequence_Number)
			}
		} else {
			//log.Printf("did not receive reply from server with id %d of term %d, for client %d Get request with key %s and sequence number %d is unsuccessful possibily due to network partition, retry with same server", leaderId, ck.currentLeaderTerm, args.Client_Serial_Number, key, args.Sequence_Number)
			ck.currentLeaderId = invalid_leader
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

	args.Client_Serial_Number = ck.Client_Serial_Number

	args.Received_Sequence_Number = ck.Sequence_Number
	ck.Sequence_Number = ck.Sequence_Number + 1
	args.Sequence_Number = ck.Sequence_Number

	reply := PutAppendReply{}
	//log.Printf("Client %d 's %s request init with (key %s, value %s) and sequence number %d", ck.Client_Serial_Number, op, key, value, args.Sequence_Number)
	
	for {
		leaderId := ck.currentLeaderId
		//log.Printf("leaderId is %d", leaderId)
		if (leaderId == invalid_leader) {
			leaderId = ck.randServer()
			//log.Printf("For client %d, %s request with (key %s, value %s) and sequence number %d re-route to random server %d", ck.Client_Serial_Number, op, key, value, args.Sequence_Number, leaderId)
		} 

		ck.RPC_Count = ck.RPC_Count + 1
		//log.Printf("RPC sent by clerk %d is %d, to %d", ck.Client_Serial_Number, ck.RPC_Count, leaderId)

		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		//log.Printf("clerk %d received response from %d", ck.Client_Serial_Number, leaderId)
		if ok {
			err := reply.Err
			if (err == OK) {
				//log.Printf("for client %d 's %s request with (key %s, value %s) and sequence number %d is successful", ck.Client_Serial_Number, op, key, value, args.Sequence_Number)
				ck.currentLeaderId = leaderId
				return 
			} else if (err == ErrServerKilled) {
				//log.Printf("server with id %d of term %d has been killed, for client %d, %s request with (key %s, value %s) and sequence number %d is unsuccessful, retry with random server", leaderId, ck.currentLeaderTerm, ck.Client_Serial_Number, op, key, value, args.Sequence_Number)
				ck.currentLeaderId = invalid_leader
			} else {
				ck.currentLeaderId = invalid_leader
				//log.Printf("server with id %d of term %d has been lost leadership/or is not leader, role is %s, for client %d Get request with key %s and sequence number %d is unsuccessful, retry with random server", leaderId, reply.CurrentLeaderTerm, role, args.Client_Serial_Number, key, args.Sequence_Number)
			}
		} else {
			//log.Printf("did not receive reply from server with id %d of term %d, for client %d, %s request with (key %s, value %s) and sequence number %d is unsuccessful, retry with random server",leaderId,  ck.currentLeaderTerm, ck.Client_Serial_Number, op, key, value, args.Sequence_Number)
			ck.currentLeaderId = invalid_leader
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
