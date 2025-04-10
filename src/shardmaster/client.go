package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

import "sync"

import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	Client_Serial_Number int64 // serial number with which we uniquely identify the client
	Sequence_Number int //sequence number for requests used for duplication detection to ensure linearizability

	mu      sync.Mutex
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
	// Your code here.

	ck.Client_Serial_Number = nrand()
	ck.Sequence_Number = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num


	args.Client_Serial_Number = ck.Client_Serial_Number
	ck.Sequence_Number = ck.Sequence_Number + 1
	args.Sequence_Number = ck.Sequence_Number
	log.Printf("client %d init Query request with num %d, sequence number %d", ck.Client_Serial_Number, num, args.Sequence_Number)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				log.Printf("client %d received Query request with num %d, sequence number %d", ck.Client_Serial_Number, num, args.Sequence_Number)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers


	args.Client_Serial_Number = ck.Client_Serial_Number
	ck.Sequence_Number = ck.Sequence_Number + 1
	args.Sequence_Number = ck.Sequence_Number
	log.Printf("client %d init Join request sequence number %d", ck.Client_Serial_Number, args.Sequence_Number)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				log.Printf("client %d received Join request sequence number %d", ck.Client_Serial_Number, args.Sequence_Number)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids


	args.Client_Serial_Number = ck.Client_Serial_Number
	ck.Sequence_Number = ck.Sequence_Number + 1
	args.Sequence_Number = ck.Sequence_Number
	log.Printf("client %d init Leave request sequence number %d", ck.Client_Serial_Number, args.Sequence_Number)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				log.Printf("client %d received Leave request sequence number %d", ck.Client_Serial_Number, args.Sequence_Number)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid


	args.Client_Serial_Number = ck.Client_Serial_Number
	ck.Sequence_Number = ck.Sequence_Number + 1
	args.Sequence_Number = ck.Sequence_Number
	log.Printf("client %d init Move request with shard %d, gid %d, and sequence number %d", ck.Client_Serial_Number, shard, gid, args.Sequence_Number)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				log.Printf("client %d received Move request with shard %d, gid %d, and sequence number %d", ck.Client_Serial_Number, shard, gid, args.Sequence_Number)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
