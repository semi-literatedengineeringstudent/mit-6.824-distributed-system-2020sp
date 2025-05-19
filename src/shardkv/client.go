package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

//import "log"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	Client_Serial_Number int64

	Sequence_Number_Gid map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.Client_Serial_Number = nrand()

	ck.Sequence_Number_Gid = make(map[int]int)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.Client_Serial_Number = ck.Client_Serial_Number

	//log.Printf("Client %d init Get request with key %s", ck.Client_Serial_Number, key)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			args.Sequence_Number = ck.Sequence_Number_Gid[gid] + 1
			args.Received_Sequence_Number = ck.Sequence_Number_Gid[gid] 
			args.Client_Config_Num = ck.config.Num

			args.Shard = shard
			
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				//log.Printf("Client %d init Get request with key %s to server %d in groups with gid %d with sequence number %d at shard %d", ck.Client_Serial_Number, key, si, gid, args.Sequence_Number, shard)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.Sequence_Number_Gid[gid] = ck.Sequence_Number_Gid[gid] + 1
					//log.Printf("Client %d successfully received response from Get request with key %s to server %d in groups with gid %d with sequence number %d at shard %d", ck.Client_Serial_Number, key, si, gid, args.Sequence_Number, shard)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.Sequence_Number_Gid[gid] = ck.Sequence_Number_Gid[gid] + 1
					//log.Printf("Client %d got ErrWrongGroup for Get request with key %s to server %d in groups with gid %d with sequence number %d, Client_Config_Number is %d, Server_Config_Number is %d at shard %d, in case of standalone server, try another one", ck.Client_Serial_Number, key, si, gid, args.Sequence_Number, args.Client_Config_Num, reply.Server_Config_Num, shard)
					continue
				} 
				if ok && (reply.Err == ErrWrongLeader) {
					//log.Printf("Client %d got ErrWrongLeader for Get request with key %s to server %d in groups with gid %d with sequence number %d, try a different server at shard %d", ck.Client_Serial_Number, key, si, gid, args.Sequence_Number, shard)
					continue
				}

				if ok && (reply.Err == ErrServerKilled) {
					//log.Printf("Client %d got ErrServerKilled for Get request with key %s to server %d in groups with gid %d with sequence number %d, try a different server at shard %d", ck.Client_Serial_Number, key, si, gid, args.Sequence_Number, shard)
					continue
				}
				
				if !ok {
					// just try another server
					//log.Printf("Client %d got did not receive response for Get request with key %s to server %d in groups with gid %d with sequence number %d, try a different server at shard %d", ck.Client_Serial_Number, key, si, gid, args.Sequence_Number, shard)
					continue
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		//log.Printf("did not get reply, fetch latest config")
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
		for gid, _ := range ck.config.Groups {
			_, ok := ck.Sequence_Number_Gid[gid]
			if !ok {
				ck.Sequence_Number_Gid[gid] = 0
			}
		}
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	args.Client_Serial_Number = ck.Client_Serial_Number

	//log.Printf("Client %d init %s request with key %s and value %s", ck.Client_Serial_Number, op, key, value)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			args.Sequence_Number = ck.Sequence_Number_Gid[gid] + 1
			args.Received_Sequence_Number = ck.Sequence_Number_Gid[gid] 
			args.Client_Config_Num = ck.config.Num
			args.Shard = shard
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply

				//log.Printf("Client %d init %s request with key %s and value %s to server %d in groups with gid %d with sequence number %d", ck.Client_Serial_Number, op, key, value, si, gid, args.Sequence_Number)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK) {
					ck.Sequence_Number_Gid[gid] = ck.Sequence_Number_Gid[gid] + 1
					//log.Printf("Client %d successfully received response from %s request with key %s and value %s to server %d in groups with gid %d with sequence number %d at shard %d", ck.Client_Serial_Number, op, key, value, si, gid, args.Sequence_Number, shard)
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.Sequence_Number_Gid[gid] = ck.Sequence_Number_Gid[gid] + 1
					//log.Printf("Client %d got ErrWrongGroup for %s request with key %s and value %s to server %d in groups with gid %d with sequence number %d, Client_Config_Number is %d, Server_Config_Number is %d at shard %d, in case of standalone server we try another one.", ck.Client_Serial_Number, op, key, value, si, gid, args.Sequence_Number, args.Client_Config_Num, reply.Server_Config_Num, shard)
					continue
				}
				if ok && (reply.Err == ErrWrongLeader) {
					//log.Printf("Client %d got ErrWrongLeader for %s request with key %s and value %s to server %d in groups with gid %d with sequence number %d, try a different server at shard %d", ck.Client_Serial_Number, op, key, value, si, gid, args.Sequence_Number, shard)
					continue
				}

				if ok && (reply.Err == ErrServerKilled) {
					//log.Printf("Client %d got ErrServerKilled for %s request with key %s and value %s to server %d in groups with gid %d with sequence number %d, try a different server at shard %d", ck.Client_Serial_Number, op, key, value, si, gid, args.Sequence_Number, shard)
					continue
				}

				if !ok {
					//log.Printf("Client %d got did not receive response for %s request with key %s and value %s to server %d in groups with gid %d with sequence number %d, try a different server at shard %d", ck.Client_Serial_Number, op, key, value, si, gid, args.Sequence_Number, sha)
					continue
				}
				// ... not ok, or ErrWrongLeader
			}
		}

		//log.Printf("did not get reply, fetch latest config")
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
		for gid, _ := range ck.config.Groups {
			_, ok := ck.Sequence_Number_Gid[gid]
			if !ok {
				ck.Sequence_Number_Gid[gid] = 0
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
