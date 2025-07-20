package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrMigrationInconsistent = "ErrMigrationInconsistent" 
	// meanning the server from whom current server asks shard is not in same stage of migration therefore cannot migrate shard to the current server
	// more specificially, the server receiving request either has smaller version number in shard (its config agreement is not up to date)
	ErrGarbage = "ErrGarbage" 
	// meaning the server from whom current server asks shard has received ack signal
	// so the current server that requests data can just wait until the shard data is received by the server through raft channel
	ErrPulling = "ErrPulling"
	// meaning the gid server client requests the data from is the rightful owner of the shard but is still pulling
	// and thus cannot reply yet, and the client should retry the same gid server 

	ErrServerKilled  = "ErrServerKilled"

	Garbage = 0 // indicating the gid server currently not owning the shard, and the rightful owner has the shard so the owner simply cleared the shard
	Pulling = 1 // indicating the gid server currently owns the shard but does not have it, so it is trying to pull the shard from the rightful owner from previous config
	Serving = 2 // indicating the gid server currently owns the shard and has pulled it from the owner in previous config, and is now able to serve the data
	Sending = 3 // indicating the gid server currently not owning the shard, but has not received ack signal from the rightful owner that needs the shard, so the data still persists in the storage
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Client_Serial_Number int64

	Received_Sequence_Number int
	Sequence_Number int

	Client_Config_Num int

	Shard int
}

type PutAppendReply struct {
	Err string

	CurrentLeaderId int
	CurrentLeaderTerm int

	ServerRole int

	Server_Config_Num int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	Client_Serial_Number int64

	Received_Sequence_Number int
	Sequence_Number int

	Client_Config_Num int

	Shard int
}

type GetReply struct {
	Err   string
	Value string

	CurrentLeaderId int
	CurrentLeaderTerm int

	ServerRole int

	Server_Config_Num int
}

type FetchShardArgs struct {
	ShardRequested int // the shard requested by the server requesting
	
	Num_Target int 
	// the config number of the target config the server tries to migrate to
	// this is to ensure both Gid leader that requesting data and the Gid leader that is returning data are migrating to the same config
	// at the moment we assume migration for entire server group would be continuous-- no gap in migration, not migrate from i to version > i + 1

}

type FetchShardReply struct {
	Err string // error of reply

	Data map[string]string // the shard data the other server returns to the current server

	Config_Num int // config number the server that receives request has if we have ErrGarbage or ErrMigrationInconsistent

	State int //state the server that receives request has if we have ErrGarbage or ErrMigrationInconsistent

	CurrentLeaderId int
}

type AckShardArgs struct {
	ShardAcked int // the shard that has been received by the server

	Num_Target int
	// the config number the 
}

type AckShardReply struct {
	Err string 
	// the id of current leader in gid raft cluster

	Config_Num int // config number the server that receives request has if we have ErrGarbage or ErrMigrationInconsistent

	State int //state the server that receives request has if we have ErrGarbage or ErrMigrationInconsistent

	CurrentLeaderId int
}