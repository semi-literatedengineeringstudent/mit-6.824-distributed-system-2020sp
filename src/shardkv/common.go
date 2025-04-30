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
	ErrMigrationNotStarted = "ErrMigrationNotStarted" // returned when server leader for GID whose data is requested has not had config aggrement with its raft peers in the group

	Operating = 0 // not migrating, means the server can operate
	NeedMigration = 1 // servers reach agreement to migrate but migration has not started yet
	MigratingAsLeader = 2 // this server is the leader of current gid group and is in charge of exchange data with other servers
	MigratingAsNoneLeader = 3 // this server is not the leader of current gid group and is not responsible for data exchange but is prepared to act as leader to exchange data with other shard servers
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
}

type PutAppendReply struct {
	Err string

	CurrentLeaderId int
	CurrentLeaderTerm int

	ServerRole int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	Client_Serial_Number int64

	Received_Sequence_Number int
	Sequence_Number int
}

type GetReply struct {
	Err   string
	Value string

	CurrentLeaderId int
	CurrentLeaderTerm int

	ServerRole int
}

type RequestShardsArgs struct {
	ShardsRequested []int // a list of shards requested by the server
	Num int 
	// the config number of the target config the server tries to migrate to
	// this is to ensure both Gid leader that requesting data and the Gid leader that is returning data are migrating to the same config
	// at the moment we assume migration for entire server group would be continuous-- no gap in migration, not migrate from i to version > i + 1
}
