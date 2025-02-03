package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	ErrServerKilled  = "ErrServerKilled"

)

type Err string

type DeletePrevRequestArgs struct {
	PrevRequests []int64
}
type DeletePrevRequestReply struct {
	Err Err
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Serial_Number int64

	//PrevRequests []int64
}

type PutAppendReply struct {
	Err Err

	CurrentLeaderId int
	CurrentLeaderTerm int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	Serial_Number int64

	//PrevRequests []int64
}

type GetReply struct {
	Err   Err
	Value string

	CurrentLeaderId int
	CurrentLeaderTerm int
}
