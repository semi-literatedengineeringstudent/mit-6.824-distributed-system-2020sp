package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	ErrServerKilled  = "ErrServerKilled"

	ErrAlreadyReceived = "ErrAlreadyReceived"
)

//type Err string

type DeletePrevRequestArgs struct {
	PrevRequests []int64
}
type DeletePrevRequestReply struct {
	Err string
}

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
