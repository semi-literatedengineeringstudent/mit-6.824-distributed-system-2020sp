package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	Get            = "Get" // on existiing key, return most recent value, on non-existing key, return ErrNoKey
	Put            = "Put" // on existing key or non-existing key, replace
	Append         = "Append" // on existing key, concatenate, on non-existing key, act like put
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

	Serial_Number int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	Serial_Number int64
}

type GetReply struct {
	Err   Err
	Value string
}
