package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrNoSupportOp  = "ErrNoSupportOp"
	ErrCommitFailed = "ErrCommitFailed"
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
	ClerkId       int
	NextCallIndex bool
	Rid           int
}

type PutAppendReply struct {
	Err      Err
	Term     int
	LogIndex int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
type AckArgs struct {
	Rid int
}
type AckReply struct {
}
