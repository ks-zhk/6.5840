package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrConfigNotLatest = "ErrNotLatest"
	ErrTooNew          = "ErrTooNew" // 表示cfg太新了，延缓处理。
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid ClientId
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid ClientId
}

type GetReply struct {
	Err   Err
	Value string
}
type MigrateArgs struct {
	Cfg shardctrler.Config
	KV  map[string]string
	Cid ClientId
}
type MigrateReply struct {
	Cfg shardctrler.Config
	Err Err
}
