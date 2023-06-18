package kvraft

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

// client本身并不会产生并发，并发的是多个client并发。
// 因此client这里不需要处理并法逻辑，server那边需要处理并发逻辑
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// leader缓存，减少rpc的调用。
	nowLeader     int
	clerkId       int
	nextCallIndex bool // 0 or 1
}

// 为了简单实现，这里就用全局变量了。
// 如果需要更强的通用性以及安全性，可以考虑向server进行注册。
var nextIndex int = 0
var nmu sync.Mutex = sync.Mutex{}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.nowLeader = 0 //假设0成为了leader
	ck.nextCallIndex = false
	nmu.Lock()
	ck.clerkId = nextIndex
	nextIndex += 1
	nmu.Unlock()
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
