package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

var ClerkNextId int = 1
var gmu sync.Mutex = sync.Mutex{}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	nextOpsIndex int
	clerkId      int
}

func (ck *Clerk) getClientId() ClientId {
	return ClientId{ClerkId: ck.clerkId, NextCallIndex: ck.nextOpsIndex}
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
	gmu.Lock()
	ck.clerkId = ClerkNextId
	ClerkNextId += 1
	gmu.Unlock()
	ck.nextOpsIndex = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.nextOpsIndex += 1
	args.Cid = ck.getClientId()
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("[c %v] success get config = %v\n", ck.clerkId, reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.nextOpsIndex += 1
	args.Servers = servers
	args.Cid = ck.getClientId()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.nextOpsIndex += 1
	args.GIDs = gids
	args.Cid = ck.getClientId()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return
			}
		}
		//time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.nextOpsIndex += 1
	args.Shard = shard
	args.GID = gid
	args.Cid = ck.getClientId()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
