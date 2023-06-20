package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
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
	//latestIndex   int
}
type RespIndex struct {
	resp string
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
	//go ck.heartBeat()
	// You'll have to add code here.
	return ck
}
func (ck *Clerk) GetFromSpecServer(sid int, key string, mu *sync.Mutex, finished *bool, ch chan int, resp *string, wg *sync.WaitGroup) {
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	res := false
	for {
		res = false
		for res == false {
			reply = GetReply{}
			mu.Lock()
			if *finished {
				mu.Unlock()
				ch <- sid
				wg.Done()
				return
			}
			mu.Unlock()
			res = ck.servers[sid].Call("KVServer.Get", &args, &reply)
		}
		if reply.Err == ErrNoKey || reply.Err == OK {
			*resp = reply.Value
			ch <- sid
			wg.Done()
			return
		}
		if reply.Err == ErrNoSupportOp || reply.Err == ErrWrongLeader {
			panic("not support error")
		}
	}
}

//func (ck *Clerk) Get(key string) string {
//	//
//	DPrintf("[c %v] try to get key = %v\n", ck.clerkId, key)
//	mu := sync.Mutex{}
//	wg := sync.WaitGroup{}
//	resps := make([]string, len(ck.servers))
//	previousGetString := make([]string, len(ck.servers))
//	previousGet := make([]bool, len(ck.servers))
//	respMap := make(map[string]int)
//	ch := make(chan int, len(ck.servers))
//	finished := false
//	for i, _ := range ck.servers {
//		previousGet[i] = false
//		previousGetString[i] = ""
//		wg.Add(1)
//		go ck.GetFromSpecServer(i, key, &mu, &finished, ch, &resps[i], &wg)
//	}
//
//	res := ""
//	for {
//		sid := <-ch
//		if previousGet[sid] == false {
//			previousGet[sid] = true
//			previousGetString[sid] = resps[sid]
//			respMap[resps[sid]] += 1
//		} else {
//			if previousGetString[sid] != resps[sid] {
//				previousGetString[sid] = resps[sid]
//				respMap[resps[sid]] += 1
//			}
//		}
//		if respMap[resps[sid]] >= (len(ck.servers)+1)/2 {
//			res = resps[sid]
//			mu.Lock()
//			finished = true
//			mu.Unlock()
//			break
//		}
//		wg.Add(1)
//		go ck.GetFromSpecServer(sid, key, &mu, &finished, ch, &resps[sid], &wg)
//	}
//	wg.Wait()
//	//DPrintf("[%v] key = %Get  = %v\n", ck.clerkId, res)
//	return res
//}

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
func (ck *Clerk) GetOld(key string) string {
	DPrintf("[%v] try to get value by key = %v\n", ck.clerkId, key)
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	res := false
	for {
		res = false
		for res == false {
			reply = GetReply{}
			res = ck.servers[ck.nowLeader].Call("KVServer.Get", &args, &reply)
		}
		DPrintf("[%v] get a reply = %v\n", ck.clerkId, reply)
		for reply.Err == ErrWrongLeader {
			ck.nowLeader = (ck.nowLeader + 1) % len(ck.servers)
			res = false
			for res == false {
				reply = GetReply{}
				res = ck.servers[ck.nowLeader].Call("KVServer.Get", &args, &reply)
			}
		}
		if reply.Err == ErrNoKey {
			break
		}
		if reply.Err == ErrNoSupportOp {
			panic("not support get")
		}
		if reply.Err == OK {
			return reply.Value
		}
	}
	// You will have to modify this function.
	return ""
}
func (ck *Clerk) PutAppendSpecificServer(sid int, mu *sync.Mutex, finished *bool, key string, value string, op string, callIndex bool, wg *sync.WaitGroup, ch chan int, val *string) {
	args := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		ClerkId:       ck.clerkId,
		NextCallIndex: callIndex,
	}
	server := ck.servers[sid]
	reply := PutAppendReply{}
	res := false
	for {
		res = false
		for res == false {
			mu.Lock()
			if *finished {
				mu.Unlock()
				wg.Done()
				ch <- sid
				return
			}
			mu.Unlock()
			reply = PutAppendReply{}
			res = server.Call("KVServer.PutAppend", &args, &reply)
		}
		if reply.Err == ErrCommitFailed {
			wg.Done()
			ch <- sid
			return
		}
		if reply.Err == ErrWrongLeader {
			wg.Done()
			ch <- sid
			return
		}
		if reply.Err == ErrNoSupportOp || reply.Err == ErrNoKey {
			panic("stale error")
		}
		if reply.Err == OK {
			mu.Lock()
			if *finished {
				panic("multi leader!")
				return
			}
			*finished = true
			*val = reply.Value
			mu.Unlock()
			wg.Done()
			ch <- sid
			return
		}
	}
}
func (ck *Clerk) heartBeat() {
	for {
		time.Sleep(time.Second)
		ck.Get("0")
	}
}
func (ck *Clerk) PutAppend(key string, value string, op string, res *string) {
	ck.nextCallIndex = !ck.nextCallIndex
	wg := sync.WaitGroup{}
	ch := make(chan int, len(ck.servers))
	finished := false
	mu := sync.Mutex{}
	//for finished == false {
	//	finished = false
	for i, _ := range ck.servers {
		wg.Add(1)
		//DPrintf("")
		go ck.PutAppendSpecificServer(i, &mu, &finished, key, value, op, ck.nextCallIndex, &wg, ch, res)
	}
	for {
		sid := <-ch
		mu.Lock()
		if finished {
			mu.Unlock()
			break
		}
		mu.Unlock()
		wg.Add(1)
		go ck.PutAppendSpecificServer(sid, &mu, &finished, key, value, op, ck.nextCallIndex, &wg, ch, res)
	}
	wg.Wait()
	//}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppendOld(key string, value string, op string) {
	// You will have to modify this function.
	ck.nextCallIndex = !ck.nextCallIndex
	args := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		ClerkId:       ck.clerkId,
		NextCallIndex: ck.nextCallIndex,
	}
	reply := PutAppendReply{}
	res := false
	// check a leader
	for {
		res = false
		for res == false {
			reply = PutAppendReply{}
			res = ck.servers[ck.nowLeader].Call("KVServer.PutAppend", &args, &reply)
		}
		DPrintf("[%v] get a reply = %v\n", ck.clerkId, reply)
		for reply.Err == ErrWrongLeader {
			ck.nowLeader = (ck.nowLeader + 1) % len(ck.servers)
			res = false
			for res == false {
				reply = PutAppendReply{}
				res = ck.servers[ck.nowLeader].Call("KVServer.PutAppend", &args, &reply)
			}
		}
		if reply.Err == ErrCommitFailed {
			continue
		}
		if reply.Err == ErrNoSupportOp {
			panic("not support op")
		}
		break
	}
}
func (ck *Clerk) Put(key string, value string) {
	res := ""
	DPrintf("[c %v] try to Put key = %v, value = %v\n", ck.clerkId, key, value)
	ck.PutAppend(key, value, "Put", &res)
	DPrintf("[c %v] success Put key = %v, value = %v\n", ck.clerkId, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	res := ""
	DPrintf("[c %v] try to Append key = %v, value = %v\n", ck.clerkId, key, value)
	ck.PutAppend(key, value, "Append", &res)
	DPrintf("[c %v] success Append key = %v, value = %v\n", ck.clerkId, key, value)
}
func (ck *Clerk) Get(key string) string {
	res := ""
	DPrintf("[c %v] try to GET key = %v\n", ck.clerkId, key)
	ck.PutAppend(key, "", "GetLog", &res)
	DPrintf("[c %v] success to GET key = %v value = %v\n", ck.clerkId, key, res)
	return res
}
