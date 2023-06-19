package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//func (kv *KVServer) MyDebugNoneLock(format string, a ...interface{}) {
//	DPrintf("KVServer id = %v", kv.me)
//	DPrintf(format, a...)
//}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type OpType
	Key  string
	Args string
}
type OpType int

const (
	AppendOp OpType = 0
	PutOp    OpType = 1
)

type OpRes int

const (
	OpResOk   OpRes = 0
	OpResFail OpRes = 1
	OpUnknown OpRes = 2
)

type ClientId struct {
	ClerkId       int
	NextCallIndex bool
}

func (cd *ClientId) previousCallIndex() ClientId {
	return ClientId{ClerkId: cd.ClerkId, NextCallIndex: !cd.NextCallIndex}
}

type OpChan struct {
	expectTerm int
	op         Op
	cond       *sync.Cond
	opRes      OpRes
	index      int
}

// 错误处理应该指导分支，而不是直接退出
// 不应该侵占任何其他逻辑，做好本分的增删改查即可
type CacheTable struct {
	dupTable map[ClientId]*OpChan
	IndexId  map[int]ClientId
}

func (ct *CacheTable) insert(clientId ClientId, op *OpChan) {
	ct.dupTable[clientId] = op
	ct.IndexId[op.index] = clientId
}
func (ct *CacheTable) tryGetOpChan(id ClientId) (*OpChan, bool) {
	res, ok := ct.dupTable[id]
	return res, ok
}
func (ct *CacheTable) tryGetByIndex(idx int) (*OpChan, bool) {
	if id, ok := ct.IndexId[idx]; ok {
		if op, okk := ct.dupTable[id]; okk {
			return op, true
		}
	}
	return nil, false
}
func (ct *CacheTable) clearByClientId(id ClientId) {
	//delete(ct.dupTable, id)
	if res, ok := ct.dupTable[id]; ok {
		index := res.index
		delete(ct.IndexId, index)
		delete(ct.dupTable, id)
	}
}
func (ct *CacheTable) clearByPreviousId(id ClientId) {
	pid := id.previousCallIndex()
	ct.clearByClientId(pid)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine      map[string]string
	cacheTable        CacheTable
	maxCommitIndex    int
	snapshot          []byte
	snapshotLastIndex int
}

func (kv *KVServer) applier(ch chan raft.ApplyMsg) {
	for msg := range ch {
		if !msg.SnapshotValid && !msg.CommandValid {
			return
		}
		DPrintf("[%v] get a commit %v\n", kv.me, msg)
		if msg.CommandValid {
			kv.mu.Lock()
			index := msg.CommandIndex
			term := msg.CommandTerm
			if opc, ok := kv.cacheTable.tryGetByIndex(index); ok && msg.CommandIndex > kv.maxCommitIndex {
				DPrintf("[%v] hit the pending index = %v\n", kv.me, index)
				opc.cond.L.Lock()
				if opc.opRes == OpUnknown {
					if opc.expectTerm == term {
						opc.opRes = OpResOk
					} else {
						opc.opRes = OpResFail
					}
					opc.cond.Broadcast()
				}
				opc.cond.L.Unlock()
			}
			if op, ok := msg.Command.(Op); ok && msg.CommandIndex > kv.maxCommitIndex {
				if op.Type == AppendOp {
					if m, ok := kv.stateMachine[op.Key]; ok {
						m += op.Args
						kv.stateMachine[op.Key] = m
					} else {
						kv.stateMachine[op.Key] = op.Args
					}
				}
				if op.Type == PutOp {
					kv.stateMachine[op.Key] = op.Args
				}
				kv.maxCommitIndex = msg.CommandIndex
			}
			kv.mu.Unlock()
		} else {
			// TODO: 等待实现snapshot的形式
		}
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// TODO: 思考，是否需要进行leader判断。还是进行majority获取
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	if m, ok := kv.stateMachine[args.Key]; ok {
		reply.Err = OK
		reply.Value = m
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{}
	clientId := ClientId{ClerkId: args.ClerkId, NextCallIndex: args.NextCallIndex}
	kv.cacheTable.clearByClientId(clientId.previousCallIndex())
	DPrintf("[%v] get a PutAppend req = %v\n", kv.me, args)
	res, ok := kv.cacheTable.tryGetOpChan(clientId)
	if ok == false {
		if args.Op == "Put" {
			op.Type = PutOp
		} else if args.Op == "Append" {
			op.Type = AppendOp
		} else {
			reply.Err = ErrNoSupportOp
			kv.mu.Unlock()
			return
		}
		op.Key = args.Key
		op.Args = args.Value
		index, term, isLeader := kv.rf.Start(op)

		if !isLeader {
			DPrintf("[%v] this req is fresh new, however this server is not leader, just return reply\n", kv.me)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		DPrintf("[%v] this req is fresh new, and this server is a leader, try to start\n", kv.me)
		opChan := OpChan{
			cond:       sync.NewCond(&sync.Mutex{}),
			expectTerm: term,
			op:         op,
			index:      index,
		}
		opChan.opRes = OpUnknown
		kv.cacheTable.insert(clientId, &opChan)
		kv.mu.Unlock()
		opChan.cond.L.Lock()
		DPrintf("[%v] success lock the cond lock\n", kv.me)
		for opChan.opRes == OpUnknown {
			opChan.cond.Wait()
		}
		if opChan.opRes == OpResFail {
			reply.Err = ErrCommitFailed
		} else {
			reply.Err = OK
		}
		opChan.cond.L.Unlock()
	} else {
		kv.mu.Unlock()
		res.cond.L.Lock()
		for res.opRes == OpUnknown {
			res.cond.Wait()
		}
		if res.opRes == OpResFail {
			reply.Err = ErrCommitFailed
		} else {
			reply.Err = OK
		}
		res.cond.L.Unlock()
	}
}
func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.applyCh <- raft.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: false,
	}

}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.cacheTable = CacheTable{
		dupTable: make(map[ClientId]*OpChan),
		IndexId:  make(map[int]ClientId),
	}
	kv.snapshotLastIndex = 0
	kv.maxCommitIndex = kv.snapshotLastIndex
	kv.stateMachine = make(map[string]string)
	kv.dead = 0
	go kv.applier(kv.applyCh)
	// You may need initialization code here.

	return kv
}
