package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/utils"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     OpType
	Key      string
	Args     string
	ClientId ClientId
}
type OpType int

const (
	AppendOp OpType = 0
	PutOp    OpType = 1
	GetLog   OpType = 2
	NoneOp   OpType = 3
)

type OpRes int

const (
	OpResOk   OpRes = 0
	OpResFail OpRes = 1
	OpUnknown OpRes = 2
)

type ClientId struct {
	ClerkId       int
	NextCallIndex int
}

func (cd *ClientId) previousCallIndex() ClientId {
	return ClientId{ClerkId: cd.ClerkId, NextCallIndex: cd.NextCallIndex - 1}
}

type OpChan struct {
	expectTerm int
	op         Op
	cond       *utils.MCond
	opRes      OpRes
	index      int
	val        string
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

type GetWait struct {
	cond     *sync.Cond
	res      string
	key      string
	exit     bool
	finished bool
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
	lastApplied       int
	snapshot          []byte
	snapshotLastIndex int
	wTable            map[int]*GetWait // Get的等待表
}

func (kv *KVServer) applyCommand(op Op) string {
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
	if res, ok := kv.stateMachine[op.Key]; ok {
		return res
	} else {
		return ""
	}
}

//func (kv *KVServer) clearStaleLogs(msg raft.ApplyMsg) {
//	if _, ok := msg.Command.(Op); ok {
//
//	}
//}

// put a no-op into this server, to update the commit to the latest, for updating the server.cacheTable to the latest
func (kv *KVServer) putNoneOp() Err {
	kv.mu.Lock()
	DPrintf("[server %v] in putNoneOp\n", kv.me)
	clientId := ClientId{
		ClerkId:       0, // not exit clientId
		NextCallIndex: 0,
	}
	op := Op{
		Type:     NoneOp,
		Key:      "",
		Args:     "",
		ClientId: clientId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	DPrintf("[server %v] wait for PutNoneOp Success\n", kv.me)
	opChan := &OpChan{
		cond:       utils.NewCond(&sync.Mutex{}),
		expectTerm: term,
		op:         op,
		opRes:      OpUnknown,
		index:      index,
		val:        "",
	}
	kv.cacheTable.insert(clientId, opChan)
	kv.mu.Unlock()
	opChan.cond.L.Lock()
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	DPrintf("[server %v] opRes = %v\n", kv.me, opChan.opRes)
	var err Err
	if opChan.opRes == OpUnknown || opChan.opRes == OpResFail {
		err = ErrCommitFailed
	} else {
		err = OK
	}
	opChan.cond.L.Unlock()
	return err
}
func (kv *KVServer) applier(ch chan raft.ApplyMsg) {
	for msg := range ch {
		if kv.killed() {
			return
		}
		if msg.CommandValid {
			kv.mu.Lock()
			if op, ok := msg.Command.(Op); ok && msg.CommandIndex > kv.lastApplied {
				if opChan, ok := kv.cacheTable.tryGetOpChan(op.ClientId); ok {
					val := kv.applyCommand(op)
					opChan.cond.L.Lock()
					opChan.val = val
					if opChan.opRes == OpUnknown {
						if opChan.index == msg.CommandIndex && opChan.expectTerm == msg.CommandTerm {
							opChan.opRes = OpResOk
						} else {
							opChan.opRes = OpResFail
						}
					}
					opChan.cond.Broadcast()
					opChan.cond.L.Unlock()
				} else {
					// 添加一下这一步非常关键。但是呢，返回只是它commit了，不代表server也commit了，
					// 这只能防止一部分情况
					// 还需要添加额外的限制来进行防止。
					// 如何解决这个问题 client_request -> hit leader, start, success replicate, commit -> reply error -> become follower,
					// client_request -> hit new leader, -> .....
					// 解决方案：在每次append之前，判断当前server是否进行了部分转换，如果进行了转换，则现进行一次None-op的append，用于将该server中的所有log进行一次commit
					// 最逆天的解决方案：每次append之前，都直接put一个空操作。
					// 驱使下方代码的执行！。完美解决了兄弟们。
					if op.Type != NoneOp {
						opChan := OpChan{
							expectTerm: msg.CommandTerm,
							op:         op,
							cond:       utils.NewCond(&sync.Mutex{}),
							opRes:      OpResOk,
							index:      msg.CommandIndex,
							val:        kv.applyCommand(op),
						}
						kv.cacheTable.insert(op.ClientId, &opChan)
					}
				}
				kv.lastApplied = msg.CommandIndex
			}
			kv.mu.Unlock()
		}
	}
}
func (kv *KVServer) applierOld(ch chan raft.ApplyMsg) {
	for msg := range ch {
		if !msg.SnapshotValid && !msg.CommandValid {
			return
		}
		//DPrintf("[s %v] get a commit %v\n", kv.me, msg)
		if msg.CommandValid {
			kv.mu.Lock()
			index := msg.CommandIndex
			term := msg.CommandTerm
			if op, ok := msg.Command.(Op); ok {
				DPrintf("[s %v] get a command apply, command = %v\n", kv.me, msg)
				if msg.CommandIndex > kv.lastApplied {
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
					kv.lastApplied = msg.CommandIndex
				}
				for _, v := range kv.cacheTable.dupTable {
					if v.index == msg.CommandIndex && v.expectTerm == msg.CommandTerm {
						v.cond.L.Lock()
						if op.Type == GetLog {
							if val, ok := kv.stateMachine[op.Key]; ok {
								v.val = val
							} else {
								v.val = ""
							}
						}
						if v.opRes == OpUnknown {
							if v.expectTerm == term && v.index == index {
								v.opRes = OpResOk
							} else {
								v.opRes = OpResFail
							}
							v.cond.Broadcast()
						}
						v.cond.L.Unlock()
					} else if v.index == msg.CommandIndex && v.expectTerm != msg.CommandTerm {
						v.cond.L.Lock()
						v.opRes = OpResFail
						v.cond.Broadcast()
						v.cond.L.Unlock()
					} else {
						if v.index > msg.CommandIndex && v.expectTerm < msg.CommandTerm {
							// FAIL
							v.cond.L.Lock()
							v.opRes = OpResFail
							v.cond.Broadcast()
							v.cond.L.Unlock()
						}
						if v.index < msg.CommandIndex && v.expectTerm > msg.CommandTerm {
							v.cond.L.Lock()
							v.opRes = OpResFail
							v.cond.Broadcast()
							v.cond.L.Unlock()
						}
					}
				}
			}
			kv.mu.Unlock()
		} else {
			// TODO: 等待实现snapshot的形式
		}
	}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	noRes := kv.putNoneOp()
	if noRes != OK {
		reply.Err = noRes
		return
	}
	DPrintf("[server %v] in PutAppend, noRes = %v\n", kv.me, noRes)
	kv.mu.Lock()
	op := Op{}
	clientId := ClientId{ClerkId: args.ClerkId, NextCallIndex: args.NextCallIndex}
	kv.cacheTable.clearByClientId(clientId.previousCallIndex())
	res, ok := kv.cacheTable.tryGetOpChan(clientId)
	DPrintf("[server %v] res = %v\n", kv.me, res)
	if ok == false {
		DPrintf("[server %v] in PutAppend, first PutAppend\n", kv.me)
		if args.Op == "Put" {
			op.Type = PutOp
		} else if args.Op == "Append" {
			op.Type = AppendOp
		} else if args.Op == "GetLog" {
			op.Type = GetLog
		} else {
			reply.Err = ErrNoSupportOp
			kv.mu.Unlock()
			return
		}
		op.Key = args.Key
		op.Args = args.Value
		op.ClientId = clientId
		index, term, isLeader := kv.rf.Start(op)

		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		DPrintf("[%v][%v] this req %v is fresh new, and this server is a leader, try to start\n", kv.me, term, args)
		opChan := OpChan{
			cond:       utils.NewCond(&sync.Mutex{}),
			expectTerm: term,
			op:         op,
			opRes:      OpUnknown,
			index:      index,
			val:        "",
		}
		DPrintf("[%v] opchan = %v\n", kv.me, opChan)
		opChan.opRes = OpUnknown
		kv.cacheTable.insert(clientId, &opChan)
		res = &opChan
	}
	kv.mu.Unlock()
	res.cond.L.Lock()
	if res.opRes == OpUnknown {
		res.cond.WaitWithTimeout(time.Second)
	}
	if res.opRes == OpResFail || res.opRes == OpUnknown {
		reply.Err = ErrCommitFailed
		reply.Value = res.val
		DPrintf("[%v] in dup apply key = %v, opchan = %v\n", kv.me, args.Key, res)
	} else {
		reply.Err = OK
		reply.LogIndex = res.index
		reply.Value = res.val
		DPrintf("[%v] in dup success apply key = %v, opchan = %v\n", kv.me, args.Key, res)
	}
	res.cond.L.Unlock()
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

//func (kv *KVServer) statusChecker() {
//	term, _ := kv.rf.GetState()
//	for !kv.killed() {
//		time.Sleep(time.Millisecond * 500)
//		nTerm, nIsLeader := kv.rf.GetState()
//		if nTerm > term && nIsLeader {
//			// no_op log into raft
//			kv.rf.Start(Op{
//				Type:     NoneOp,
//				Key:      "",
//				Args:     "",
//				ClientId: ClientId{},
//			})
//		}
//		term = nTerm
//	}
//}

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
	kv.wTable = make(map[int]*GetWait)
	kv.snapshotLastIndex = 0
	kv.lastApplied = kv.snapshotLastIndex
	kv.stateMachine = make(map[string]string)
	kv.dead = 0
	go kv.applier(kv.applyCh)
	// You may need initialization code here.

	return kv
}
