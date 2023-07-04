package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"6.5840/utils"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type ClientId struct {
	ClerkId       int
	NextCallIndex int
}

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
func (cd *ClientId) previousCallIndex() ClientId {
	return ClientId{ClerkId: cd.ClerkId, NextCallIndex: cd.NextCallIndex - 1}
}

type OpRes int

const (
	OpResOk      OpRes = 0
	OpUnknown    OpRes = 1
	OpWrongGroup OpRes = 2
)

type OpType int

const (
	AppendOp     OpType = 0
	PutOp        OpType = 1
	GetLog       OpType = 2
	NoneOp       OpType = 3
	CfgChgLog    OpType = 4
	GoFlightLog  OpType = 5
	GetFlightLog OpType = 6
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key  string
	Args string
	Type OpType
	Cid  ClientId
	Mid  MigrateId
	Cfg  shardctrler.Config
	KV   map[string]string
	Gid  int
}
type OpChan struct {
	index int
	cond  *utils.MCond
	opRes OpRes
	val   string
}

type MigrateInfo struct {
	flightFinished   map[int]bool
	waitGetFinished  map[int]bool
	flightAims       []int
	lastFlightSeqNum int
}
type MigrateId struct {
	cfgNum int
	seqNum int // from big to small, eg. 8,7,6,5,4,3,2,1,0
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead                 int32
	clientQueryResStore  map[ClientId]string
	clientLastCallIndex  map[int]int
	lastApplied          int
	cacheTable           CacheTable
	migrateDupTbl        map[MigrateId]*OpChan
	snapshot             []byte
	snapshotLastIndex    int
	persister            *raft.Persister
	migrateNextCallIndex int

	stateMachine map[string]string
	// 表示其负责的shard
	nextCfgNum        int
	cfg               shardctrler.Config
	newCfg            shardctrler.Config
	respShard         map[int]bool
	migrateInfo       MigrateInfo
	migratingToCfgNum int
}

func intExitInArray(ele int, arr []int) bool {
	for _, el := range arr {
		if ele == el {
			return true
		}
	}
	return false
}

//func (kv *ShardKV) isChangingNoneLock() bool {
//	kv.migrateNeed.cond.L.Lock()
//	defer kv.migrateNeed.cond.L.Unlock()
//	return kv.migrateNeed.needGetRPCNum != 0 || kv.migrateNeed.needSendRPCNum != 0
//}
func (kv *ShardKV) applyCommand(op Op) string {
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
func (opChan *OpChan) ok(val string) {
	opChan.cond.L.Lock()
	opChan.opRes = OpResOk
	opChan.val = val
	opChan.cond.Broadcast()
	opChan.cond.L.Unlock()
}
func (kv *ShardKV) applier(ch chan raft.ApplyMsg) {
	for msg := range ch {
		if kv.killed() {
			return
		}
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex != kv.lastApplied+1 {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			if op.Type == PutOp || op.Type == AppendOp || op.Type == GetLog {
				opChan, ok := kv.cacheTable.tryGetOpChan(op.Cid)
				if _, okk := kv.clientLastCallIndex[op.Cid.ClerkId]; okk && op.Cid.NextCallIndex <= kv.clientLastCallIndex[op.Cid.ClerkId] {
					if ok {
						opChan.ok(kv.clientQueryResStore[op.Cid])
					}
					kv.mu.Unlock()
					continue
				}
				val := kv.applyCommand(op)
				if ok {
					opChan.ok(val)
				}
				if op.Type == GetLog {
					kv.clientQueryResStore[op.Cid] = val
				}
				kv.clientLastCallIndex[op.Cid.ClerkId] = op.Cid.NextCallIndex
			}
			if op.Type == CfgChgLog {
				if kv.migratingToCfgNum == -1 && kv.nextCfgNum == op.Cfg.Num {
					kv.migratingToCfgNum = kv.nextCfgNum
				}
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshotNoneLock(kv.lastApplied))
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			if msg.Snapshot == nil {
				panic("snapshot can not be nil")
			}
			kv.mu.Lock()
			kv.installSnapshotNoneLock(msg.Snapshot)
			kv.mu.Unlock()
		} else {
			panic("invalid apply Type")
		}
	}
}
func (kv *ShardKV) makeMigrateInfo(newCfg shardctrler.Config) MigrateInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldCfg := kv.cfg
	if newCfg.Num != oldCfg.Num+1 {
		panic("should increase one by one")
	}
	aims := make(map[int]bool)
	gets := make(map[int]bool)
	flightAims := []int{}
	for idx, val := range oldCfg.Shards {
		if newCfg.Shards[idx] == val {
			continue
		}
		if val == kv.gid {
			if _, ok := aims[newCfg.Shards[idx]]; !ok {
				flightAims = append(flightAims, newCfg.Shards[idx])
			}
			aims[newCfg.Shards[idx]] = false
		}
		if newCfg.Shards[idx] == kv.gid {
			gets[val] = false
		}
	}
	return MigrateInfo{
		flightFinished:   aims,
		waitGetFinished:  gets,
		flightAims:       flightAims,
		lastFlightSeqNum: 0,
	}
}

//func migrateTo(me int, nextCallIndex int, peers []*labrpc.ClientEnd, kv map[string]string, cfg shardctrler.Config) {
//	args := MigrateArgs{}
//	args.KV = kv
//	args.Cfg = cfg
//	args.Cid = ClientId{ClerkId: -me, NextCallIndex: nextCallIndex}
//	for i := 0; i < len(peers); i = (i + 1) % len(peers) {
//		reply := MigrateReply{}
//		ok := peers[i].Call("ShardKV.Migrate", &args, &reply)
//		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTooNew {
//			if i == len(peers)-1 {
//				time.Sleep(100 * time.Millisecond)
//			}
//			continue
//		}
//		break
//	}
//}

//func (kv *ShardKV) isChangingNoneLock() bool {
//	for _, v := range kv.migrateInfo.flightFinished {
//		if !v {
//			return true
//		}
//	}
//	for _, v := range kv.migrateInfo.waitGetFinished {
//		if !v {
//			return true
//		}
//	}
//	return false
//}
func (kv *ShardKV) putCfgChgLog(newCfg shardctrler.Config) Err {
	kv.mu.Lock()
	if newCfg.Num != kv.nextCfgNum {
		kv.mu.Unlock()
		return ErrTooOld
	}
	// TODO: 在applier的时候别忘了的生成cid
	cid := ClientId{ClerkId: 0, NextCallIndex: newCfg.Num}
	op := Op{
		Type: CfgChgLog,
		Cfg:  newCfg,
		Cid:  cid,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	opChan := OpChan{
		cond:  utils.NewCond(&sync.Mutex{}),
		index: index,
		opRes: OpUnknown,
		val:   "",
	}
	kv.cacheTable.insert(cid, &opChan)
	kv.mu.Unlock()
	opChan.cond.L.Lock()
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	defer opChan.cond.L.Unlock()
	if opChan.opRes == OpUnknown {
		return ErrCommitFail
	} else {
		return OK
	}
}
func (kv *ShardKV) putFlight(cfgNum int, seqNum int, aim int) Err {
	kv.mu.Lock()
	mid := MigrateId{cfgNum: cfgNum, seqNum: seqNum}
	op := Op{
		Type: GoFlightLog,
		Mid:  mid,
		Gid:  aim,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	opChan := OpChan{
		cond:  utils.NewCond(&sync.Mutex{}),
		index: index,
		opRes: OpUnknown,
		val:   "",
	}
	kv.migrateDupTbl[mid] = &opChan
	kv.mu.Unlock()
	opChan.cond.L.Lock()
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	defer opChan.cond.L.Unlock()
	if opChan.opRes == OpUnknown {
		return ErrCommitFail
	} else {
		return OK
	}
}
func (kv *ShardKV) configDetection() {
	clerk := shardctrler.MakeClerk(kv.ctrlers)
	var cfg shardctrler.Config
	for !kv.killed() {
		// 每隔100毫秒询问一次配置服务器
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			// 不是leader就返回
			continue
		}
		kv.mu.Lock()
		nextCfgNum := kv.nextCfgNum
		kv.mu.Unlock()
		cfg = clerk.Query(nextCfgNum)
		cfgChgLogOk := false
		for {
			res := kv.putCfgChgLog(cfg)
			if res == OK {
				cfgChgLogOk = true
				break
			}
			if res == ErrWrongLeader {
				break
			}
		}
		if !cfgChgLogOk {
			continue
		}
		migrateInfo := kv.makeMigrateInfo(cfg)
		for idx, aim := range migrateInfo.flightAims {
			flightOk := false
			for {
				res := kv.putFlight(cfg.Num, idx, aim)
				if res == OK {
					flightOk = true
					break
				}
				if res == ErrWrongLeader {
					// !!!防止cpu占用率过高。
					time.Sleep(time.Millisecond * 50)
					break
				}
			}
			if !flightOk {
				break
			}
		}
	}
}
func (kv *ShardKV) makeSnapshotNoneLock(snapshotIndex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshotIndex)
	e.Encode(kv.clientLastCallIndex)
	e.Encode(kv.stateMachine)
	kv.lastApplied = snapshotIndex
	return w.Bytes()
}
func (kv *ShardKV) installSnapshotNoneLock(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var clientApplyInfo map[int]int
	var stateMachine map[string]string
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&clientApplyInfo) != nil ||
		d.Decode(&stateMachine) != nil {
		log.Fatalf("snapshot decode error")
	}
	if lastIncludedIndex < kv.lastApplied {
		return
	}
	//DPrintf("[s %v] install snapshot lastInclude Index = %v, clientApplyInfo = %v, stateMachine = %v\n", kv.me, lastIncludedIndex, clientApplyInfo, stateMachine)
	kv.clientLastCallIndex = clientApplyInfo
	kv.snapshotLastIndex = lastIncludedIndex
	kv.lastApplied = lastIncludedIndex
	kv.stateMachine = stateMachine
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	cid := args.Cid
	op := Op{
		Type: GetLog,
		Cid:  cid,
		Key:  args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	opChan := OpChan{
		cond:  utils.NewCond(&sync.Mutex{}),
		index: index,
		opRes: OpUnknown,
		val:   "",
	}
	kv.cacheTable.insert(cid, &opChan)
	kv.mu.Unlock()
	opChan.cond.L.Lock()
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	reply.Value = opChan.val
	if opChan.opRes == OpUnknown {
		reply.Err = ErrCommitFail
	} else if opChan.opRes == OpWrongGroup {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
	}
	opChan.cond.L.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	cid := args.Cid
	var tp OpType
	if args.Op == "Put" {
		tp = PutOp
	} else {
		tp = AppendOp
	}
	op := Op{
		Type: tp,
		Cid:  cid,
		Key:  args.Key,
		Args: args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	opChan := OpChan{
		cond:  utils.NewCond(&sync.Mutex{}),
		index: index,
		opRes: OpUnknown,
		val:   "",
	}
	kv.cacheTable.insert(cid, &opChan)
	kv.mu.Unlock()
	opChan.cond.L.Lock()
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	if opChan.opRes == OpUnknown {
		reply.Err = ErrCommitFail
	} else if opChan.opRes == OpWrongGroup {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
	}
	opChan.cond.L.Unlock()
	return

}
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	cid := ClientId{ClerkId: -args.Cid.ClerkId, NextCallIndex: args.Cid.NextCallIndex}
	op := Op{
		Type: GetFlightLog,
		KV:   args.KV,
		Cid:  cid, // [gid]cfgNum
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	opChan := OpChan{
		cond:  utils.NewCond(&sync.Mutex{}),
		index: index,
		opRes: OpUnknown,
		val:   "",
	}
	kv.cacheTable.insert(cid, &opChan)
	kv.mu.Unlock()
	opChan.cond.L.Lock()
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	if opChan.opRes == OpUnknown {
		reply.Err = ErrCommitFail
	} else {
		reply.Err = OK
	}
	opChan.cond.L.Unlock()
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.cfg = shardctrler.Config{}
	kv.cfg.Groups = make(map[int][]string)
	kv.cfg.Num = 0
	kv.newCfg = shardctrler.Config{}
	kv.migratingToCfgNum = -1 // -1 means finished
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.migrateNextCallIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.nextCfgNum = 1
	return kv
}
