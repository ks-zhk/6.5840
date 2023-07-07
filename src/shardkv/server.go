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

const debugMode bool = true
const RES_TIMEOUT time.Duration = time.Millisecond * 100

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debugMode {
		log.Printf(format, a...)
	}
	return
}

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
	//ct.IndexId[op.index] = clientId
}
func (ct *CacheTable) tryGetOpChan(id ClientId) (*OpChan, bool) {
	res, ok := ct.dupTable[id]
	return res, ok
}
func (ct *CacheTable) clearByClientId(id ClientId) {
	//delete(ct.dupTable, id)
	if res, ok := ct.dupTable[id]; ok {
		index := res.index
		delete(ct.IndexId, index)
		delete(ct.dupTable, id)
	}
}

//	func (ct *CacheTable) clearByPreviousId(id ClientId) {
//		pid := id.previousCallIndex()
//		ct.clearByClientId(pid)
//	}
func (cd *ClientId) previousCallIndex() ClientId {
	return ClientId{ClerkId: cd.ClerkId, NextCallIndex: cd.NextCallIndex - 1}
}

type OpRes int

const (
	OpResOk        OpRes = 0
	OpUnknown      OpRes = 1
	OpWrongGroup   OpRes = 2
	OpCfgTooOld    OpRes = 3
	OpFlightTooNew OpRes = 4
	OpFlightTooOld OpRes = 5
)

type OpType int

const (
	AppendOp          OpType = 0
	PutOp             OpType = 1
	GetLog            OpType = 2
	CfgChgLog         OpType = 4
	GoFlightLog       OpType = 5
	GetFlightLog      OpType = 6
	CfgFlightFinished OpType = 7
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
	CLI  map[int]int
	CQRS map[ClientId]string
	Gid  int
}
type OpChan struct {
	index int
	cond  *utils.MCond
	opRes OpRes
	val   string
}

type MigrateInfo struct {
	FlightFinished    map[int]bool
	FlightShardInfo   map[int][]int
	WaitGetFinished   map[int]bool
	WaitGetShardInfo  map[int][]int
	FlightAllFinished bool
	FlightAims        []int
	LastFlightSeqNum  int
}

func (mgi *MigrateInfo) mgInfoFinished() bool {
	for _, v := range mgi.FlightFinished {
		if v == false {
			return false
		}
	}
	for _, v := range mgi.WaitGetFinished {
		if v == false {
			return false
		}
	}
	return true
}

type MigrateId struct {
	CfgNum int
	SeqNum int // from big to small, eg. 8,7,6,5,4,3,2,1,0
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
	dead                int32
	clientQueryResStore map[ClientId]string
	clientLastCallIndex map[int]int
	lastApplied         int
	cacheTable          CacheTable
	migrateDupTbl       map[MigrateId]*OpChan
	snapshot            []byte
	snapshotLastIndex   int
	persister           *raft.Persister
	//migrateNextCallIndex int

	stateMachine map[string]string
	//cfgOpChan    map[int]*OpChan
	// 表示其负责的shard
	nextCfgNum           int
	cfg                  shardctrler.Config
	newCfg               shardctrler.Config
	respShard            map[int]bool
	migrateInfo          MigrateInfo
	migratingToCfgNum    int
	flightFinishedOpChan map[int]*OpChan
}

func intExitInArray(ele int, arr []int) bool {
	for _, el := range arr {
		if ele == el {
			return true
		}
	}
	return false
}
func (kv *ShardKV) freshNewCfgNoneLock(newCfg shardctrler.Config) {
	kv.cfg = newCfg
	kv.nextCfgNum = newCfg.Num + 1
	kv.migratingToCfgNum = -1
	DPrintf("[scc (%v, %v)] update one fresh new cfg = %v, now resp_shard = %v\n", kv.gid, kv.me, kv.cfg, kv.respShard)
}

//	func (kv *ShardKV) isChangingNoneLock() bool {
//		kv.migrateNeed.cond.L.Lock()
//		defer kv.migrateNeed.cond.L.Unlock()
//		return kv.migrateNeed.needGetRPCNum != 0 || kv.migrateNeed.needSendRPCNum != 0
//	}
func (kv *ShardKV) applyCommand(op Op) string {
	DPrintf("[ssc (%v, %v)] success apply op = %v\n", kv.gid, kv.me, op)
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
func (opChan *OpChan) finish(opRes OpRes, val string) {
	opChan.cond.L.Lock()
	opChan.opRes = opRes
	opChan.val = val
	opChan.cond.Broadcast()
	opChan.cond.L.Unlock()
}
func (kv *ShardKV) deletePreviousQueryRes(cid ClientId) {
	ccid := ClientId{ClerkId: cid.ClerkId, NextCallIndex: cid.NextCallIndex - 1}
	for ccid.NextCallIndex >= 0 {
		if _, ok := kv.clientQueryResStore[ccid]; ok {
			delete(kv.clientQueryResStore, ccid)
			//break
		}
		ccid = ClientId{ClerkId: ccid.ClerkId, NextCallIndex: ccid.NextCallIndex - 1}
	}
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
			op := msg.Command.(Op)
			DPrintf("[ssc (%v,%v)] get applier msg, type = %v [0: AppendOP, 1: PutOp, 2: GetLog, 3: NoneOp, 4: CfgChgLog, 5: GoFlightLog, 6: GetFlightLog]\n", kv.gid, kv.me, op.Type)
			if op.Type == PutOp || op.Type == AppendOp || op.Type == GetLog {
				opChan, ok := kv.cacheTable.tryGetOpChan(op.Cid)
				kv.deletePreviousQueryRes(op.Cid)
				if _, okk := kv.clientLastCallIndex[op.Cid.ClerkId]; okk && op.Cid.NextCallIndex <= kv.clientLastCallIndex[op.Cid.ClerkId] {
					if ok {
						//DPrintf("in thisssssssssssssssssssssssssssssssssss!\n")
						opChan.ok(kv.clientQueryResStore[op.Cid])
					}
					kv.lastApplied = msg.CommandIndex
					kv.mu.Unlock()
					continue
				}
				if !kv.respShard[key2shard(op.Key)] {
					// TODO: solve the result
					DPrintf("[ssc (%v, %v)] try to append/put/get kv, but this group is not resp for this shard %v\n", kv.gid, kv.me, key2shard(op.Key))
					if ok {
						opChan.finish(OpWrongGroup, "")
					}
				} else {
					val := kv.applyCommand(op)
					if ok {
						opChan.ok(val)
					}
					if op.Type == GetLog {
						kv.clientQueryResStore[op.Cid] = val
					}
					kv.clientLastCallIndex[op.Cid.ClerkId] = op.Cid.NextCallIndex
				}
				//kv.clientLastCallIndex[op.Cid.ClerkId] = op.Cid.NextCallIndex
			}
			if op.Type == CfgChgLog {
				// 初始化
				DPrintf("[ssc (%v, %v)] try to commit new Config = %v\n", kv.gid, kv.me, op.Cfg)
				if op.Cfg.Num > kv.nextCfgNum {
					DPrintf("[ssc (%v, %v) ]op.cfg.num = %v, kv.nextCfgNum = %v\n", kv.gid, kv.me, op.Cfg.Num, kv.nextCfgNum)
					panic("op.Cfg.Num > kv.nextCfgNum")
				}
				cfgLogOpChan, ok := kv.cacheTable.tryGetOpChan(op.Cid)

				if kv.migratingToCfgNum == -1 && kv.nextCfgNum == op.Cfg.Num {
					kv.migratingToCfgNum = kv.nextCfgNum
					kv.newCfg = op.Cfg
					kv.migrateInfo = kv.makeMigrateInfoNoneLock(op.Cfg)
					kv.solveZeroMigrate()
					if kv.migrateInfo.mgInfoFinished() {
						kv.freshNewCfgNoneLock(kv.newCfg)
					}
					kv.checkStateMachine()
				}
				if kv.nextCfgNum > op.Cfg.Num {
					if ok {
						// too old 是可以被认为造就已经被commit的，可以直接认为也是成功。
						cfgLogOpChan.finish(OpCfgTooOld, "")
					}
				} else {
					if ok {
						cfgLogOpChan.ok("")
					}
				}
			}
			if op.Type == GetFlightLog {
				kv.onGetFlightLog(op)
			}
			if op.Type == GoFlightLog {

				kv.onGoFlightLog(op)
			}
			if op.Type == CfgFlightFinished {
				DPrintf("rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr\n")
				kv.onFlightFinishedNoneLock(op.Cfg.Num)
			}
			kv.lastApplied = msg.CommandIndex
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
func (kv *ShardKV) solveZeroMigrate() {
	for k, _ := range kv.migrateInfo.WaitGetFinished {
		if k == 0 {
			//DPrintf("[scc (%v, %v)]migrate from ZERO node, with shards = %v\n", kv.gid, kv.me)
			kv.migrateInfo.WaitGetFinished[k] = true
			for _, shard := range kv.migrateInfo.WaitGetShardInfo[k] {
				kv.respShard[shard] = true
			}
			break
		}
	}
	for k, v := range kv.migrateInfo.FlightShardInfo {
		if k == 0 {
			kv.migrateInfo.FlightFinished[k] = true
			toClear := []string{}
			for kk, _ := range kv.stateMachine {
				if intExitInArray(key2shard(kk), v) {
					toClear = append(toClear, kk)
				}
			}
			for _, shard := range v {
				kv.respShard[shard] = false
			}
			for _, key := range toClear {
				delete(kv.stateMachine, key)
			}
			break
		}
	}
}
func (kv *ShardKV) onGoFlightLog(op Op) {
	if op.Type != GoFlightLog {
		panic("type is not GoFlightLog")
	}
	GoFlightLogOpChan, ok := kv.migrateDupTbl[op.Mid]
	if op.Mid.CfgNum > kv.nextCfgNum {
		panic("op.Mid.cfgNum > kv.nextCfgNum")
	}
	if kv.migratingToCfgNum == -1 && op.Mid.CfgNum == kv.nextCfgNum {
		panic("kv.migratingToCfgNum == -1 && op.Mid.cfgNum == kv.nextCfgNum")
	}
	if kv.migratingToCfgNum == op.Mid.CfgNum {
		res, okk := kv.migrateInfo.FlightFinished[op.Gid]
		if !okk {
			panic("!okk")
		}
		if res == false {
			// ready to send request
			sm := make(map[string]string)
			for k, v := range kv.stateMachine {
				sm[k] = v
			}
			cli := make(map[int]int)
			for k, v := range kv.clientLastCallIndex {
				cli[k] = v
			}
			cqrs := make(map[ClientId]string)
			for k, v := range kv.clientQueryResStore {
				cqrs[k] = v
			}
			args := MigrateArgs{
				Cfg:                 kv.newCfg,
				KV:                  sm,
				Cid:                 ClientId{ClerkId: kv.gid, NextCallIndex: kv.newCfg.Num},
				ClientLastCallIndex: cli,
				ClientQueryResStore: cqrs,
			}
			peers, okkk := kv.newCfg.Groups[op.Gid]
			kv.mu.Unlock()
			//success := false
			if !okkk {
				panic("group does not exist")
			}
			for i := 0; i < len(peers); i = (i + 1) % len(peers) {
				reply := MigrateReply{}
				peer := kv.make_end(peers[i])
				DPrintf("[ssc (%v, %v)] try to connect to %v\n", kv.gid, kv.me, peers[i])
				ress := peer.Call("ShardKV.Migrate", &args, &reply)
				if ress == false || reply.Err == ErrWrongLeader || reply.Err == ErrTooNew {
					if i == len(peers)-1 {
						time.Sleep(time.Millisecond * 100)
					}
					DPrintf("[ssc (%v, %v)] Migrate fail, try again! with err = %v\n", kv.gid, kv.me, reply.Err)
					continue
				}
				//if reply.Err == ErrTooNew {
				//	break
				//}
				if reply.Err == OK || reply.Err == ErrTooOld {
					//success = true
					break
				}
			}
			kv.mu.Lock()
			//if !success {
			//	return
			//}
			kv.migrateInfo.FlightFinished[op.Gid] = true
			// we can clear
			// TODO: challenge1, garbage clear
			kv.clearGarbageKVNoneLock(op.Gid)
			for _, shard := range kv.migrateInfo.FlightShardInfo[op.Gid] {
				kv.respShard[shard] = false
			}
			kv.checkStateMachine()
			if kv.migrateInfo.mgInfoFinished() {
				kv.freshNewCfgNoneLock(kv.newCfg)
			}
		}
		if ok {
			GoFlightLogOpChan.ok("")
		}
	}
	if kv.migratingToCfgNum == -1 && op.Mid.CfgNum < kv.nextCfgNum {
		// must dup, just success
		if ok {
			GoFlightLogOpChan.ok("")
		}
	}
	if op.Mid.CfgNum > kv.nextCfgNum {
		if ok {
			GoFlightLogOpChan.finish(OpFlightTooNew, "")
		}
	}
}
func (kv *ShardKV) displayDetailedInfo() {
	kv.mu.Lock()
	keyNum := len(kv.stateMachine)
	stateMachineSize := 0
	for k, v := range kv.stateMachine {
		stateMachineSize += len(k)
		stateMachineSize += len(v)
	}
	cacheTblSize := len(kv.cacheTable.dupTable)
	migTblSize := len(kv.migrateDupTbl)
	lastQueryKeyNum := len(kv.clientQueryResStore)
	lastQuerySolveSize := 0
	for k, v := range kv.clientQueryResStore {
		DPrintf("[ssc (%v, %v)] lastQueryKey = %v\n", kv.gid, kv.me, k)
		lastQuerySolveSize += len(v)
	}
	DPrintf("[ssc (%v, %v)]sc_keyNum = %v, sc_size = %v, cacheTblSize = %v, migTblSize = %v, lastQueryKeyNum = %v, lastQuerySolveSize = %v\n", kv.gid, kv.me, keyNum, stateMachineSize, cacheTblSize, migTblSize, lastQueryKeyNum, lastQuerySolveSize)
	kv.mu.Unlock()
}
func (kv *ShardKV) getKeyNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.stateMachine)
}
func (kv *ShardKV) clearGarbageKVNoneLock(gid int) {
	toClear := []string{}
	for k, _ := range kv.stateMachine {
		if intExitInArray(key2shard(k), kv.migrateInfo.FlightShardInfo[gid]) {
			toClear = append(toClear, k)
		}
	}
	for _, k := range toClear {
		DPrintf("[ssc (%v, %v)] delete k %v\n", kv.gid, kv.me, k)
		delete(kv.stateMachine, k)
	}
}
func (kv *ShardKV) onGetFlightLog(op Op) {
	// 从其他peer那边获取了flight信息
	// 1. 本节点已经进入相同的模式，进入接受模式
	// 2. 本节点尚未从ctler获取最新的cfg，两种选择：1. 直接拒绝，等待本机循环获取。 2. 进行初始化，等待本机循环获取
	// 这里选择进行初始化，减少网络请求发送的次数。
	//for clerkId, lastCallIndex := range op.CLI {
	//	res, ok := kv.clientLastCallIndex[clerkId]
	//	if !ok || lastCallIndex > res {
	//		kv.clientLastCallIndex[clerkId] = lastCallIndex
	//	}
	//}
	//for clientId, res := range op.CQRS {
	//	kv.clientQueryResStore[clientId] = res
	//}
	if op.Cfg.Num == kv.nextCfgNum && kv.migratingToCfgNum == -1 {
		// 2. 还没进入模式，进行操作。
		kv.migratingToCfgNum = op.Cfg.Num
		kv.newCfg = op.Cfg
		kv.migrateInfo = kv.makeMigrateInfoNoneLock(op.Cfg)
		kv.solveZeroMigrate()
		if kv.migrateInfo.mgInfoFinished() {
			kv.freshNewCfgNoneLock(kv.newCfg)
			panic("can not become finished")
		}
	}
	getFlightLogOpChan, ok := kv.cacheTable.tryGetOpChan(op.Cid)
	if op.Cfg.Num == kv.migratingToCfgNum {
		// 1. 已经进入了，开始尝试进行匹配
		res, okk := kv.migrateInfo.WaitGetFinished[op.Gid]
		DPrintf("[scc (%v,%v)] get a flight = %v from peer %v\n", kv.gid, kv.me, op, op.Gid)
		if !okk {
			//DPrintf("[scc (%v,%v)] get a flight = %v from peer %v\n", kv.gid, kv.me, op, op.Gid)
			panic("should not get migrate from this peer, invalid")
		}
		if res == false {
			// begin get into state machine
			for k, v := range op.KV {
				if intExitInArray(key2shard(k), kv.migrateInfo.WaitGetShardInfo[op.Gid]) {
					kv.stateMachine[k] = v
				}
			}
			kv.migrateInfo.WaitGetFinished[op.Gid] = true
			for _, shard := range kv.migrateInfo.WaitGetShardInfo[op.Gid] {
				kv.respShard[shard] = true
			}
			for clerkId, lastCallIndex := range op.CLI {
				res, ok := kv.clientLastCallIndex[clerkId]
				if !ok || lastCallIndex > res {
					kv.clientLastCallIndex[clerkId] = lastCallIndex
				}
			}
			for clientId, res := range op.CQRS {
				kv.clientQueryResStore[clientId] = res
			}
			if kv.migrateInfo.mgInfoFinished() {
				kv.freshNewCfgNoneLock(kv.newCfg)
				kv.checkStateMachine()
			}
		}
		if ok {
			getFlightLogOpChan.ok("")
		}
	} else if op.Cfg.Num > kv.nextCfgNum {
		// too new
		DPrintf("[scc (%v,%v)] too new get a flight = %v from peer %v\n", kv.gid, kv.me, op, op.Gid)
		DPrintf("[ssc (%v,%v)] mgInfo = %v\n", kv.gid, kv.me, kv.migrateInfo)
		DPrintf("[ssc (%v,%v)] now cfgNum = %v, migratingCfgNum = %v\n", kv.gid, kv.me, kv.cfg.Num, kv.migratingToCfgNum)
		if ok {
			getFlightLogOpChan.finish(OpFlightTooNew, "")
		}
	} else {
		// too old
		DPrintf("[scc (%v,%v)] too old get a flight = %v from peer %v\n", kv.gid, kv.me, op, op.Gid)
		DPrintf("[ssc (%v,%v)] mgInfo = %v\n", kv.gid, kv.me, kv.migrateInfo)
		DPrintf("[ssc (%v,%v)] now cfgNum = %v, migratingCfgNum = %v\n", kv.gid, kv.me, kv.cfg.Num, kv.migratingToCfgNum)
		if ok {
			getFlightLogOpChan.finish(OpFlightTooOld, "")
		}
	}
}

func (kv *ShardKV) onFlightFinishedNoneLock(cfgNum int) {
	flightFinishedOpChan, ok := kv.flightFinishedOpChan[cfgNum]
	if cfgNum <= kv.cfg.Num {
		if ok {
			flightFinishedOpChan.finish(OpCfgTooOld, "")
		}
		return
	}
	if cfgNum == kv.migratingToCfgNum {
		kv.migrateInfo.FlightAllFinished = true
		for k, v := range kv.migrateInfo.FlightFinished {
			if !v {
				kv.migrateInfo.FlightFinished[k] = true
				kv.clearGarbageKVNoneLock(k)
				for _, shard := range kv.migrateInfo.FlightShardInfo[k] {
					kv.respShard[shard] = false
				}
				kv.checkStateMachine()
			}
		}
		if kv.migrateInfo.mgInfoFinished() {
			kv.nextCfgNum = cfgNum + 1
			kv.migratingToCfgNum = -1
			kv.cfg = kv.newCfg
		}
		if ok {
			flightFinishedOpChan.ok("")
		}
		return
	}
	DPrintf("[ssc (%v, %v)] flightFinishedCfgNum = %v, and now migratingToCfgNum == %v, now cfg num = %v\n", kv.gid, kv.me, cfgNum, kv.migratingToCfgNum, kv.cfg.Num)

}

// func (kv *ShardKV) onTryToCommitFlights
func (kv *ShardKV) makeMigrateInfoNoneLock(newCfg shardctrler.Config) MigrateInfo {
	oldCfg := kv.cfg
	if newCfg.Num != oldCfg.Num+1 {
		DPrintf("[ssc (%v,%v)] newCfg.Num = %v, oldCfg.Num = %v\n", kv.gid, kv.me, newCfg.Num, oldCfg.Num)
		panic("should increase one by one")
	}
	aims := make(map[int]bool)
	gets := make(map[int]bool)
	getsInfo := make(map[int][]int)
	aimsInfo := make(map[int][]int)
	FlightAims := []int{}
	for idx, val := range oldCfg.Shards {
		if newCfg.Shards[idx] == val {
			continue
		}
		if val == kv.gid {
			if _, ok := aims[newCfg.Shards[idx]]; !ok {
				FlightAims = append(FlightAims, newCfg.Shards[idx])
				aimsInfo[newCfg.Shards[idx]] = []int{}
			}
			aimsInfo[newCfg.Shards[idx]] = append(aimsInfo[newCfg.Shards[idx]], idx)
			aims[newCfg.Shards[idx]] = false
		}
		if newCfg.Shards[idx] == kv.gid {
			if _, ok := gets[val]; !ok {
				getsInfo[val] = []int{}
			}
			getsInfo[val] = append(getsInfo[val], idx)
			gets[val] = false
		}
	}
	return MigrateInfo{
		FlightFinished:    aims,
		FlightShardInfo:   aimsInfo,
		WaitGetFinished:   gets,
		WaitGetShardInfo:  getsInfo,
		FlightAims:        FlightAims,
		FlightAllFinished: false,
		LastFlightSeqNum:  0,
	}
}
func (kv *ShardKV) makeMigrateInfo(newCfg shardctrler.Config) (MigrateInfo, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldCfg := kv.cfg
	if newCfg.Num != oldCfg.Num+1 {
		return MigrateInfo{}, false
	}
	aims := make(map[int]bool)
	gets := make(map[int]bool)
	getsInfo := make(map[int][]int)
	aimsInfo := make(map[int][]int)
	FlightAims := []int{}
	for idx, val := range oldCfg.Shards {
		if newCfg.Shards[idx] == val {
			continue
		}
		if val == kv.gid {
			if _, ok := aims[newCfg.Shards[idx]]; !ok {
				FlightAims = append(FlightAims, newCfg.Shards[idx])
				aimsInfo[newCfg.Shards[idx]] = []int{}
			}
			aimsInfo[newCfg.Shards[idx]] = append(aimsInfo[newCfg.Shards[idx]], idx)
			aims[newCfg.Shards[idx]] = false
		}
		if newCfg.Shards[idx] == kv.gid {
			if _, ok := gets[val]; !ok {
				getsInfo[val] = []int{}
			}
			getsInfo[val] = append(getsInfo[val], idx)
			gets[val] = false
		}
	}
	return MigrateInfo{
		FlightFinished:    aims,
		FlightShardInfo:   aimsInfo,
		WaitGetFinished:   gets,
		WaitGetShardInfo:  getsInfo,
		FlightAllFinished: false,
		FlightAims:        FlightAims,
		LastFlightSeqNum:  0,
	}, true
}
func (kv *ShardKV) putCfgChgLog(newCfg shardctrler.Config) Err {
	kv.mu.Lock()
	if newCfg.Num != kv.nextCfgNum {
		kv.mu.Unlock()
		return ErrTooOld
	}
	if kv.nextCfgNum > newCfg.Num {
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
		opChan.cond.WaitWithTimeout(RES_TIMEOUT)
	}
	defer opChan.cond.L.Unlock()
	if opChan.opRes == OpUnknown {
		return ErrCommitFail
	} else if opChan.opRes == OpCfgTooOld {
		return ErrTooOld
	} else {
		return OK
	}
}
func (kv *ShardKV) putFlight(cfgNum int, seqNum int, aim int) Err {
	kv.mu.Lock()
	mid := MigrateId{CfgNum: cfgNum, SeqNum: seqNum}
	op := Op{
		Type: GoFlightLog,
		Mid:  mid,
		Gid:  aim,
	}
	if kv.nextCfgNum > cfgNum {
		kv.mu.Unlock()
		return ErrTooOld
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
		opChan.cond.WaitWithTimeout(RES_TIMEOUT)
	}
	defer opChan.cond.L.Unlock()
	if opChan.opRes == OpUnknown {
		return ErrCommitFail
	} else if opChan.opRes == OpFlightTooOld {
		return OK
	} else {
		return OK
	}
}

func (kv *ShardKV) tryToCommitChange(newCfg shardctrler.Config) {
	cfgChgLogOk := false
	for !kv.killed() {
		res := kv.putCfgChgLog(newCfg)
		if res == OK {
			cfgChgLogOk = true
			break
		}
		if res == ErrWrongLeader || res == ErrTooOld {
			break
		}
	}
	if !cfgChgLogOk {
		return
	}
	DPrintf("[ssc %v] success put a CfgChgLog into Raft\n", kv.me)
	migrateInfo, ok := kv.makeMigrateInfo(newCfg)
	if !ok {
		return
	}
	//all := true
	for idx, aim := range migrateInfo.FlightAims {
		if aim == 0 {
			continue
		}
		flightOk := false
		for !kv.killed() {
			res := kv.putFlight(newCfg.Num, idx, aim)
			if res == OK {
				flightOk = true
				break
			}
			if res == ErrTooOld || res == ErrWrongLeader {
				break
			}
			if res == ErrCommitFail {
				time.Sleep(time.Millisecond * 50)
			}
		}
		if !flightOk {
			//all = false
			break
		}
		DPrintf("[ssc %v] success put a flight aim = %v into raft log\n", kv.me, aim)
	}
	//if all {
	//	for {
	//		kv.mu.Lock()
	//		op := Op{
	//			Type: CfgFlightFinished,
	//			Cfg:  newCfg,
	//		}
	//		index, _, isLeader := kv.rf.Start(op)
	//		if !isLeader {
	//			kv.mu.Unlock()
	//			return
	//		}
	//		opChan := OpChan{
	//			cond:  utils.NewCond(&sync.Mutex{}),
	//			index: index,
	//			opRes: OpUnknown,
	//			val:   "",
	//		}
	//		kv.flightFinishedOpChan[newCfg.Num] = &opChan
	//		kv.mu.Unlock()
	//		opChan.cond.L.Lock()
	//		if opChan.opRes == OpUnknown {
	//			opChan.cond.WaitWithTimeout(RES_TIMEOUT)
	//		}
	//		if opChan.opRes == OpCfgTooOld || opChan.opRes == OpResOk {
	//			opChan.cond.L.Unlock()
	//			return
	//		}
	//		opChan.cond.L.Unlock()
	//	}
	//}
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
		go kv.tryToCommitChange(cfg)
		//DPrintf("[ssc (%v, %v)] get an cfg from ctler = %v\n", kv.gid, kv.me, cfg)
		//cfgChgLogOk := false
		//for !kv.killed() {
		//	res := kv.putCfgChgLog(cfg)
		//	if res == OK {
		//		cfgChgLogOk = true
		//		break
		//	}
		//	if res == ErrWrongLeader || res == ErrTooOld {
		//		break
		//	}
		//}
		//if !cfgChgLogOk {
		//	continue
		//}
		//go kv.tryToCommitChange(cfg)
		//DPrintf("[ssc %v] success put a CfgChgLog into Raft\n", kv.me)
		//migrateInfo, ok := kv.makeMigrateInfo(cfg)
		//if !ok {
		//	continue
		//}
		//all := true
		//for idx, aim := range migrateInfo.FlightAims {
		//	flightOk := false
		//	for !kv.killed() {
		//		res := kv.putFlight(cfg.Num, idx, aim)
		//		if res == OK {
		//			flightOk = true
		//			break
		//		}
		//		if res == ErrTooOld || res == ErrWrongLeader {
		//			break
		//		}
		//		if res == ErrCommitFail {
		//			time.Sleep(time.Millisecond * 50)
		//		}
		//	}
		//	if !flightOk {
		//		all = false
		//		break
		//	}
		//	DPrintf("[ssc %v] success put a flight aim = %v into raft log\n", kv.me, aim)
		//}
		//if all {
		//	for {
		//		kv.mu.Lock()
		//
		//		op := Op{
		//			Type: CfgFlightFinished,
		//			Cfg:  cfg,
		//		}
		//		index, _, isLeader := kv.rf.Start(op)
		//		if !isLeader {
		//			kv.mu.Unlock()
		//			break
		//		}
		//		opChan := OpChan{
		//			cond:  utils.NewCond(&sync.Mutex{}),
		//			index: index,
		//			opRes: OpUnknown,
		//			val:   "",
		//		}
		//		kv.flightFinishedOpChan[cfg.Num] = &opChan
		//		kv.mu.Unlock()
		//		opChan.cond.L.Lock()
		//		if opChan.opRes == OpUnknown {
		//			opChan.cond.WaitWithTimeout(RES_TIMEOUT)
		//		}
		//		if opChan.opRes == OpResOk || opChan.opRes == OpCfgTooOld {
		//			opChan.cond.L.Unlock()
		//			break
		//		}
		//		opChan.cond.L.Unlock()
		//	}
		//}
	}
}
func (kv *ShardKV) makeSnapshotNoneLock(snapshotIndex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshotIndex)
	e.Encode(kv.clientLastCallIndex)
	e.Encode(kv.stateMachine)
	e.Encode(kv.cfg)
	e.Encode(kv.newCfg)
	e.Encode(kv.nextCfgNum)
	e.Encode(kv.respShard)
	e.Encode(kv.migratingToCfgNum)
	e.Encode(kv.migrateInfo)
	e.Encode(kv.clientQueryResStore)
	//e.Encode(kv.)
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
	var cfg shardctrler.Config
	var newCfg shardctrler.Config
	var nextCfgNum int
	var respShard map[int]bool
	var migratingToCfgNum int
	var migrateInfo MigrateInfo
	var clientQueryResStore map[ClientId]string
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&clientApplyInfo) != nil ||
		d.Decode(&stateMachine) != nil ||
		d.Decode(&cfg) != nil ||
		d.Decode(&newCfg) != nil ||
		d.Decode(&nextCfgNum) != nil ||
		d.Decode(&respShard) != nil ||
		d.Decode(&migratingToCfgNum) != nil ||
		d.Decode(&migrateInfo) != nil ||
		d.Decode(&clientQueryResStore) != nil {
		log.Fatalf("snapshot decode error")
	}
	if lastIncludedIndex < kv.lastApplied {
		return
	}
	kv.clientLastCallIndex = clientApplyInfo
	kv.snapshotLastIndex = lastIncludedIndex
	kv.lastApplied = lastIncludedIndex
	kv.stateMachine = stateMachine
	kv.cfg = cfg
	kv.newCfg = newCfg
	kv.nextCfgNum = nextCfgNum
	kv.respShard = respShard
	kv.migratingToCfgNum = migratingToCfgNum
	kv.migrateInfo = migrateInfo
	kv.clientQueryResStore = clientQueryResStore
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[scc (%v, %v)] try to get, args = %v\n", kv.gid, kv.me, args)
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
		opChan.cond.WaitWithTimeout(RES_TIMEOUT)
	}
	reply.Value = opChan.val
	if opChan.opRes == OpUnknown {
		DPrintf("[scc (%v, %v)] try to Get, but timeout, args = %v\n", kv.gid, kv.me, args)
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
	DPrintf("[scc (%v, %v)] try to put, args = %v\n", kv.gid, kv.me, args)
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
		opChan.cond.WaitWithTimeout(RES_TIMEOUT)
	}
	if opChan.opRes == OpUnknown {
		DPrintf("[scc (%v, %v)] try to putAppend, but timeout, args = %v\n", kv.gid, kv.me, args)
		reply.Err = ErrCommitFail
	} else if opChan.opRes == OpWrongGroup {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
	}
	opChan.cond.L.Unlock()
	return
}
func displayCostTime(startTime time.Time) {
	costTime := time.Now().UnixMicro() - startTime.UnixMicro()
	DPrintf("cost time = %v\n", costTime)
}
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	startTime := time.Now()
	kv.mu.Lock()
	if (kv.migratingToCfgNum == -1 && args.Cfg.Num <= kv.cfg.Num) || (kv.migratingToCfgNum != -1 && args.Cfg.Num < kv.migratingToCfgNum) {
		reply.Err = ErrTooOld
		kv.mu.Unlock()
		return
	}
	cid := ClientId{ClerkId: -args.Cid.ClerkId, NextCallIndex: args.Cid.NextCallIndex}
	op := Op{
		Type: GetFlightLog,
		KV:   args.KV,
		Cid:  cid, // [gid]cfgNum
		Cfg:  args.Cfg,
		Gid:  args.Cid.ClerkId,
		CLI:  args.ClientLastCallIndex,
		CQRS: args.ClientQueryResStore,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("[ssc (%v, %v)] is not leader, return wrong leader!\n", kv.gid, kv.me)
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
		opChan.cond.WaitWithTimeout(RES_TIMEOUT)
	}
	if opChan.opRes == OpUnknown {
		reply.Err = ErrCommitFail
	} else if opChan.opRes == OpFlightTooOld {
		reply.Err = ErrTooOld
	} else if opChan.opRes == OpFlightTooNew {
		reply.Err = ErrTooNew
	} else {
		reply.Err = OK
	}
	opChan.cond.L.Unlock()
	displayCostTime(startTime)
	//costTime := time.Now().UnixMicro() - startTime.UnixMicro()
	//DPrintf("migrate cost %v\n", costTime)
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
func (kv *ShardKV) checkStateMachine() {
	for k, _ := range kv.stateMachine {
		if !kv.respShard[key2shard(k)] {
			panic("have key that does not response for")
		}
	}
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
	labgob.Register(MigrateInfo{})
	labgob.Register(MigrateId{})
	kv := new(ShardKV)
	kv.me = me
	kv.dead = 0
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
	//kv.migrateNextCallIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.nextCfgNum = 1
	kv.respShard = make(map[int]bool)
	kv.stateMachine = make(map[string]string)
	kv.persister = persister
	kv.cacheTable = CacheTable{
		dupTable: make(map[ClientId]*OpChan),
		IndexId:  map[int]ClientId{},
	}
	//kv.cfgOpChan = make(map[int]*OpChan)
	kv.migrateDupTbl = map[MigrateId]*OpChan{}
	kv.clientLastCallIndex = map[int]int{}
	kv.lastApplied = 0
	kv.clientQueryResStore = map[ClientId]string{}
	kv.flightFinishedOpChan = map[int]*OpChan{}
	kv.installSnapshotNoneLock(persister.ReadSnapshot())
	go kv.configDetection()
	go kv.applier(kv.applyCh)
	return kv
}
