package shardctrler

import (
	"6.5840/raft"
	"6.5840/utils"
	"math"
	"sync/atomic"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "github.com/google/btree"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// Your data here.

	configs []Config // indexed by config num, state machine
	cfgNum  int
	//gInfo            btree.BTree
	gidMapShardNum   map[int]int
	clientLastIndex  []int
	lastAppliedIndex int
	dupTable         CacheTable
}

var debugMode bool = true

type GInfo struct {
	gid      int
	shardNum int
}

func (gInfo GInfo) Less(item btree.Item) bool {
	than := item.(GInfo)
	if gInfo.shardNum == than.shardNum {
		return gInfo.gid < than.gid
	}
	return gInfo.shardNum < than.shardNum
}

type OpType int

const (
	JoinT  OpType = 0
	LeaveT OpType = 1
	MoveT  OpType = 2
	QueryT OpType = 3
)

type ClientId struct {
	ClerkId       int
	NextCallIndex int
}
type OpRes int

const (
	OpResOk   OpRes = 0
	OpUnknown OpRes = 1
)

type OpChan struct {
	cond  *utils.MCond
	opRes OpRes
	index int
	res   Config
}

func (cd *ClientId) previousCallIndex() ClientId {
	return ClientId{ClerkId: cd.ClerkId, NextCallIndex: cd.NextCallIndex - 1}
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

type Op struct {
	// Your data here.
	Type OpType
	Args interface{}
	Cid  ClientId
}
type ShardChangeLog struct {
	FromGid     int
	FromServers []string
	ToGid       int
	ToServers   []string
	ShardNum    int
}

func (sc *ShardCtrler) balance() []ShardChangeLog {
	chgs := []ShardChangeLog{}
	var maxGid int = math.MaxInt
	var maxV int = -1
	var minGid int = -1
	var minV int = math.MaxInt
	for {
		find := false
		// init
		for gid, sd := range sc.gidMapShardNum {
			maxGid = gid
			maxV = sd
			minGid = gid
			minV = sd
			find = true
			break
		}
		// find max and int
		for gid, sd := range sc.gidMapShardNum {
			if sd >= maxV && gid < maxGid {
				maxGid = gid
				maxV = sd
			}
			if sd <= minV && gid > minGid {
				minGid = gid
				minV = sd
			}
		}
		if !find || maxV-minV <= 1 {
			break
		}
		sc.gidMapShardNum[maxGid] -= 1
		sc.gidMapShardNum[minGid] += 1
		if sc.gidMapShardNum[minGid] > NShards {
			panic("gidMapShardNum[minGid] can not above NShard")
		}
		if sc.gidMapShardNum[maxGid] < 0 {
			panic("gidMapShardNum[maxGid] can not below 0")
		}
		for idx, gid := range sc.configs[sc.cfgNum].Shards {
			if gid == maxGid {
				sc.configs[sc.cfgNum].Shards[idx] = minGid
				chgs = append(chgs, ShardChangeLog{
					FromGid:     maxGid,
					ToGid:       minGid,
					ShardNum:    idx,
					FromServers: sc.configs[sc.cfgNum].Groups[maxGid],
					ToServers:   sc.configs[sc.cfgNum].Groups[minGid],
				})
				break
			}
		}
	}
	return chgs
}
func intExitInArray(ele int, arr []int) bool {
	for el := range arr {
		if el == ele {
			return true
		}
	}
	return false
}
func (sc *ShardCtrler) reassign(gidRs []int, shards []int) []ShardChangeLog {
	// 贪心，每次选出最小值，分给他。
	chgs := []ShardChangeLog{}
	var minV int
	var minGid int
	for shard := range shards {
		// step1: find min
		find := false
		for gid, sd := range sc.gidMapShardNum {
			if intExitInArray(gid, gidRs) {
				continue
			}
			minV = sd
			minGid = gid
			find = true
		}
		if !find {
			break
		}
		for gid, sd := range sc.gidMapShardNum {
			if sd <= minV && gid > minGid && !intExitInArray(gid, gidRs) {
				minV = sd
				minGid = gid
			}
		}
		sc.gidMapShardNum[minGid] += 1
		if sc.gidMapShardNum[minGid] > NShards {
			panic("gidMapShardNum[targetGid] can not above NShard")
		}
		// step2: replace the target shard to chose gid
		gidR := sc.configs[sc.cfgNum].Shards[shard]
		sc.configs[sc.cfgNum].Shards[shard] = minGid
		chgs = append(chgs, ShardChangeLog{
			FromGid:     gidR,
			FromServers: sc.configs[sc.cfgNum].Groups[gidR],
			ToGid:       minGid,
			ToServers:   sc.configs[sc.cfgNum].Groups[minGid],
			ShardNum:    shard,
		})
		sc.gidMapShardNum[gidR] -= 1
		if sc.gidMapShardNum[gidR] < 0 {
			panic("gidMapShardNum[gidR] can not below zero")
		}
	}
	return chgs
}
func (sc *ShardCtrler) cfgNumPlusOneAndCopy() {
	sc.cfgNum += 1
	sc.configs[sc.cfgNum] = Config{
		Num:    sc.cfgNum,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	for i := 0; i < NShards; i++ {
		sc.configs[sc.cfgNum].Shards[i] = sc.configs[sc.cfgNum-1].Shards[i]
	}
	for gid, servers := range sc.configs[sc.cfgNum-1].Groups {
		sc.configs[sc.cfgNum].Groups[gid] = servers
	}
}
func (sc *ShardCtrler) gidsToShards(gids []int) []int {
	res := []int{}
	for gidr := range gids {
		for shard, gid := range sc.configs[sc.cfgNum].Shards {
			if gid == gidr {
				res = append(res, shard)
			}
		}
	}
	return res
}
func (sc *ShardCtrler) solveChanges(chgs []ShardChangeLog, balance bool) {
	// TODO:
}
func (sc *ShardCtrler) checkSum() {
	if !debugMode {
		return
	}
	for gidt, shardNum := range sc.gidMapShardNum {
		realShardNum := 0
		for _, gid := range sc.configs[sc.cfgNum].Shards {
			if gid == gidt {
				realShardNum += 1
			}
		}
		if realShardNum != shardNum {
			panic("check sum failed")
		}
	}
}
func (sc *ShardCtrler) applyOp(op Op) {
	chgs := []ShardChangeLog{}
	switch op.Type {
	case JoinT:
		{
			args := op.Args.(JoinArgs)
			sc.cfgNumPlusOneAndCopy()
			for gid, server := range args.Servers {
				sc.configs[sc.cfgNum].Groups[gid] = server
				sc.gidMapShardNum[gid] = 0
			}
			chgs = append(chgs, sc.reassign([]int{-1}, sc.gidsToShards([]int{-1}))...)
			chgs = append(chgs, sc.balance()...)
			sc.solveChanges(chgs, true)
		}
	case LeaveT:
		{
			args := op.Args.(LeaveArgs)
			sc.cfgNumPlusOneAndCopy()
			chgs = sc.reassign(args.GIDs, sc.gidsToShards(args.GIDs))
			for gid := range args.GIDs {
				delete(sc.gidMapShardNum, gid)
				delete(sc.configs[sc.cfgNum].Groups, gid)
			}
			// 查询分配失败的shard, 将其设置野shard
			for shard, gid := range sc.configs[sc.cfgNum].Shards {
				if intExitInArray(gid, args.GIDs) {
					sc.configs[sc.cfgNum].Shards[shard] = -1
				}
			}
			sc.solveChanges(chgs, false)
		}
	case MoveT:
		{
			args := op.Args.(MoveArgs)
			sc.cfgNumPlusOneAndCopy()
			fromGid := sc.configs[sc.cfgNum].Shards[args.Shard]
			toGid := args.GID
			sc.gidMapShardNum[fromGid] -= 1
			sc.gidMapShardNum[toGid] += 1
			if sc.gidMapShardNum[fromGid] < 0 {
				panic("gidMapShardNum[fromGid] can not below zero")
			}
			if sc.gidMapShardNum[toGid] > NShards {
				panic("gidMapShardNum[toGid] can not above NShards")
			}
			chgs = append(chgs, ShardChangeLog{
				FromGid:     fromGid,
				FromServers: sc.configs[sc.cfgNum].Groups[fromGid],
				ToGid:       toGid,
				ToServers:   sc.configs[sc.cfgNum].Groups[toGid],
				ShardNum:    args.Shard,
			})
			sc.solveChanges(chgs, true)
		}
	case QueryT:
		{

		}
	}
}
func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if sc.Killed() {
			return
		}
		if msg.SnapshotValid {
			panic("shardCtl do not support snapshot!")
		}
		sc.mu.Lock()
		if msg.CommandIndex != sc.lastAppliedIndex+1 {
			sc.mu.Unlock()
			continue
		}
		sc.lastAppliedIndex = msg.CommandIndex
		op := msg.Command.(Op)
		cid := op.Cid
		opChan, ok := sc.dupTable.tryGetOpChan(cid)
		if cid.NextCallIndex <= sc.clientLastIndex[cid.ClerkId] {
			sc.mu.Unlock()
			continue
		}
		if ok {
			opChan.cond.L.Lock()
			opChan.opRes = OpResOk
			opChan.cond.Broadcast()
			opChan.cond.L.Unlock()
		}
		sc.clientLastIndex[cid.ClerkId] = cid.NextCallIndex
		sc.mu.Unlock()
	}
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
	// Your code here, if desired.
}
func (sc *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	atomic.StoreInt32(&sc.dead, 0)
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
