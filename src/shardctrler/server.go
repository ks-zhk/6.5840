package shardctrler

import (
	"6.5840/raft"
	"6.5840/utils"
	"log"
	"math"
	"sync/atomic"
	"time"
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
	clientLastIndex  map[int]int
	queryNumHistory  map[ClientId]int
	lastAppliedIndex int
	dupTable         CacheTable
}

const debugMode bool = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debugMode {
		log.Printf(format, a...)
	}
	return
}

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

func (sc *ShardCtrler) balance(expect []int) []ShardChangeLog {
	chgs := []ShardChangeLog{}
	var maxGid int = math.MaxInt
	var maxV int = -1
	var minGid int = -1
	var minV int = math.MaxInt
	for {
		find := false
		// init
		for gid, sd := range sc.gidMapShardNum {
			if intExitInArray(gid, expect) {
				continue
			}
			maxGid = gid
			maxV = sd
			minGid = gid
			minV = sd
			find = true
			break
		}
		// find max and int
		DPrintf("[sc %v] gidMapShardNum = %v\n", sc.me, sc.gidMapShardNum)
		for gid, sd := range sc.gidMapShardNum {
			if intExitInArray(gid, expect) {
				continue
			}
			if (sd == maxV && gid < maxGid) || sd > maxV {
				maxGid = gid
				maxV = sd
			}
			if (sd == minV && gid > minGid) || sd < minV {
				minGid = gid
				minV = sd
			}
		}
		DPrintf("[sc %v] find = %v, maxGid = %v, maxV = %v, minGid = %v, minV = %v\n", sc.me, find, maxGid, maxV, minGid, minV)
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
		sc.checkSum()
	}
	sc.checkBalance()
	return chgs
}
func intExitInArray(ele int, arr []int) bool {
	for _, el := range arr {
		if el == ele {
			return true
		}
	}
	return false
}
func (sc *ShardCtrler) reassign(gidRs []int, shards []int) []ShardChangeLog {
	// 贪心，每次选出最小值，分给他。
	DPrintf("[sc %v] ready to reassign, gidRs = %v, shards = %v\n", sc.me, gidRs, shards)
	chgs := []ShardChangeLog{}
	var minV int
	var minGid int
	for _, shard := range shards {
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
			if ((sd == minV && gid > minGid) || sd < minV) && !intExitInArray(gid, gidRs) {
				minV = sd
				minGid = gid
			}
		}
		DPrintf("[sc %v] in reassign, find = %v, minV = %v, minGid = %v\n", sc.me, find, minV, minGid)
		sc.gidMapShardNum[minGid] += 1
		if sc.gidMapShardNum[minGid] > NShards {
			panic("gidMapShardNum[targetGid] can not above NShard")
		}
		// step2: replace the target shard to chose gid
		gidR := sc.configs[sc.cfgNum].Shards[shard]
		sc.configs[sc.cfgNum].Shards[shard] = minGid
		//DPrintf("[sc %v] shard [%v] change to %v\n", sc.me, shards minGid)
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
		sc.checkSum()
	}
	DPrintf("[sc %v] after reassign, config = %v\n", sc.me, sc.configs[sc.cfgNum].Shards)
	//sc.checkBalance()
	return chgs
}
func (sc *ShardCtrler) cfgNumPlusOneAndCopy() {
	sc.cfgNum += 1
	sc.configs = append(sc.configs, Config{})
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
	//DPrintf("[sc %v] now configs's shards = %v\n", sc.me, sc.configs[sc.cfgNum].Shards)
	for _, gidr := range gids {
		for shard, gid := range sc.configs[sc.cfgNum].Shards {
			//DPrintf("[sc %v] gid = %v, gidr = %v\n", sc.me, gid, gidr)
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
			DPrintf("[sc %v] gidT = %v, realShardNum = %v, shardNum = %v\n", sc.me, gidt, realShardNum, shardNum)
			panic("check sum failed")
		}
	}
}
func (sc *ShardCtrler) checkBalance() {
	var minV int
	var maxV int
	find := false
	for gid, sd := range sc.gidMapShardNum {
		if gid == 0 {
			continue
		}
		minV = sd
		maxV = sd
		find = true
		break
	}
	if !find {
		return
	}
	for gid, sd := range sc.gidMapShardNum {
		if gid == 0 {
			continue
		}
		if sd > maxV {
			maxV = sd
		}
		if sd < minV {
			minV = sd
		}
		break
	}
	if maxV > minV+1 {
		DPrintf("[sc %v] sc.gidMapShardNum = %v, max = %v, min = %v\n", sc.me, sc.gidMapShardNum, maxV, minV)
		panic("check balance failed")
	}
}
func (sc *ShardCtrler) applyOp(op Op) Config {
	chgs := []ShardChangeLog{}
	defer sc.checkSum()
	switch op.Type {
	case JoinT:
		{
			DPrintf("[sc %v] prepare Join\n", sc.me)
			args := op.Args.(JoinArgs)
			sc.cfgNumPlusOneAndCopy()
			for gid, server := range args.Servers {
				sc.configs[sc.cfgNum].Groups[gid] = server
				sc.gidMapShardNum[gid] = 0
			}
			chgs = append(chgs, sc.reassign([]int{0}, sc.gidsToShards([]int{0}))...)
			chgs = append(chgs, sc.balance([]int{0})...)
			sc.checkSum()
			sc.solveChanges(chgs, true)
			log.Printf("[ctler %v] new cfg = %v\n", sc.me, sc.configs[sc.cfgNum])
		}
	case LeaveT:
		{
			args := op.Args.(LeaveArgs)
			args.GIDs = append(args.GIDs, 0)
			sc.cfgNumPlusOneAndCopy()
			DPrintf("[sc %v] try to leave %v, shards = %v\n", sc.me, args.GIDs, sc.gidsToShards(args.GIDs))
			chgs = sc.reassign(args.GIDs, sc.gidsToShards(args.GIDs))
			for _, gid := range args.GIDs {
				delete(sc.gidMapShardNum, gid)
				delete(sc.configs[sc.cfgNum].Groups, gid)
			}
			DPrintf("[sc %v] sc.gidMapShardNum = %v\n", sc.me, sc.gidMapShardNum)
			// 查询分配失败的shard, 将其设置野shard
			for shard, gid := range sc.configs[sc.cfgNum].Shards {
				if intExitInArray(gid, args.GIDs) {
					DPrintf("[sc %v] in fail gid = %v\n", sc.me, gid)
					sc.configs[sc.cfgNum].Shards[shard] = 0
					sc.gidMapShardNum[0] += 1
				}
			}
			sc.checkSum()
			sc.checkBalance()
			sc.solveChanges(chgs, false)
			log.Printf("[ctler %v] new cfg = %v\n", sc.me, sc.configs[sc.cfgNum])
		}
	case MoveT:
		{
			// no balance ops
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
			sc.configs[sc.cfgNum].Shards[args.Shard] = toGid
			chgs = append(chgs, ShardChangeLog{
				FromGid:     fromGid,
				FromServers: sc.configs[sc.cfgNum].Groups[fromGid],
				ToGid:       toGid,
				ToServers:   sc.configs[sc.cfgNum].Groups[toGid],
				ShardNum:    args.Shard,
			})
			sc.solveChanges(chgs, true)
			sc.checkSum()
			log.Printf("[ctler %v] new cfg = %v\n", sc.me, sc.configs[sc.cfgNum])
		}
	case QueryT:
		{
			args := op.Args.(QueryArgs)
			num := args.Num
			DPrintf("[sc %v] try to query, Num = %v\n", sc.me, args.Num)
			if args.Num == -1 || args.Num > sc.cfgNum {
				num = sc.cfgNum
			}
			cfg := sc.configs[num]
			return cfg
		}
	}
	return sc.configs[sc.cfgNum]
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
		DPrintf("[sc %v] opchan = %v\n", sc.me, opChan)
		if _, okk := sc.clientLastIndex[cid.ClerkId]; okk && cid.NextCallIndex <= sc.clientLastIndex[cid.ClerkId] {
			DPrintf("[sc %v] dup of getNextC = %v, expect should bigger than %v\n", sc.me, cid.NextCallIndex, sc.clientLastIndex[cid.ClerkId])
			if ok {
				var cfg Config
				var num int = sc.cfgNum
				if op.Type == QueryT {
					if _, ok := sc.queryNumHistory[op.Cid]; ok {
						num = sc.queryNumHistory[op.Cid]
					}
					cfg = sc.configs[num]
				} else {
					cfg = Config{}
				}
				opChan.cond.L.Lock()
				DPrintf("[sc %v] make opChan res = OpResOK\n", sc.me)
				opChan.opRes = OpResOk
				opChan.res = cfg
				opChan.cond.Broadcast()
				opChan.cond.L.Unlock()
			}
			sc.mu.Unlock()
			continue
		}
		//DPrintf("[sc %v] apply op = %v\n", sc.me, op)
		cfg := sc.applyOp(op)
		DPrintf("[sc %v] apply op = %v, latest config = %v\n", sc.me, op, cfg)
		if ok {
			opChan.cond.L.Lock()
			DPrintf("[sc %v] make opChan res = OpResOK\n", sc.me)
			opChan.opRes = OpResOk
			opChan.res = cfg
			opChan.cond.Broadcast()
			opChan.cond.L.Unlock()
		}
		if op.Type == QueryT {
			num := op.Args.(QueryArgs).Num
			if op.Args.(QueryArgs).Num == -1 || op.Args.(QueryArgs).Num > sc.cfgNum {
				num = sc.cfgNum
			}
			sc.queryNumHistory[op.Cid] = num
		}
		sc.clientLastIndex[cid.ClerkId] = cid.NextCallIndex
		sc.mu.Unlock()
	}
}
func freshOpChan(index int) OpChan {
	return OpChan{
		cond:  utils.NewCond(&sync.Mutex{}),
		opRes: OpUnknown,
		index: index,
		res:   Config{},
	}
}
func (sc *ShardCtrler) waitForCondTimeOutNoneLock(opChan *OpChan, cid ClientId) (Err, Config) {
	opChan.cond.L.Lock()
	var err Err
	var cfg Config
	if opChan.opRes == OpUnknown {
		opChan.cond.WaitWithTimeout(time.Second)
	}
	if opChan.opRes == OpUnknown {
		err = TimeOut
		cfg = Config{}
		opChan.cond.L.Unlock()
	} else {
		err = OK
		cfg = opChan.res
		opChan.cond.L.Unlock()
		sc.mu.Lock()
		sc.dupTable.clearByClientId(cid.previousCallIndex())
		sc.mu.Unlock()
	}
	return err, cfg
}
func (sc *ShardCtrler) startOp(op Op) (wrongLeader bool, err Err, cfg Config) {
	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		DPrintf("[sc %v] not leader, failed\n", sc.me)
		sc.mu.Unlock()
		wrongLeader = true
		err = WrongLeader
		return
	}
	opChan := freshOpChan(index)
	sc.dupTable.insert(op.Cid, &opChan)
	DPrintf("[sc %v] now cache table = %v\n", sc.me, sc.dupTable)
	sc.mu.Unlock()
	wrongLeader = false
	err, cfg = sc.waitForCondTimeOutNoneLock(&opChan, op.Cid)
	return
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("[sc %v] try to join, args = %v\n", sc.me, args)
	op := Op{Type: JoinT, Args: *args, Cid: args.Cid}
	wrongLeader, err, _ := sc.startOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	DPrintf("[sc %v] finished join, res = %v\n", sc.me, reply)
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: LeaveT, Args: *args, Cid: args.Cid}
	wrongLeader, err, _ := sc.startOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Type: MoveT, Args: *args, Cid: args.Cid}
	wrongLeader, err, _ := sc.startOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Type: QueryT, Args: *args, Cid: args.Cid}
	wrongLeader, err, cfg := sc.startOp(op)
	reply.WrongLeader = wrongLeader
	reply.Config = cfg
	reply.Err = err
	DPrintf("[sc %v] finished query, res = %v\n", sc.me, reply)
	return
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
	sc.cfgNum = 0
	atomic.StoreInt32(&sc.dead, 0)
	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.dupTable = CacheTable{
		dupTable: make(map[ClientId]*OpChan),
		IndexId:  make(map[int]ClientId),
	}
	sc.gidMapShardNum = map[int]int{}
	sc.gidMapShardNum[0] = NShards
	sc.lastAppliedIndex = 0
	sc.clientLastIndex = map[int]int{}
	sc.queryNumHistory = map[ClientId]int{}
	// Your code here.
	go sc.applier()
	return sc
}
