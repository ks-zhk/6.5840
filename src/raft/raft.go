package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"errors"
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

const MAX_LOG_NUM = 1999

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state, 暂时是一把大锁保平安
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// below is (2A)
	term                 int   // current term, or latest term server has seen. initial is 0 on first boot, need
	state                State // current state, [leader, follower, candidate]
	voteGet              int   // when in candidate, and only in candidate state, this value is valid, means vote get from follower
	hasVoted             bool  // only when in follower state is valid. every time when term update, the hasVote will reset to false
	lastVotedCandidateId int
	getMsg               bool
	lastHeartBeatOver    []bool
	applyCh              []chan reqWarp
	// below if (2B)
	logs        []Entry
	commitIndex int
	lastApplied int

	nextIndex     []int
	matchIndex    []int
	CallerApplyCh chan ApplyMsg
	MineApplyCh   chan ApplyMsg
	heartBeatChan chan int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	snapshot              []byte
	snapshotLastIndex     int
	snapshotLastTerm      int
	snapshotLastOffset    int
	snapshotLastIndexTemp int
}

func (rf *Raft) solveApplyMsg(me int) {
	for m := range rf.MineApplyCh {
		if !m.CommandValid && !m.SnapshotValid {
			return
		}
		rf.CallerApplyCh <- m
		DPrintf("%v applied %v\n", me, m)
	}
}

func assert(res bool, errMsg string) {
	if !res {
		panic(errMsg)
	}
}

// idx ok
func (rf *Raft) tryGetEntryByIndexNoneLock(idx int) (Entry, error) {
	if idx <= rf.snapshotLastIndex {
		return Entry{}, errors.New("index is too old which has become snapshot")
	}
	return rf.logs[rf.snapshotLastIndex+idx-1], nil
}

// idx ok
func (rf *Raft) getEntryByIndexNoneLock(idx int) Entry {
	if idx-rf.snapshotLastIndex == 0 {
		return Entry{
			Index: rf.snapshotLastIndex,
			Term:  rf.snapshotLastTerm,
		}
	}
	return rf.logs[idx-rf.snapshotLastIndex-1]
}
func (rf *Raft) convertLocalIndex(idx int) int {
	return idx - rf.snapshotLastIndex
}
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return rf.snapshotLastIndex
	}
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) needSendInstallSnapshotNoneLock(peerId int) bool {
	return rf.nextIndex[peerId] <= rf.snapshotLastIndex
}

// idx ok
func (rf *Raft) getNowAppendRequestNoneLock(peerId int) (RequestAppendEntries, bool) {
	var prevIndex int
	var prevTerm int
	if rf.nextIndex[peerId] <= rf.snapshotLastIndex {
		return RequestAppendEntries{}, false
	}
	if rf.nextIndex[peerId] == rf.snapshotLastIndex+1 {
		prevIndex = rf.snapshotLastIndex
		prevTerm = rf.snapshotLastTerm
	} else {
		prevIndex = rf.nextIndex[peerId] - 1
		prevTerm = rf.logs[rf.convertLocalIndex(prevIndex)-1].Term
	}
	var entries []Entry
	if len(rf.logs)+1+rf.snapshotLastIndex == rf.nextIndex[peerId] {
		entries = []Entry{}
	} else {
		entries = make([]Entry, len(rf.logs[rf.convertLocalIndex(rf.nextIndex[peerId])-1:]))
		copy(entries, rf.logs[rf.convertLocalIndex(rf.nextIndex[peerId])-1:])
	}
	return RequestAppendEntries{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}, true
}

type reqWarp struct {
	Req RequestAppendEntries
	Msg Msg
}

// idx ok
func (rf *Raft) getLastLogTermNoneLock() int {
	if len(rf.logs) == 0 {
		return rf.snapshotLastTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

// idx ok
func (rf *Raft) getPrevInfoByPeerIdxNoneLock(peerId int) (prevIndex int, prevTerm int) {
	if rf.nextIndex[peerId] <= rf.snapshotLastIndex+1 {
		prevIndex = rf.snapshotLastIndex
		prevTerm = rf.snapshotLastTerm
	} else {
		prevIndex = rf.nextIndex[peerId] - 1
		prevTerm = rf.getEntryByIndexNoneLock(prevIndex).Term
	}
	return
}
func (rf *Raft) needQuitForLeaderNoneLock(term int) bool {
	return rf.killed() || rf.state != Leader || rf.term != term
}

// 首先假设当前的raft是一个leader，然后上锁，检查是否应该继续保持上锁
// 如果不满足本routine的原则，那么就直接解锁，然后返回false
// 成功通过检测，则继续持有锁，然后返回true
func (rf *Raft) lockWithCheckForLeader(term int) bool {
	// DPrintf("[%v] try to lock in lockWithCheckForLeader\n", rf.me)
	rf.mu.Lock()
	if rf.needQuitForLeaderNoneLock(term) {
		rf.mu.Unlock()
		return false
	}
	return true
}

// idx ok
func (rf *Raft) convertToLeaderConfigNoneLock() {
	DPrintf("[%v][%v][%v] become leader\n", rf.me, rf.term, rf.snapshotLastIndex)
	for i := 0; i < len(rf.peers); i++ {
		if len(rf.logs) == 0 {
			rf.nextIndex[i] = rf.snapshotLastIndex + 1
		} else {
			rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
		}
		rf.matchIndex[i] = 0
		rf.lastHeartBeatOver[i] = true
		rf.applyCh[i] = make(chan reqWarp, 100)
		go rf.onePeerOneChannelConcurrent(i, rf.term)
	}
	go rf.sendHeartBeatAliveJustLoop(rf.term)
	rf.persistNoneLock()
}

type State int

const (
	Leader    State = 0
	Follower  State = 1
	Candidate State = 2
)

type Msg int

const (
	Normal        Msg = 0
	Quit          Msg = 1
	HeartBeat     Msg = 2
	MustHeartBeat Msg = 3
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isLeader = rf.state == Leader
	return term, isLeader
}

// idx ok
func (rf *Raft) getVoteReqArgsNoneLock() RequestVoteArgs {
	// TODO: change for 2B
	var lastLogIndex int
	if len(rf.logs) == 0 {
		lastLogIndex = rf.snapshotLastIndex
	} else {
		lastLogIndex = rf.snapshotLastIndex + len(rf.logs)
	}
	return RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLogTermNoneLock(),
		LastLogIndex: lastLogIndex,
	}
}

// idx ok
func (rf *Raft) getHeartBeatMsgNoneLock(peerId int) (RequestAppendEntries, error) {
	if rf.state != Leader {
		return RequestAppendEntries{}, errors.New("raft is not leader")
	}
	prevLogIndex, prevLogTerm := rf.getPrevInfoByPeerIdxNoneLock(peerId)
	return RequestAppendEntries{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      []Entry{},
		LeaderCommit: rf.commitIndex,
	}, nil
}
func (rf *Raft) sendVoteReqToAllPeerNoneLock(term int) {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		req := rf.getVoteReqArgsNoneLock()
		var reply RequestVoteReply
		go func(req RequestVoteArgs, reply RequestVoteReply, idx int, term int) {
			rf.sendRequestVote(idx, &req, &reply, term)
		}(req, reply, idx, term)
	}
}
func (rf *Raft) becomeCandidateNoneLock() {
	rf.term += 1
	rf.state = Candidate
	rf.getMsg = true
	rf.hasVoted = true
	rf.voteGet = 1
	rf.persistNoneLock()
	DPrintf("[%v][%v][%v] become candidate\n", rf.me, rf.term, rf.snapshotLastIndex)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// 其实应该使用temp file，然后atomic存储，从而防止不一致性。
func (rf *Raft) persistNoneLock() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteGet)
	e.Encode(rf.logs)
	e.Encode(rf.state)
	e.Encode(rf.hasVoted)
	e.Encode(rf.lastVotedCandidateId)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	//e.Encode(rf.snapshotLastCommand)
	//e.Encode(rf.snapshot), realize in snapshot
	raftState := w.Bytes()
	rf.persister.Save(raftState, clone(rf.snapshot))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var term int
	var voteGet int
	var logs []Entry
	var state State
	var hasVoted bool
	var lastVotedCandidateId int
	var snapshotLastIndex int
	var snapshotLastTerm int
	if decoder.Decode(&term) != nil ||
		decoder.Decode(&voteGet) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&state) != nil ||
		decoder.Decode(&hasVoted) != nil ||
		decoder.Decode(&lastVotedCandidateId) != nil ||
		decoder.Decode(&snapshotLastIndex) != nil ||
		decoder.Decode(&snapshotLastTerm) != nil {
		panic("decode error")
	} else {
		rf.term = term
		rf.logs = logs
		rf.voteGet = voteGet
		rf.state = state
		rf.hasVoted = hasVoted
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// TODO: realize snapshot
	// DPrintf("[%v] try to lock in snapshot\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotLastIndex || index <= rf.snapshotLastIndexTemp {
		// snapshot is not the latest
		if rf.commitIndex < index {
			rf.commitIndex = index
		}
		DPrintf("[%v][%v][%v] snapshot is not the latest\n", rf.me, rf.term, rf.snapshotLastIndex)
		return
	}
	if index > rf.getLastLogIndex() {
		// do nothing
		DPrintf("[%v][%v][%v] snapshot is too big, can not fit\n", rf.me, rf.term, rf.snapshotLastIndex)
		return
	}
	rf.snapshot = snapshot
	lastEntry := rf.getEntryByIndexNoneLock(index)
	// 准备commit
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	rf.logs = rf.logs[rf.convertLocalIndex(index):]
	rf.snapshotLastIndex = index
	rf.snapshotLastTerm = lastEntry.Term
	rf.persistNoneLock()
	return
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// below is for 2A
	Term         int // me term
	CandidateId  int // me id
	LastLogIndex int
	LastLogTerm  int
	// below is for 2B
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int  // peer term
	Voted bool // true if peer vote for me
}
type RequestAppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type ReplyAppendEntries struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}
type RequestInstallSnapshot struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type ReplyInstallSnapshot struct {
	Term  int
	Valid bool
}

func (rf *Raft) InstallSnapshot(args *RequestInstallSnapshot, reply *ReplyInstallSnapshot) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Valid = false
		return
	}
	if args.Term > rf.term || rf.state == Candidate {
		rf.convertToFollowerNoneLock(args.Term)
	}
	reply.Term = rf.term
	if args.Offset < rf.snapshotLastOffset {
		reply.Valid = false
		return
	}
	// reset timeout
	rf.getMsg = true
	// 使用 >= 实现幂等性
	if args.Offset == 0 {
		// create snapshot file
		rf.snapshot = []byte{}
	}
	rf.snapshotLastIndexTemp = args.LastIncludedIndex
	if rf.snapshotLastIndexTemp <= rf.snapshotLastIndex {
		// 旧的不要了，直接切断吧
		reply.Valid = false
		return
	}
	if len(rf.snapshot) == args.Offset {
		rf.snapshot = append(rf.snapshot, args.Data...)
		rf.snapshotLastOffset = rf.snapshotLastOffset + len(args.Data)
	} else {
		if len(rf.snapshot) != args.Offset+len(args.Data) {
			panic("TODO 幂等性失效，这里需要重新填写snapshot")
		}
	}
	if !args.Done {
		reply.Valid = true
		return
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.convertLocalIndex(args.LastIncludedIndex) > 0 && args.LastIncludedIndex < rf.getLastLogIndex() {
		entry := rf.getEntryByIndexNoneLock(args.LastIncludedIndex)
		if entry.Term == args.LastIncludedTerm {
			rf.logs = rf.logs[rf.convertLocalIndex(args.LastIncludedIndex):]
		} else {
			rf.logs = []Entry{}
		}
	} else {
		rf.logs = []Entry{}
	}
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.LastIncludedTerm
	rf.snapshotLastIndexTemp = -1
	rf.snapshotLastOffset = 0
	rf.persistNoneLock()
	// 这边就已经包含了等待提交的信息
	// fmt.Printf("[%v][%v] try to send snapshot\n", rf.me, rf.term)
	rf.MineApplyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      clone(rf.snapshot),
		SnapshotIndex: rf.snapshotLastIndex,
		SnapshotTerm:  rf.snapshotLastTerm,
	}
	reply.Valid = true
}

// idx ok
func (rf *Raft) AppendEntriesNew(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	// DPrintf("[%v] try to lock in AppendEntriesNew\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer DPrintf("[%v][%v] logs = %v\n", rf.me, rf.term, rf.logs)
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		return
	}
	if args.Term > rf.term {
		rf.convertToFollowerNoneLock(args.Term)
	}
	rf.getMsg = true
	reply.Term = rf.term
	if rf.state == Candidate {
		rf.convertToFollowerNoneLock(rf.term)
	}
	if rf.state == Follower {
		// 特殊情况特殊讨论
		// 1. if not long enough(rf.log[last].Index < args.prevLog)
		// |-----snapshot-----|-------logs--------|----prevLog-------
		// 2. long enough, also in logs, wait to match term
		// |-----snapshot-----|------prevLog--logs|--------------- (include rf.snapshotLastIndex)
		// 3. long enough, but in snapshot, do not know anything
		// |-prevLog-snapshot-|-------logs--------|---------------
		if rf.getLastLogIndex() < args.PrevLogIndex {
			assert(rf.snapshotLastIndex+len(rf.logs) == rf.getLastLogIndex(), "entry index should same with log's local index")
			// not long enough
			reply.Success = false
			reply.XLen = rf.snapshotLastIndex + len(rf.logs)
			reply.XTerm = -1
			reply.XIndex = -1
			return
		}
		if rf.convertLocalIndex(args.PrevLogIndex) > 0 {
			// long enough, check the
			entry := rf.getEntryByIndexNoneLock(args.PrevLogIndex)
			if entry.Term != args.PrevLogTerm {
				reply.XTerm = entry.Term
				// 状态转移
				rf.logs = rf.logs[:rf.convertLocalIndex(args.PrevLogIndex)-1]
				reply.XIndex = args.PrevLogIndex
				for _, log := range rf.logs {
					if log.Term == entry.Term {
						reply.XIndex = log.Index
						break
					}
				}
				reply.Success = false
				reply.XLen = rf.snapshotLastIndex + len(rf.logs)
				return
			}
		}
		// del
		if rf.convertLocalIndex(args.PrevLogIndex) == 0 {
			if rf.snapshotLastTerm != args.PrevLogTerm {
				// snapshot 不同
				reply.XTerm = rf.snapshotLastTerm
				reply.Success = false
				reply.XLen = rf.snapshotLastIndex - 1
				reply.XIndex = -1 //unknown
				panic("snapshot 不可能出错！！")
				return
			}
		}
		// del
		if rf.convertLocalIndex(args.PrevLogIndex) < 0 {
			// snapshot未知, 应该进行snapshot覆盖
			reply.XTerm = -1 // 是没有冲突的
			reply.Success = false
			reply.XLen = rf.snapshotLastIndex + len(rf.logs)
			reply.XIndex = -1 // 没有冲突
			return
		}
		var i int
		conflict := false
		for i = args.PrevLogIndex; i < args.PrevLogIndex+len(args.Entries) && i < rf.snapshotLastIndex+len(rf.logs); i++ {
			if rf.logs[rf.convertLocalIndex(i)].Term != args.Entries[i-args.PrevLogIndex].Term {
				conflict = true
				break
			}
		}
		if conflict {
			DPrintf("[%v][%v][%v] conflict\n", rf.me, rf.term, rf.snapshotLastIndex)
			rf.logs = rf.logs[:rf.convertLocalIndex(i)]
		}
		for _, log := range args.Entries[i-args.PrevLogIndex:] {

			rf.logs = append(rf.logs, log)
			if rf.snapshotLastIndex+len(rf.logs) != log.Index {
				DPrintf("[%v][%v][%v] need idx = %v, get idx = %v\n", rf.me, rf.term, rf.snapshotLastIndex, rf.snapshotLastIndex+len(rf.logs), log.Index)
				panic("log index is not match")
			}
		}
		rf.persistNoneLock()
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.snapshotLastIndex+len(rf.logs) {
				for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
					// TODO: should reply with command?
					if rf.convertLocalIndex(i) < 1 {
						continue
					}
					DPrintf("[%v][%v][%v] apply index = %v\n", rf.me, rf.term, rf.snapshotLastIndex, i)
					rf.MineApplyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.getEntryByIndexNoneLock(i).Command,
						CommandIndex: i,
					}
				}
				rf.commitIndex = args.LeaderCommit
				//fmt.Println(rf.commitIndex)
			} else {
				var i int
				for i = rf.commitIndex + 1; i <= rf.snapshotLastIndex+len(rf.logs) && i <= args.PrevLogIndex+len(args.Entries); i++ {
					if rf.convertLocalIndex(i) < 1 {
						continue
					}
					DPrintf("[%v][%v][%v] apply index = %v\n", rf.me, rf.term, rf.snapshotLastIndex, i)
					rf.MineApplyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.getEntryByIndexNoneLock(i).Command,
						CommandIndex: i,
					}
				}
				rf.commitIndex = i - 1
				//fmt.Println(rf.commitIndex)
			}
		}
		reply.Success = true
	} else if rf.state == Candidate {
		// candidate
		rf.convertToFollowerNoneLock(args.Term)
		reply.Success = true
	} else {
		// leader
		reply.Success = false
	}
	// fmt.Printf("in %v, log = %v\n", rf.me, rf.logs)
	return
}

// have new AppendEntriesFunc
//func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	//defer DPrintf("[%v][%v] logs = %v\n", rf.me, rf.term, rf.logs)
//	if args.Term > rf.term {
//		DPrintf("[%v][%v] get append req, but the term is big\n", rf.me, rf.term)
//		rf.convertToFollowerNoneLock(args.Term)
//	}
//	if args.Term < rf.term {
//		DPrintf("[%v][%v] get append req, but the term is small\n", rf.me, rf.term)
//		reply.Term = rf.term
//		reply.Success = false
//		return
//	}
//	rf.getMsg = true
//	reply.Term = rf.term
//	if rf.state == Candidate {
//		rf.convertToFollowerNoneLock(rf.term)
//	}
//	if rf.state == Follower {
//		// 特殊情况特殊讨论
//		if args.PrevLogIndex == 0 {
//			// 最特殊的情况, 直接无脑加log即可
//			// fmt.Printf("in it\n")
//			var i int
//			conflict := false
//			for i = args.PrevLogIndex; i < args.PrevLogIndex+len(args.Entries) && i < len(rf.logs); i++ {
//				if rf.logs[i].Term != args.Entries[i-args.PrevLogIndex].Term {
//					conflict = true
//					break
//				}
//			}
//			// TODO: 直接清空
//			// 这边其实不应该考虑截断日志。
//			if conflict {
//				rf.logs = rf.logs[:i]
//			}
//			for _, log := range args.Entries[i-args.PrevLogIndex:] {
//				rf.logs = append(rf.logs, log)
//			}
//			rf.persistNoneLock()
//			if args.LeaderCommit > rf.commitIndex {
//				if args.LeaderCommit < len(rf.logs) {
//					for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
//						rf.MineApplyCh <- ApplyMsg{
//							CommandValid: true,
//							Command:      rf.logs[i-1].Command,
//							CommandIndex: rf.logs[i-1].Index,
//						}
//					}
//					rf.commitIndex = args.LeaderCommit
//				} else {
//					// TODO: 是否要在之前的操作中把那个玩意给干掉。
//					for i := rf.commitIndex + 1; i <= len(rf.logs); i++ {
//						rf.MineApplyCh <- ApplyMsg{
//							CommandValid: true,
//							Command:      rf.logs[i-1].Command,
//							CommandIndex: rf.logs[i-1].Index,
//						}
//					}
//					rf.commitIndex = len(rf.logs)
//				}
//			}
//			reply.Success = true
//			// fmt.Printf("in %v, log = %v\n", rf.me, rf.logs)
//			return
//		}
//		if len(rf.logs) < args.PrevLogIndex {
//			reply.Success = false
//			reply.XLen = len(rf.logs)
//			reply.XTerm = -1
//			reply.XIndex = -1
//			return
//		}
//		if rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
//			reply.XTerm = rf.logs[args.PrevLogIndex-1].Term
//			reply.XIndex = args.PrevLogIndex
//			rf.logs = rf.logs[:args.PrevLogIndex-1]
//			reply.Success = false
//			reply.XLen = len(rf.logs)
//			// TODO: 从后往前找而不是从前往后
//			for idx, log := range rf.logs {
//				if log.Term == reply.XTerm {
//					reply.XIndex = idx + 1
//					break
//				}
//			}
//			rf.persistNoneLock()
//			return
//		}
//		var i int
//		conflict := false
//		for i = args.PrevLogIndex; i < args.PrevLogIndex+len(args.Entries) && i < len(rf.logs); i++ {
//			if rf.logs[i].Term != args.Entries[i-args.PrevLogIndex].Term {
//				conflict = true
//				break
//			}
//		}
//		if conflict {
//			rf.logs = rf.logs[:i]
//		}
//		for _, log := range args.Entries[i-args.PrevLogIndex:] {
//			rf.logs = append(rf.logs, log)
//			//rf.CallerApplyCh <- ApplyMsg{
//			//	CommandValid: true,
//			//	CommandIndex: len(rf.logs),
//			//	Command:      log.Command,
//			//}
//		}
//		rf.persistNoneLock()
//		if args.LeaderCommit > rf.commitIndex {
//			if args.LeaderCommit < len(rf.logs) {
//				for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
//					rf.MineApplyCh <- ApplyMsg{
//						CommandValid: true,
//						Command:      rf.logs[i-1].Command,
//						CommandIndex: rf.logs[i-1].Index,
//					}
//				}
//				rf.commitIndex = args.LeaderCommit
//			} else {
//				// TODO: 是否要在之前的操作中把那个玩意给干掉。
//				for i := rf.commitIndex + 1; i <= len(rf.logs); i++ {
//					rf.MineApplyCh <- ApplyMsg{
//						CommandValid: true,
//						Command:      rf.logs[i-1].Command,
//						CommandIndex: rf.logs[i-1].Index,
//					}
//				}
//				rf.commitIndex = len(rf.logs)
//			}
//		}
//		reply.Success = true
//	} else if rf.state == Candidate {
//		// candidate
//		rf.convertToFollowerNoneLock(args.Term)
//		reply.Success = true
//	} else {
//		// leader
//		reply.Success = false
//	}
//	// fmt.Printf("in %v, log = %v\n", rf.me, rf.logs)
//	return
//}

func (rf *Raft) convertToFollowerNoneLock(newTerm int) {
	DPrintf("[%v][%v][%v] become follower with new term %v\n", rf.me, rf.term, rf.snapshotLastIndex, newTerm)
	rf.term = newTerm
	rf.state = Follower
	rf.hasVoted = false
	rf.snapshotLastOffset = 0
	rf.snapshotLastIndexTemp = -1
	//rf.getMsg = true
	rf.voteGet = 0
	rf.persistNoneLock()
}

// example RequestVote RPC handler.
// this func is to solve request from peer.
// idx is ok
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// below is for 2A
	// DPrintf("[%v] try to lock in RequestVote\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Voted = false
		reply.Term = rf.term
		return
	}
	if args.Term > rf.term {
		rf.convertToFollowerNoneLock(args.Term)
	}
	switch rf.state {
	case Follower:
		{
			// 先来先到原则
			if !rf.hasVoted {
				if args.LastLogTerm < rf.getLastLogTermNoneLock() {
					reply.Voted = false
					reply.Term = rf.term
					DPrintf("[%v][%v][%v] deny %v's vote request for last log term is not at least up-to-date\n", rf.me, rf.term, rf.snapshotLastIndex, args.CandidateId)
					return
				} else if args.LastLogTerm == rf.getLastLogTermNoneLock() {
					if rf.getLastLogIndex() > args.LastLogIndex {
						reply.Voted = false
						reply.Term = rf.term
						DPrintf("[%v][%v][%v] deny %v's vote request for same term but log is not longer\n", rf.me, rf.term, rf.snapshotLastIndex, args.CandidateId)
						return
					} else {
						DPrintf("[%v][%v][%v] vote for %v\n", rf.me, rf.term, rf.snapshotLastIndex, args.CandidateId)
						rf.hasVoted = true
						reply.Voted = true
						rf.getMsg = true
						rf.lastVotedCandidateId = args.CandidateId
					}
				} else {
					DPrintf("[%v][%v][%v] vote for %v\n", rf.me, rf.term, rf.snapshotLastIndex, args.CandidateId)
					rf.hasVoted = true
					reply.Voted = true
					rf.getMsg = true
					rf.lastVotedCandidateId = args.CandidateId
				}
				rf.persistNoneLock()
			} else {
				// fmt.Println("deny!!")
				if rf.lastVotedCandidateId == args.CandidateId {
					DPrintf("[%v][%v][%v] vote for %v\n", rf.me, rf.term, rf.snapshotLastIndex, args.CandidateId)
					reply.Voted = true
					rf.getMsg = true
				} else {
					DPrintf("[%v][%v][%v] deny for %v because of hasBeen voted\n", rf.me, rf.term, rf.snapshotLastIndex, args.CandidateId)
					reply.Voted = false
				}
			}
		}
	case Candidate:
		{
			reply.Voted = false
		}
	case Leader:
		{
			reply.Voted = false
		}
	}
	reply.Term = rf.term
	return
}
func (rf *Raft) sendAppendRequest(term int, peerId int, argsStatic RequestAppendEntries) {
	var reply ReplyAppendEntries
	var prevNextIndex int
	res := false
	for res == false {
		if !rf.lockWithCheckForLeader(term) {
			return
		}
		args, ok := rf.getNowAppendRequestNoneLock(peerId)
		prevNextIndex = rf.nextIndex[peerId]
		for ok == false {
			rf.mu.Unlock()
			if rf.sendSnapShotInstallRequest(term, peerId, prevNextIndex) {
				// snapshot 安装成功
				if !rf.lockWithCheckForLeader(term) {
					return
				}
				args, ok = rf.getNowAppendRequestNoneLock(peerId)
			} else {
				return
			}
		}
		DPrintf("[%v][%v][%v] try send append req to %v, with req = %v\n", rf.me, rf.term, rf.snapshotLastIndex, peerId, args)
		argsStatic = args
		rf.mu.Unlock()
		reply = ReplyAppendEntries{}
		res = rf.peers[peerId].Call("Raft.AppendEntriesNew", &args, &reply)
	}
	for reply.Success == false {
		// decrease
		if !rf.lockWithCheckForLeader(term) {
			return
		}
		DPrintf("[%v][%v][%v] get reply from [%v] = %v\n", rf.me, rf.term, rf.snapshotLastIndex, peerId, reply)
		if reply.Term > rf.term {
			rf.convertToFollowerNoneLock(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Term < rf.term {
			rf.mu.Unlock()
			break
		}
		if rf.nextIndex[peerId] == prevNextIndex {
			if reply.XLen < rf.snapshotLastIndex {
				rf.mu.Unlock()
				if rf.sendSnapShotInstallRequest(term, peerId, prevNextIndex) {
					if !rf.lockWithCheckForLeader(term) {
						return
					}
				} else {
					return
				}
			} else {
				if reply.XTerm == -1 && reply.XIndex == -1 {
					//if rf.nextIndex
					if reply.XLen < rf.snapshotLastIndex {
						rf.mu.Unlock()
						if rf.sendSnapShotInstallRequest(term, peerId, prevNextIndex) {
							if !rf.lockWithCheckForLeader(term) {
								return
							}
						} else {
							return
						}
					} else {
						rf.nextIndex[peerId] = reply.XLen + 1
					}
				} else {
					find := false
					for i := len(rf.logs) - 1; i >= 0; i-- {
						if rf.logs[i].Term == reply.XTerm {
							find = true
							rf.nextIndex[peerId] = i + 1
							break
						}
					}
					if !find {
						rf.nextIndex[peerId] = reply.XIndex
					}
				}
			}
		}
		rf.mu.Unlock()
		res = false
		for res == false {
			if !rf.lockWithCheckForLeader(term) {
				return
			}
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			args, ok := rf.getNowAppendRequestNoneLock(peerId)
			prevNextIndex = rf.nextIndex[peerId]
			for ok == false {
				rf.mu.Unlock()
				if rf.sendSnapShotInstallRequest(term, peerId, prevNextIndex) {
					if !rf.lockWithCheckForLeader(term) {
						return
					}
					args, ok = rf.getNowAppendRequestNoneLock(peerId)
				} else {
					return
				}
			}
			DPrintf("[%v][%v][%v] try send append req to %v, with req = %v\n", rf.me, rf.term, rf.snapshotLastIndex, peerId, args)
			argsStatic = args
			rf.mu.Unlock()
			reply = ReplyAppendEntries{}
			res = rf.peers[peerId].Call("Raft.AppendEntriesNew", &args, &reply)
		}
	}
	if !rf.lockWithCheckForLeader(term) {
		return
	}
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.term {
		rf.convertToFollowerNoneLock(reply.Term)
		rf.mu.Unlock()
		return
	}
	if reply.Term < rf.term {
		rf.mu.Unlock()
		return
	}
	// fmt.Printf("LEADER %v's log %v\n", rf.me, rf.logs)
	DPrintf("[%v][%v][%v] log's entry len = %v\n", rf.me, rf.term, rf.snapshotLastIndex, len(argsStatic.Entries))
	if argsStatic.PrevLogIndex+len(argsStatic.Entries) > rf.matchIndex[peerId] {
		rf.matchIndex[peerId] = argsStatic.PrevLogIndex + len(argsStatic.Entries)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
	}
	DPrintf("[%v][%v][%v] receive success append apply from %v, nextIndex = %v, matchIndex = %v\n", rf.me, rf.term, rf.snapshotLastIndex, peerId, rf.nextIndex[peerId], rf.matchIndex[peerId])
	for n := rf.snapshotLastIndex + len(rf.logs); n > rf.commitIndex; n-- {
		cnt := 0
		for _, idx := range rf.matchIndex {
			if idx >= n {
				cnt++
			}
		}
		if cnt >= (len(rf.peers)+1)/2 && rf.logs[rf.convertLocalIndex(n)-1].Term == rf.term {
			for i := rf.commitIndex + 1; i <= n; i++ {
				//DPrintf("[%v][%v] commit log = %v\n", rf.me, rf.term, rf.logs[i-1])
				// fmt.Printf("[%v][%v] try to send applyMsg\n", rf.me, rf.term)
				rf.MineApplyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.convertLocalIndex(i)-1].Command,
					CommandIndex: i,
				}
			}
			if rf.commitIndex < n {
				rf.commitIndex = n
			}
			break
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendSnapShotInstallRequest(term int, peerId int, prevLastNextIndex int) bool {
	offset := 0
	firstChunkSize := 4096
	chunkMaxSize := 4096
	for {
		if !rf.lockWithCheckForLeader(term) {
			return false
		}
		DPrintf("[%v][%v][%v] sending snapshot to %v\n", rf.me, rf.term, rf.snapshotLastIndex, peerId)
		// locked
		var size int
		var done bool
		var data []byte
		if offset == 0 {
			size = firstChunkSize
		} else {
			size = chunkMaxSize
		}
		if offset+size >= len(rf.snapshot) {
			done = true
			data = clone(rf.snapshot[offset:])
			size = len(rf.snapshot[offset:])
		} else {
			done = false
			data = clone(rf.snapshot[offset : offset+size])
		}
		req := RequestInstallSnapshot{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.snapshotLastIndex,
			LastIncludedTerm:  rf.snapshotLastTerm,
			Offset:            offset,
			Data:              data,
			Done:              done,
		}
		var res = false
		var reply ReplyInstallSnapshot
		rf.mu.Unlock()
		for res == false {
			if !rf.lockWithCheckForLeader(term) {
				return false
			}
			req.LastIncludedIndex = rf.snapshotLastIndex
			req.LastIncludedTerm = rf.snapshotLastTerm

			// locked
			rf.mu.Unlock()
			reply = ReplyInstallSnapshot{}
			res = rf.peers[peerId].Call("Raft.InstallSnapshot", &req, &reply)
		}
		if !rf.lockWithCheckForLeader(term) {
			return false
		}
		// locked
		if reply.Term < rf.term {
			rf.mu.Unlock()
			return false
		}
		if reply.Term > rf.term {
			rf.convertToFollowerNoneLock(reply.Term)
			rf.mu.Unlock()
			return false
		}
		if !reply.Valid {
			rf.mu.Unlock()
			return false
		}
		offset += size
		if done {
			if rf.nextIndex[peerId] == prevLastNextIndex {
				rf.matchIndex[peerId] = req.LastIncludedIndex
				rf.nextIndex[peerId] = req.LastIncludedIndex + 1
			}
			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) onePeerOneChannelConcurrent(peerId int, term int) {
	rf.mu.Lock()
	ch := rf.applyCh[peerId]
	rf.mu.Unlock()
	for {
		msg, ok := <-ch
		if !ok {
			return
		}
		if msg.Msg == Quit {
			return
		}
		if !rf.lockWithCheckForLeader(term) {
			return
		}
		if msg.Msg == Normal && rf.convertLocalIndex(rf.nextIndex[peerId]) == len(rf.logs)+1 {
			// 相当于HeartBeat了，这种工作还是给heartBeat干吧
			rf.mu.Unlock()
			continue
		}
		go func(term int, peerId int, args RequestAppendEntries) {
			rf.sendAppendRequest(term, peerId, args)
		}(rf.term, peerId, RequestAppendEntries{})
		rf.mu.Unlock()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, _reply *RequestVoteReply, term int) {
	//fmt.Println("in line 769")
	var reply RequestVoteReply
	var res = false
	for res == false {
		reply = RequestVoteReply{}
		rf.mu.Lock()
		if rf.state != Candidate || rf.term != term || rf.killed() {
			rf.mu.Unlock()
			return
		}
		args.LastLogIndex = rf.getLastLogIndex()
		args.LastLogTerm = rf.getLastLogTermNoneLock()
		DPrintf("[%v][%v][%v] try send vote req to %v\n", rf.me, rf.term, rf.snapshotLastIndex, server)
		rf.mu.Unlock()
		res = rf.peers[server].Call("Raft.RequestVote", args, &reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		DPrintf("[%v][%v][%v] rf is killed, ignore the vote\n", rf.me, rf.term, rf.snapshotLastIndex)
		return
	}
	if reply.Term < rf.term {
		DPrintf("[%v][%v][%v] from [%v][%v], vote is out-data, ignore it\n", rf.me, rf.term, rf.snapshotLastIndex, server, reply.Term)
		return
	}
	if reply.Term > rf.term {
		rf.convertToFollowerNoneLock(reply.Term)
		return
	}
	if rf.term != term {
		DPrintf("[%v][%v][%v] in this func term %v is not latest, ignore\n", rf.me, rf.term, rf.snapshotLastIndex, term)
		return
	}
	if reply.Voted {
		if rf.state == Candidate {
			DPrintf("[%v][%v][%v] get vote from %v, now has voted = %v\n", rf.me, rf.term, rf.snapshotLastIndex, server, rf.voteGet)
			rf.voteGet += 1
			if rf.voteGet >= (len(rf.peers)+1)/2 {
				rf.state = Leader
				rf.convertToLeaderConfigNoneLock()
			} else {
				rf.persistNoneLock()
			}
		} else {
			DPrintf("[%v][%v][%v] get vote, however state is not candidate\n", rf.me, rf.term, rf.snapshotLastIndex)
		}
	}
	return
}
func (rf *Raft) sendHeartBeatAliveJustLoop(term int) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader || rf.term != term {
			rf.mu.Unlock()
			break
		}
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			_, err := rf.getHeartBeatMsgNoneLock(idx)
			if err != nil {
				panic("state must be leader")
			}
			var hb reqWarp
			hb = reqWarp{Req: RequestAppendEntries{}, Msg: HeartBeat}
			rf.applyCh[idx] <- hb
		}
		rf.mu.Unlock()
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	DPrintf("[%v][%v][%v] get a command = %v\n", rf.me, rf.term, rf.snapshotLastIndex, command)
	defer rf.mu.Unlock()
	//defer DPrintf("[%v][%v] in start defer logs = %v\n", rf.me, rf.term, rf.logs)
	isLeader = rf.state == Leader
	if !isLeader || rf.killed() {
		DPrintf("[%v][%v][%v] is not leader, return false\n", rf.me, rf.term, rf.snapshotLastIndex)
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.logs = append(rf.logs, Entry{
		Term:    rf.term,
		Index:   rf.snapshotLastIndex + len(rf.logs) + 1,
		Command: command,
	})
	rf.persistNoneLock()
	rf.matchIndex[rf.me] = rf.snapshotLastIndex + len(rf.logs)
	index = rf.snapshotLastIndex + len(rf.logs)
	term = rf.term
	// TODO: start to append log
	// first, 检查所有peer的nextIndex，查看是否比较落后
	// 如果不是最新的，那么就直接发送一个msg，进行复制操作
	for idx, nextIdx := range rf.nextIndex {
		if idx == rf.me {
			continue
		}
		if nextIdx <= rf.snapshotLastIndex+len(rf.logs) {
			rf.applyCh[idx] <- reqWarp{Req: RequestAppendEntries{}, Msg: Normal}
		}
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// close req send loop
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, ch := range rf.applyCh {
		ch <- reqWarp{Req: RequestAppendEntries{}, Msg: Quit}
		close(ch)
	}
	rf.MineApplyCh <- ApplyMsg{CommandValid: false, SnapshotValid: false}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type ElectionMsg int

const (
	GetVote        ElectionMsg = 1
	NewRount       ElectionMsg = 2
	BecomeFollower ElectionMsg = 3
)

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// DPrintf("[%v] try to lock in Ticker\n", rf.me)
		rf.mu.Lock()
		DPrintf("[%v][%v][%v] tick, and getMsg = %v\n", rf.me, rf.term, rf.snapshotLastIndex, rf.getMsg)
		if rf.getMsg == false && rf.state != Leader {
			rf.becomeCandidateNoneLock()
			rf.sendVoteReqToAllPeerNoneLock(rf.term)
		} else {
			rf.getMsg = false
		}
		rf.mu.Unlock()
		ms := 200 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("in make raft %v\n", me)
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.applyCh = []chan reqWarp{}
	// 为1的ch
	rf.heartBeatChan = make(chan int)
	rf.MineApplyCh = make(chan ApplyMsg, 10000)
	for _, _ = range peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.lastHeartBeatOver = append(rf.lastHeartBeatOver, true)
		rf.applyCh = append(rf.applyCh, make(chan reqWarp, 10000))
	}
	rf.commitIndex = 0
	rf.state = Follower
	rf.getMsg = false
	rf.voteGet = 0
	rf.hasVoted = false
	rf.logs = []Entry{}
	rf.term = 0
	rf.dead = 0
	rf.CallerApplyCh = applyCh
	rf.snapshotLastTerm = 0
	rf.snapshotLastIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.snapshotLastIndex
	rf.snapshot = persister.snapshot
	if rf.state == Candidate {
		rf.sendVoteReqToAllPeerNoneLock(rf.term)
	} else if rf.state == Leader {
		rf.convertToLeaderConfigNoneLock()
	}
	// start heart beat loop
	//go rf.sendHeartBeatAliveLoop()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.solveApplyMsg(rf.me)

	return rf
}
