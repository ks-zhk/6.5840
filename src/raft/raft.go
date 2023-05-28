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
	"errors"
	"sync"

	//	"bytes"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state, 暂时是一把大锁保平安
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// below is (2A)
	term              int   // current term, or latest term server has seen. initial is 0 on first boot
	state             State // current state, [leader, follower, candidate]
	voteGet           int   // when in candidate, and only in candidate state, this value is valid, means vote get from follower
	hasVoted          bool  // only when in follower state is valid. every time when term update, the hasVote will reset to false
	getMsg            bool
	lastHeartBeatOver []bool
	applyCh           []chan reqWarp
	// below if (2B)
	logs        []Entry
	commitIndex int
	lastApplied int

	nextIndex     []int
	matchIndex    []int
	CallerApplyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
type reqWarp struct {
	Req RequestAppendEntries
	Msg Msg
}

func (rf *Raft) getLastLogTermNoneLock() int {
	if len(rf.logs) == 0 {
		return 0
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}
func (rf *Raft) finishLastHeartWithLock(peerId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeatOver[peerId] = true
}
func (rf *Raft) getPrevInfoByPeerIdxNoneLock(peerId int) (prevIndex int, prevTerm int) {
	if rf.nextIndex[peerId] <= 1 {
		prevIndex = 0
		prevTerm = 0
		return
	} else {
		prevIndex = rf.nextIndex[peerId] - 1
		prevTerm = rf.logs[prevIndex-1].Term
		return
	}
}
func (rf *Raft) needQuitForLeaderNoneLock(term int) bool {
	return rf.killed() || rf.state != Leader || rf.term != term
}

// 首先假设当前的raft是一个leader，然后上锁，检查是否应该继续保持上锁
// 如果不满足本routine的原则，那么就直接解锁，然后返回false
// 成功通过检测，则继续持有锁，然后返回true
func (rf *Raft) lockWithCheckForLeader(term int) bool {
	rf.mu.Lock()
	if rf.needQuitForLeaderNoneLock(term) {
		rf.mu.Unlock()
		return false
	}
	return true
}
func (rf *Raft) initOfAllServerNoneLock() {
	rf.commitIndex = 0
	rf.lastApplied = 0
}
func (rf *Raft) onGetMsgWithLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.getMsg = true
}
func (rf *Raft) convertToLeaderConfigNoneLock() {
	DPrintf("[%v][%v] become leader\n", rf.me, rf.term)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.logs) + 1
		rf.matchIndex[i] = 0
		rf.lastHeartBeatOver[i] = true
		rf.applyCh[i] = make(chan reqWarp, 10000)
		go rf.onePeerOneChannel(i, rf.term)
	}
}
func (rf *Raft) PrevEntryInfo() (prevLogTerm int, PrevLogIndex int) {
	if len(rf.logs) == 0 {
		prevLogTerm = -1
		PrevLogIndex = -1
		return
	}
	// TODO:
	panic("todo")
	return
}

type State int

const (
	Leader    State = 0
	Follower  State = 1
	Candidate State = 2
)

type Msg int

const (
	Normal    Msg = 0
	Quit      Msg = 1
	HeartBeat Msg = 2
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
func (rf *Raft) isCandidate() bool {
	var res bool
	rf.mu.Lock()
	res = rf.state == Candidate
	rf.mu.Unlock()
	return res
}
func (rf *Raft) isLeaderWithLock() bool {
	var res bool
	rf.mu.Lock()
	res = rf.state == Leader
	rf.mu.Unlock()
	return res
}
func (rf *Raft) resetTimeout() {
	rf.mu.Lock()
	rf.getMsg = false
	rf.mu.Unlock()
}
func (rf *Raft) noNeedNextElection() bool {
	var res bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	res = rf.getMsg
	if rf.state == Candidate {
		// 如果仍然是candidate, 那么表示本次选举失败，应该进行下一次选举
		res = false
		return res
	}
	rf.getMsg = false
	return res
}
func (rf *Raft) noNeedNextElectionNoneLock() bool {
	var res bool
	res = rf.getMsg
	if rf.state == Candidate {
		// 如果仍然是candidate, 那么表示本次选举失败，应该进行下一次选举
		res = false
		return res
	}
	rf.getMsg = false
	return res
}
func (rf *Raft) needNextElectionNoneLock() bool {
	var res bool
	res = rf.getMsg
	rf.getMsg = false
	return res == false && rf.state != Leader
}
func (rf *Raft) onGetVote() int {
	var res int
	rf.mu.Lock()
	rf.voteGet += 1
	res = rf.voteGet
	rf.mu.Unlock()
	return res
}
func (rf *Raft) getVoteReqArgsNoneLock() RequestVoteArgs {
	// TODO: change for 2B
	return RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLogTermNoneLock(),
		LastLogIndex: len(rf.logs),
	}
}
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
func (rf *Raft) sendVoteReqToAllPeerNoneLock() {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		req := rf.getVoteReqArgsNoneLock()
		var reply RequestVoteReply
		go func(req RequestVoteArgs, reply RequestVoteReply, idx int, term int) {
			rf.sendRequestVote(idx, &req, &reply, term)
		}(req, reply, idx, rf.term)
	}
}
func (rf *Raft) becomeCandidateNoneLock() {
	rf.term += 1
	rf.state = Candidate
	rf.getMsg = false
	rf.hasVoted = true
	rf.voteGet = 1
	DPrintf("[%v][%v] become candidate\n", rf.me, rf.term)
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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("[%v][%v] logs = %v\n", rf.me, rf.term, rf.logs)
	if args.Term > rf.term {
		DPrintf("[%v][%v] get append req, but the term is big\n", rf.me, rf.term)
		rf.convertToFollowerNoneLock(args.Term)
	}
	if args.Term < rf.term {
		DPrintf("[%v][%v] get append req, but the term is small\n", rf.me, rf.term)
		reply.Term = rf.term
		reply.Success = false
		return
	}
	rf.getMsg = true
	reply.Term = rf.term
	if rf.state == Follower {
		// 特殊情况特殊讨论
		if args.PrevLogIndex == 0 {
			// 最特殊的情况, 直接无脑加log即可
			// fmt.Printf("in it\n")
			var i int
			for i = args.PrevLogIndex; i < args.PrevLogIndex+len(args.Entries) && i < len(rf.logs); i++ {
				if rf.logs[i].Term != args.Entries[i-args.PrevLogIndex].Term {
					break
				}
			}
			// TODO: 直接清空
			rf.logs = rf.logs[:i]
			for _, log := range args.Entries[i-args.PrevLogIndex:] {
				rf.logs = append(rf.logs, log)
			}
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < len(rf.logs) {
					for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
						rf.CallerApplyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[i-1].Command,
							CommandIndex: rf.logs[i-1].Index,
						}
					}
					rf.commitIndex = args.LeaderCommit
				} else {
					// TODO: 是否要在之前的操作中把那个玩意给干掉。
					for i := rf.commitIndex + 1; i <= len(rf.logs); i++ {
						rf.CallerApplyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[i-1].Command,
							CommandIndex: rf.logs[i-1].Index,
						}
					}
					rf.commitIndex = len(rf.logs)
				}
			}
			reply.Success = true
			// fmt.Printf("in %v, log = %v\n", rf.me, rf.logs)
			return
		}
		if len(rf.logs) < args.PrevLogIndex {
			reply.Success = false
			return
		}
		if rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			rf.logs = rf.logs[:args.PrevLogIndex-1]
			reply.Success = false
			return
		}
		var i int
		for i = args.PrevLogIndex; i < args.PrevLogIndex+len(args.Entries) && i < len(rf.logs); i++ {
			if rf.logs[i].Term != args.Entries[i-args.PrevLogIndex].Term {
				break
			}
		}
		rf.logs = rf.logs[:i]
		for _, log := range args.Entries[i-args.PrevLogIndex:] {
			rf.logs = append(rf.logs, log)
			//rf.CallerApplyCh <- ApplyMsg{
			//	CommandValid: true,
			//	CommandIndex: len(rf.logs),
			//	Command:      log.Command,
			//}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.logs) {
				for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
					rf.CallerApplyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[i-1].Command,
						CommandIndex: rf.logs[i-1].Index,
					}
				}
				rf.commitIndex = args.LeaderCommit
			} else {
				// TODO: 是否要在之前的操作中把那个玩意给干掉。
				for i := rf.commitIndex + 1; i <= len(rf.logs); i++ {
					rf.CallerApplyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[i-1].Command,
						CommandIndex: rf.logs[i-1].Index,
					}
				}
				rf.commitIndex = len(rf.logs)
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

func (rf *Raft) convertToFollowerNoneLock(newTerm int) {
	DPrintf("[%v][%v] become follower with new term %v\n", rf.me, rf.term, newTerm)
	rf.term = newTerm
	rf.state = Follower
	rf.hasVoted = false
	rf.getMsg = true
	rf.voteGet = 0
}

// example RequestVote RPC handler.
// this func is to solve request from peer.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// below is for 2A
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
					DPrintf("[%v][%v] deny %v's vote request for last log term is not at least up-to-date\n", rf.me, rf.term, args.CandidateId)
				} else if args.LastLogTerm == rf.getLastLogTermNoneLock() {
					if len(rf.logs) > args.LastLogIndex {
						reply.Voted = false
						DPrintf("[%v][%v] deny %v's vote request for same term but log is not longer\n", rf.me, rf.term, args.CandidateId)
						// fmt.Println("deny!!")
					} else {
						DPrintf("[%v][%v] vote for %v\n", rf.me, rf.term, args.CandidateId)
						rf.hasVoted = true
						reply.Voted = true
					}
				} else {
					DPrintf("[%v][%v] vote for %v\n", rf.me, rf.term, args.CandidateId)
					rf.hasVoted = true
					reply.Voted = true
				}
			} else {
				// fmt.Println("deny!!")
				DPrintf("[%v][%v] deny for %v because of hasBeen voted\n", rf.me, rf.term, args.CandidateId)
				reply.Voted = false
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

//	func (rf *Raft) HeartBeat(args *RequestAppendEntries, reply *ReplyAppendEntries) {
//		// THINK: 状态是否会转变？
//		rf.mu.Lock()
//		defer rf.mu.Unlock()
//		if args.Term < rf.term {
//			reply.Success = false
//		}
//		if args.Term > rf.term {
//			rf.convertToFollowerNoneLock(args.Term)
//			reply.Success = true
//		}
//		// TODO: May Be BUG
//		//if rf.state == Leader {
//		//	panic("two leader!! split brain!!")
//		//}
//		if rf.state == Candidate {
//			rf.convertToFollowerNoneLock(args.Term)
//			reply.Success = true
//		}
//		rf.getMsg = true
//		reply.Term = rf.term
//		return
//	}
func (rf *Raft) onePeerOneChannel(peerId int, term int) {
	// lazy, but heartBeat can start it
	rf.mu.Lock()
	ch := rf.applyCh[peerId]
	rf.mu.Unlock()
	for msg := range ch {
		// TODO: 在本地生成msg,这样子就可以做到批量发送了
		switch msg.Msg {
		case Normal:
			{
				if !rf.lockWithCheckForLeader(term) {
					return
				}
				if rf.nextIndex[peerId] == len(rf.logs)+1 {
					rf.mu.Unlock()
					continue
				}
				entries := rf.logs[rf.nextIndex[peerId]-1:]
				var prevTerm = 0
				prevIndex := rf.nextIndex[peerId] - 1
				if prevIndex > 0 {
					prevTerm = rf.logs[prevIndex-1].Term
				}
				msg.Req = RequestAppendEntries{
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
					Term:         rf.term,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					// TODO: 要做redirect吗?
					LeaderId: rf.me,
				}
				rf.mu.Unlock()
				break
			}
		case Quit:
			{
				return
			}
		case HeartBeat:
			{
				// 生成heart Beat操作
				if !rf.lockWithCheckForLeader(term) {
					return
				}
				req, err := rf.getHeartBeatMsgNoneLock(peerId)
				if err != nil {
					rf.mu.Unlock()
					return
				}
				msg.Req = req
				rf.mu.Unlock()
				break
			}
		}
		if rf.killed() || !rf.isLeaderWithLock() {
			return
		}
		var reply ReplyAppendEntries
		res := false
		for res == false {
			if !rf.lockWithCheckForLeader(term) {
				return
			}
			msg.Req.LeaderCommit = rf.commitIndex
			DPrintf("[%v][%v] try send append req to %v, with req = %v\n", rf.me, rf.term, peerId, msg)
			rf.mu.Unlock()
			reply = ReplyAppendEntries{}
			res = rf.peers[peerId].Call("Raft.AppendEntries", &msg.Req, &reply)
		}
		DPrintf("[%v][%v] get reply = %v\n", rf.me, rf.term, reply)
		rf.onGetMsgWithLock()
		for reply.Success == false {
			// decrease
			if !rf.lockWithCheckForLeader(term) {
				return
			}
			if reply.Term > rf.term {
				rf.convertToFollowerNoneLock(reply.Term)
				rf.mu.Unlock()
				return
			}
			if reply.Term < rf.term {
				rf.mu.Unlock()
				break
			}
			// fmt.Printf("%v now term is %v\n", rf.me, rf.term)
			// fmt.Printf("%v nextIndex[%v] = %v\n", rf.me, peerId, rf.nextIndex[peerId])
			rf.nextIndex[peerId] -= 1
			DPrintf("[%v][%v] decrease the nextIndex of %v\n", rf.me, rf.term, peerId)
			hReq, err := rf.getHeartBeatMsgNoneLock(peerId)
			// fmt.Println(hReq)
			if err != nil {
				rf.mu.Unlock()
				return
			}
			msg.Req.Term = hReq.Term
			msg.Req.PrevLogTerm = hReq.PrevLogTerm
			msg.Req.PrevLogIndex = hReq.PrevLogIndex
			msg.Req.LeaderId = rf.me
			msg.Req.LeaderCommit = hReq.LeaderCommit
			// fmt.Printf("%v's log %v\n", rf.me, rf.logs)
			msg.Req.Entries = append([]Entry{rf.logs[rf.nextIndex[peerId]-1]}, msg.Req.Entries...)
			//fmt.Println(msg)
			rf.mu.Unlock()
			res = false
			for res == false {
				if !rf.lockWithCheckForLeader(term) {
					return
				}
				msg.Req.LeaderCommit = rf.commitIndex
				DPrintf("[%v][%v] try send append req to %v, with req = %v\n", rf.me, rf.term, peerId, msg)
				rf.mu.Unlock()
				//fmt.Println("in line 687")
				reply = ReplyAppendEntries{}
				res = rf.peers[peerId].Call("Raft.AppendEntries", &msg.Req, &reply)
			}
			// fmt.Println("ok")
			rf.onGetMsgWithLock()
		}
		if !rf.lockWithCheckForLeader(term) {
			return
		}
		if reply.Term > rf.term {
			rf.convertToFollowerNoneLock(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Term < rf.term {
			rf.mu.Unlock()
			continue
		}

		// fmt.Printf("LEADER %v's log %v\n", rf.me, rf.logs)
		rf.matchIndex[peerId] = msg.Req.PrevLogIndex + len(msg.Req.Entries)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
		DPrintf("[%v][%v] receive success append apply from %v, nextIndex = %v, matchIndex = %v\n", rf.me, rf.term, peerId, rf.nextIndex[peerId], rf.matchIndex[peerId])
		// 找到最低的那个， 二分，线性
		// 二分答案：有点懒得写。。。
		// 线性优化：从最高的开始找，尝试找到最大的那个，最高的开始也容易找到与当前term相同的,找到就可以直接break了
		// 如果n到了commit_index，那就润喽
		// 这边先写线性
		for n := len(rf.logs); n > rf.commitIndex; n-- {
			cnt := 0
			for _, idx := range rf.matchIndex {
				if idx >= n {
					cnt++
				}
			}
			if cnt >= (len(rf.peers)+1)/2 && rf.logs[n-1].Term == rf.term {
				for i := rf.commitIndex + 1; i <= n; i++ {
					DPrintf("[%v][%v] commit log = %v\n", rf.me, rf.term, rf.logs[i-1])
					rf.CallerApplyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[i-1].Command,
						CommandIndex: i,
					}
				}
				rf.commitIndex = n
				break
			}
		}
		// TODO: 二分答案优化, 如果entries的量很大，那确实二分是有点意义的，但是如果100左右，那其实没有任何优化的价值。
		// TODO: 有一种可能，就是在首次同步的时候，如果有很多entry，比如说几万个，那二分确实有那么一点点价值
		// TODO: raft其实还有很多可以优化的地方。比如log同步时也可以使用二分，不过这就要改figure2中的实现描述了，这在网络差的情况下较好
		// TODO: 现在的问题是不理解raft中日志同步的数量。
		// 还有一种可能，就是heartbeat回来的消息寄了，那么这种情况，heartbeat其实应该继续发送。
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
	_ = rf.peers[server].Call("Raft.RequestVote", args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.term {
		return
	}
	if reply.Term > rf.term {
		rf.convertToFollowerNoneLock(reply.Term)
		return
	}
	if rf.term != term {
		return
	}
	if reply.Voted {
		if rf.state == Candidate {
			rf.voteGet += 1
		} else {
			DPrintf("[%v][%v] get vote, however state is not candidate\n", rf.me, rf.term)
		}
	}
	return
}
func (rf *Raft) sendHeartBeatLoop(term int) {
	for rf.killed() == false {
		if !rf.isLeaderWithLock() {
			return
		}
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			if !rf.lockWithCheckForLeader(term) {
				return
			}
			_, err := rf.getHeartBeatMsgNoneLock(idx)
			if err != nil {
				rf.mu.Unlock()
				return
			}
			hb := reqWarp{Req: RequestAppendEntries{}, Msg: HeartBeat}
			rf.applyCh[idx] <- hb
			rf.mu.Unlock()
		}
		ms := 100 + (rand.Int63() % 15)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return
}

//	func (rf *Raft) sendHeartBeatLoopNoneLock(term int) {
//		for rf.killed() == false {
//			if rf.state != Leader {
//				return
//			}
//			for idx, _ := range rf.peers {
//				if idx == rf.me {
//					continue
//				}
//				if !rf.lockWithCheckForLeader(term) {
//					return
//				}
//				_, err := rf.getHeartBeatMsgNoneLock(idx)
//				if err != nil {
//					rf.mu.Unlock()
//					return
//				}
//				hb := reqWarp{Req: RequestAppendEntries{}, Msg: HeartBeat}
//				rf.applyCh[idx] <- hb
//				rf.mu.Unlock()
//			}
//			ms := 100 + (rand.Int63() % 10)
//			time.Sleep(time.Duration(ms) * time.Millisecond)
//		}
//		return
//	}
func (rf *Raft) requestAppendEntries(command interface{}, peerId int) {

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
	DPrintf("[%v][%v] get a command = %v\n", rf.me, rf.term, command)
	defer rf.mu.Unlock()
	defer DPrintf("[%v][%v] logs = %v\n", rf.me, rf.term, rf.logs)
	isLeader = rf.state == Leader
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.logs = append(rf.logs, Entry{
		Term:    rf.term,
		Index:   len(rf.logs) + 1,
		Command: command,
	})
	rf.matchIndex[rf.me] = len(rf.logs)
	//rf.CallerApplyCh <- ApplyMsg{
	//	CommandValid: true,
	//	CommandIndex: len(rf.logs),
	//	Command:      command,
	//}
	index = len(rf.logs)
	term = rf.term
	// TODO: start to append log
	// first, 检查所有peer的nextIndex，查看是否比较落后
	// 如果不是最新的，那么就直接发送一个msg，进行复制操作
	for idx, nextIdx := range rf.nextIndex {
		if idx == rf.me {
			continue
		}
		if nextIdx <= len(rf.logs) {
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

	// Your code here, if desired.
	// graceful shutdown
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

//	func (rf *Raft) sendAppendEntriesLoop() {
//		for msg := range rf.sendReqChan {
//			if msg.Msg == Quit {
//				return
//			}
//			if rf.killed() || !rf.isLeaderWithLock() {
//				return
//			}
//			go func(req RequestAppendEntries) {
//				for idx, _ := range rf.peers {
//					if idx == rf.me {
//						continue
//					}
//				}
//			}(msg.Req)
//		}
//	}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		rf.mu.Lock()
		//fmt.Printf("%v tick\n", rf.me)
		if rf.needNextElectionNoneLock() {
			for {
				rf.becomeCandidateNoneLock()
				rf.sendVoteReqToAllPeerNoneLock()
				rf.mu.Unlock()
				finish := false
				newTimeout := 150 + (rand.Int63() % 300)
				var cost int64 = 0
				for {
					rf.mu.Lock()
					if rf.killed() {
						rf.mu.Unlock()
						return
					}
					if rf.state == Candidate {
						if rf.voteGet >= (len(rf.peers)+1)/2 {
							rf.state = Leader
							rf.convertToLeaderConfigNoneLock()
							go rf.sendHeartBeatLoop(rf.term)
							finish = true
							//DPrintf("[%v][%v] become leader\n", rf.me, rf.term)
							break
						}
					} else if rf.state == Follower {
						finish = true
						break
					} else {
						panic("leader???")
					}
					rf.mu.Unlock()
					// 10毫秒检测一次
					time.Sleep(time.Duration(10) * time.Millisecond)
					cost += 10
					if rf.killed() {
						return
					}
					if cost > newTimeout {
						break
					}
				}
				if finish {
					break
				}
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
		ms := 150 + (rand.Int63() % 300)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.applyCh = []chan reqWarp{}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
