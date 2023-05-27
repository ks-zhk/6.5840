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
	//	"bytes"
	"math/rand"
	"sync"
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
	Term  int
	Index int
	// only in leader is valid
	LogCount int
	Command  interface{}
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
	sendReqChan       chan reqWarp
	applyCh           []chan reqWarp
	// below if (2B)
	logs        []Entry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
type reqWarp struct {
	Req RequestAppendEntries
	Msg Msg
}

func (rf *Raft) finishLastHeartWithLock(peerId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeatOver[peerId] = true
}
func (rf *Raft) getPrevInfoByPeerIdxNoneLock(peerId int) (prevIndex int, prevTerm int) {
	if rf.nextIndex[peerId] == 1 {
		prevIndex = -1
		prevTerm = -1
		return
	} else {
		prevIndex = rf.nextIndex[peerId] - 1
		prevTerm = rf.logs[prevIndex-1].Term
		return
	}
}
func (rf *Raft) initOfAllServerNoneLock() {
	rf.commitIndex = 0
	rf.lastApplied = 0
}
func (rf *Raft) convertToLeaderConfigNoneLock() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.logs) + 1
		rf.matchIndex[i] = 0
		rf.lastHeartBeatOver[i] = true
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
	// Your code here (2A). ok
	rf.mu.Lock()
	term = rf.term
	isLeader = rf.state == Leader
	rf.mu.Unlock()
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
func (rf *Raft) needNextElection() bool {
	var res bool
	res = rf.getMsg
	rf.getMsg = false
	return res == false || rf.state == Leader
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
		LastLogTerm:  -1,
		LastLogIndex: -1,
	}
}
func (rf *Raft) getHeartBeatMsgNoneLock(peerId int) (RequestAppendEntries, error) {
	if rf.state != Leader {
		return RequestAppendEntries{}, errors.New("raft is not leader")
	}
	prevLogTerm, prevLogIndex := rf.getPrevInfoByPeerIdxNoneLock(peerId)
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
		go func(req RequestVoteArgs, reply RequestVoteReply, idx int) {
			rf.sendRequestVote(idx, &req, &reply)
		}(req, reply, idx)
	}
}
func (rf *Raft) becomeCandidateNoneLock() {
	rf.term += 1
	rf.state = Candidate
	rf.getMsg = false
	rf.hasVoted = true
	rf.voteGet = 1
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
	if rf.state == Follower {
		// 特殊情况特殊讨论
		if args.PrevLogIndex == -1 {
			// 最特殊的情况, 直接无脑加log即可
			for _, log := range args.Entries {
				rf.logs = append(rf.logs, log)
			}
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < len(rf.logs) {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.logs)
				}
			}
			reply.Success = true
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
		follow := len(rf.logs) - args.PrevLogIndex
		reply.Success = true
		if len(args.Entries) > follow {
			for i := follow; i < len(args.Entries); i++ {
				rf.logs = append(rf.logs, args.Entries[i])
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.logs) {
				rf.commitIndex = args.LeaderCommit
			} else {
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
	return
}

func (rf *Raft) convertToFollowerNoneLock(newTerm int) {
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
	// TODO: 2B, about LOG Entry
	switch rf.state {
	case Follower:
		{
			// 先来先到原则
			if !rf.hasVoted {
				rf.hasVoted = true
				reply.Voted = true
			} else {
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
func (rf *Raft) HeartBeat(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	// THINK: 状态是否会转变？
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Success = false
	}
	if args.Term > rf.term {
		rf.convertToFollowerNoneLock(args.Term)
		reply.Success = true
	}
	// TODO: May Be BUG
	//if rf.state == Leader {
	//	panic("two leader!! split brain!!")
	//}
	if rf.state == Candidate {
		rf.convertToFollowerNoneLock(args.Term)
		reply.Success = true
	}
	rf.getMsg = true
	reply.Term = rf.term
	return
}
func (rf *Raft) onePeerOneChannel(peerId int) {
	// lazy, but heartBeat can start it
	for msg := range rf.applyCh[peerId] {
		// 如果是退出信号，那就直接润
		if msg.Msg == Quit {
			return
		}
		if rf.killed() || !rf.isLeaderWithLock() {
			return
		}
		var reply ReplyAppendEntries
		res := false
		for res == false {
			res = rf.peers[peerId].Call("Raft.AppendEntries", &msg.Req, &reply)
		}
		for reply.Success == false {
			// decrease
			if rf.killed() || !rf.isLeaderWithLock() {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.term {
				rf.convertToFollowerNoneLock(reply.Term)
				rf.mu.Unlock()
				return
			}
			rf.nextIndex[peerId] -= 1
			hReq, err := rf.getHeartBeatMsgNoneLock(peerId)
			if err != nil {
				rf.mu.Unlock()
				return
			}
			msg.Req.Term = hReq.Term
			msg.Req.PrevLogTerm = hReq.PrevLogTerm
			msg.Req.PrevLogIndex = hReq.PrevLogIndex
			msg.Req.LeaderId = rf.me
			msg.Req.LeaderCommit = hReq.LeaderCommit
			rf.mu.Unlock()
			res = false
			for res == false {
				res = rf.peers[peerId].Call("Raft.AppendEntries", &msg.Req, &reply)
			}
		}
		// success
		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.convertToFollowerNoneLock(reply.Term)
			rf.mu.Unlock()
			return
		}
		for _, entry := range msg.Req.Entries {
			rf.logs[entry.Index-1].LogCount += 1
			if rf.logs[entry.Index-1].LogCount == (len(rf.peers)+1)/2 {
				rf.commitIndex = entry.Index
			}
		}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	_ = rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.term {
		return
	}
	if reply.Term > rf.term {
		rf.convertToFollowerNoneLock(reply.Term)
		return
	}
	if reply.Voted {
		if rf.state == Candidate {
			rf.voteGet += 1
		} else {
			DPrintf("get vote, however state is not candidate")
		}
	}
	return
}
func (rf *Raft) sendHeartBeatLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for idx, peer := range rf.peers {
			if idx == rf.me || rf.lastHeartBeatOver[idx] == false {
				continue
			}
			rf.lastHeartBeatOver[idx] = false
			go func(peer *labrpc.ClientEnd, rf *Raft, peerId int) {
				defer rf.finishLastHeartWithLock(peerId)

				var reply ReplyAppendEntries
				rf.mu.Lock()
				req, err := rf.getHeartBeatMsgNoneLock(peerId)
				rf.mu.Unlock()
				if err != nil {
					DPrintf("error = %v", err)
					return
				}
				_ = peer.Call("Raft.AppendEntries", &req, &reply)
				// TODO: (2B)solve the false, decrease the nextIndex
				for reply.Success == false {
					// 如果已经被删了或者不是leader了，直接润喽
					if rf.killed() || !rf.isLeaderWithLock() {
						return
					}
					rf.mu.Lock()
					rf.nextIndex[peerId] = rf.nextIndex[peerId] - 1
					req, err := rf.getHeartBeatMsgNoneLock(peerId)
					rf.mu.Unlock()
					if err != nil {
						DPrintf("error = %v", err)
						return
					}
					var res = false
					// 重复发送，但是容易陷入死循环，因此每次都会判断一波
					for res == false {
						if rf.killed() || !rf.isLeaderWithLock() {
							return
						}
						res = peer.Call("Raft.AppendEntries", &req, &reply)
					}
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term < rf.term {
					return
				}
				if reply.Term > rf.term {
					rf.convertToFollowerNoneLock(reply.Term)
				}
				return
			}(peer, rf, idx)
		}
		rf.mu.Unlock()
		ms := 100 + (rand.Int63() % 20)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return
}
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
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.logs = append(rf.logs, Entry{
		Term:     rf.term,
		Index:    len(rf.logs) + 1,
		Command:  command,
		LogCount: 1,
	})
	index = len(rf.logs)
	term = rf.term
	// TODO: start to append log

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
	rf.sendReqChan <- reqWarp{
		Req: RequestAppendEntries{},
		Msg: Quit,
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

func (rf *Raft) sendAppendEntriesLoop() {
	for msg := range rf.sendReqChan {
		if msg.Msg == Quit {
			return
		}
		if rf.killed() || !rf.isLeaderWithLock() {
			return
		}
		go func(req RequestAppendEntries) {
			for idx, _ := range rf.peers {
				if idx == rf.me {
					continue
				}
			}
		}(msg.Req)
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		rf.mu.Lock()
		if rf.needNextElection() {
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
						if rf.voteGet > len(rf.peers)/2 {
							rf.state = Leader
							rf.convertToLeaderConfigNoneLock()
							go rf.sendHeartBeatLoop()
							finish = true
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
	for _, _ = range peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.lastHeartBeatOver = append(rf.lastHeartBeatOver, true)
	}
	rf.commitIndex = 0
	rf.state = Follower
	rf.getMsg = false
	rf.voteGet = 0
	rf.hasVoted = false
	rf.logs = []Entry{}
	rf.term = 0
	rf.dead = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
