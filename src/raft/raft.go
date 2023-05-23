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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state, 暂时是一把大锁保平安
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// below is (2A)
	term    int   // current term, or latest term server has seen. initial is 0 on first boot
	state   State // current state, [leader, follower, candidate]
	voteGet int   // when in candidate, and only in candidate state, this value is valid, means vote get from follower
	hasVote bool  // only when in follower state is valid. every time when term update, the hasVote will reset to false
	getMsg  bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
type State int

const (
	Leader    State = 0
	Follower  State = 1
	Candidate State = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func
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
func (rf *Raft) isLeader() bool {
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
func (rf *Raft) onGetVote() int {
	var res int
	rf.mu.Lock()
	rf.voteGet += 1
	res = rf.voteGet
	rf.mu.Unlock()
	return res
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
}
type ReplyAppendEntries struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
// this func is to solve request from peer.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// below is for 2A
	// step0: assert that mine state is candidate
	//if !rf.isCandidate() {
	//	panic("when make vote, the raft must be candidate")
	//}
	// step1: make vote
}
func (rf *Raft) HeartBeat(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	rf.mu.Lock()
	// update the time
	rf.getMsg = true
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.Success = false
	}
	rf.mu.Unlock()
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan ElectionMsg) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// TODO: 判断的过程要用一把大锁，给锁起来
	// TODO: 如果已经是leader了，那就其实代表ch已经关闭了，不应该再送消息进去了，而是直接退出
	// TODO: 还需要返回值term的有效性，如果返回值的term小于当前term，那么消息就会被丢弃。
	// TODO: 如果term大于当前term，那么term变为最新的term，并将当前的状态转换为follower, 并发送一条变为Follower的消息。同时已投票标记记为false
	// TODO: 当且仅当term相同，且为candidate时候，才会将票数结果传递。
	// TODO: 注意判断的顺序
	// TODO:
	return ok
}
func (rf *Raft) sendHeartBeatLoop() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for idx, peer := range rf.peers {
			if idx == rf.me {
				// 不能给自己发送
				continue
			}
			go func(peer *labrpc.ClientEnd) {
				var reply ReplyAppendEntries
				_ = peer.Call("Raft.HeartBeat", RequestAppendEntries{
					Term:         rf.term,
					LeaderId:     rf.me,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
				}, &reply)
				// TODO: need to solve false rpc call?
			}(peer)
		}
		rf.mu.Unlock()
		ms := 100 + (rand.Int63() % 20)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return
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

	// Your code here (2B).

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) beginElection(ch chan ElectionMsg) {
	rf.mu.Lock()
	rf.state = Candidate
	rf.term += 1
	rf.voteGet = 1 // vote for self
	rf.mu.Unlock()
	for idx, peer := range rf.peers {
		if rf.me == idx {
			continue
		}
		// TODO: 做好现有entry的准备，为之后的2B做准备
	}
	for res := range ch {
		switch res {
		case GetVote:
			{
				nowVoteGet := rf.onGetVote()
				if nowVoteGet > len(rf.peers)/2 {
					// switch to leader, and send header beat
					rf.mu.Lock()
					// 如果一瞬间重新开启了一轮，
					// TODO: 每次不重新开启新的beginElection，而是发送一个消息，让其重新开始，防止data race
					rf.state = Leader
					go rf.sendHeartBeatLoop()
					rf.mu.Unlock()
					return
				}
			}
		case Quit:
			{
				return
			}
		case BecomeFollower:
			{
				// TODO: 这里应该直接返回，此时最新的Term已经更新
			}
		}
	}
}

type ElectionMsg int

const (
	GetVote        ElectionMsg = 1
	Quit           ElectionMsg = 2
	BecomeFollower ElectionMsg = 3
)

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		if !rf.noNeedNextElection() {
			// 如果在这一瞬间变为了所谓的leader，那就寄了。所以这里应该是原子操作
			// TODO: 需要成为原子操作，稍后修改
			// TODO: 在begin election中会进行修改
		}
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

	// me是自己在整个系统中的下标
	// 在初始化的时候要注意不要与自己通信。
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
