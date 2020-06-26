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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mit/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER

	HEARTBEAT_INTERVAL    = 100
	MIN_ELECTION_INTERVAL = 400
	MAX_ELECTION_INTERVAL = 500
)

//
// A Go object implementing a single Raft peer.
//
type entry struct {
	Term     int
	LogIndex int
	LogEntry interface{}
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int
	state       int
	votedFor    int
	leaderId    int
	log         []entry
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// record time duration
	electionTimer     *time.Timer   // election timer
	electionTimeout   time.Duration // 200~400ms
	heartbeatInterval time.Duration // 50ms

}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entrys       []entry
	LeaderCommit int //leader's commit index
}
type AppendEntryReply struct {
	Sucess bool
	Term   int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//很多功能还未完善，只可初步用于选举阶段
	rf.resetTimer()
	DEBUG(HEARTBEAT, "[peer=%d] recive heartbeat mesage\n", rf.me)
	reply.Sucess = true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here (2A).
	rf.mu.Lock()

	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}
	rf.mu.Unlock()

	return term, isleader
}
func (rf *Raft) convert(state int) {
	switch state {
	case LEADER:

	}

}
func (rf *Raft) sendAllPeerBeatEntry() {
	peerSize := len(rf.peers)
	for i := 0; i < peerSize; i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntryArgs{
			Term: rf.currentTerm,
		}
		voteReply := AppendEntryReply{}
		if rf.state == LEADER {
			go func(peerId int) {
				rf.peers[peerId].Call("Raft.AppendEntry", &args, &voteReply)
			}(i)
		} else {
			return
		}
	}
}
func (rf *Raft) heartBeat() {
	for {
		if rf.state != LEADER {
			return
		}
		DEBUG(HEARTBEAT, "[peer=%d] is send heartbeat message\n", rf.me)

		rf.sendAllPeerBeatEntry()

		time.Sleep(rf.heartbeatInterval)
	}
}
func (rf *Raft) resetTimer() {
	DEBUG(TIMER, "[peer=%d] reset timer\n", rf.me)
	rf.electionTimer.Stop()
	//rf.electionTimeout = time.Duration(rand.Intn(10)) * time.Millisecond
	rf.electionTimer = time.AfterFunc(rf.electionTimeout, rf.candidateResquestVote)
}
func (rf *Raft) stopTimer() {
	DEBUG(TIMER, "[peer=%d] stop timer\n", rf.me)
	rf.electionTimer.Stop()

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int // used when not vote, return self term if candidate is out of date.
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.state == FOLLOWER {

		if args.CandidateTerm > rf.currentTerm {
			rf.currentTerm = args.CandidateTerm
			rf.votedFor = -1
		}
		if rf.votedFor == -1 && args.CandidateTerm >= rf.currentTerm && args.LastLogIndex >= (len(rf.log)-1) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
		} else {
			reply.VoteGranted = false
		}
		rf.mu.Unlock()
		reply.CurrentTerm = rf.currentTerm
		DEBUG(ElectionState, "vote: peers: %d vote to peers: %d, and result is %v\n", rf.me, args.CandidateID, reply.VoteGranted)
	} else {
		rf.mu.Unlock()
		reply.VoteGranted = false
	}

	// Your code here (2A, 2B).
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) candidateResquestVote() {
	rf.resetTimer()
	DEBUG(ElectionState, "vote-req: peer %d is request vote\n", rf.me)
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	peerSize := len(rf.peers)
	voteArg := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateID:   rf.me,
		LastLogIndex:  len(rf.log),
		LastLogTerm:   rf.log[len(rf.log)-1].Term,
	}
	agree := 1
	disagree := 0
	replyHandler := func(reply *RequestVoteReply, peer int, currentTerm int) {
		if rf.state == LEADER {
			return
		}
		rf.mu.Lock()
		DEBUG(ElectionState, "recive-message: peer:%d recive peer:%d 's vote-%v\n", rf.me, peer, reply.VoteGranted)
		defer rf.mu.Unlock()
		if reply.VoteGranted == true && rf.state == CANDIDATE {
			agree++
			if agree > peer_size/2 {
				rf.state = LEADER
				rf.stopTimer()
				go rf.heartBeat()
				// 开始心跳
				fmt.Printf("peer: %d is leader and begin heartbeat\n", rf.me)
				//go rf.AppendEntry()
				return
			}
		} else {
			disagree++
			if disagree > peer_size/2 {
				rf.state = FOLLOWER
				rf.votedFor = -1
			}
			if reply.CurrentTerm > rf.currentTerm {
				rf.currentTerm = reply.CurrentTerm
				rf.state = FOLLOWER
				return
			}
		}
	}
	currentTerm := rf.currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < peerSize; i++ {
		if i == rf.me {
			continue
		}
		voteReply := RequestVoteReply{}
		if rf.state == CANDIDATE {
			go func(peerID int) {
				rf.sendRequestVote(peerID, &voteArg, &voteReply)
				replyHandler(&voteReply, peerID, currentTerm)
			}(i)
		} else {
			return
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.log = make([]entry, 0)
	rf.log = append(rf.log, entry{Term: rf.currentTerm, LogIndex: 0})
	rf.heartbeatInterval = time.Duration(50) * time.Millisecond

	rf.electionTimeout = time.Duration(100+rand.Intn(100)) * time.Millisecond
	fmt.Printf("[peer=%d]'s sleeptime is %v\n", rf.me, rf.electionTimeout)
	rf.electionTimer = time.AfterFunc(rf.electionTimeout, rf.candidateResquestVote)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//rf.candidateResquestVote()

	return rf
}
