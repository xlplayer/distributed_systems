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

import "math/rand"
import "time"
import "sync"
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"



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

//
// A Go object implementing a single Raft peer.
//

type State int
const(
	leader State = 0
	follower State = 1
	candidate State = 2
)

type LogEntry struct{
	Term int
	Command interface{}
}

type Timer struct{
	election_timeout time.Duration
	last_time time.Time
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex   []int
	matchIndex  []int

	//snapshot
	firstIndex  int
	applyCh		chan ApplyMsg

	leaderIdx   int//control leaderrun
	state State //leader,follower,candidate
	timer Timer
	applyCond *sync.Cond
}
func (rf *Raft) getLog(i int) LogEntry{
	return rf.log[i-rf.firstIndex]
}
func (rf *Raft) addIdx(i int) int {
    return rf.firstIndex + i
}

func (rf *Raft) subIdx(i int) int {
    return i - rf.firstIndex
}

func (rf *Raft) lastIdx() int {
    return rf.firstIndex + len(rf.log) - 1
}

func (rf *Raft) lastTerm() int {
    return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf("{%v}persist log{%v}",rf.me,rf.log)
}

func (rf *Raft) GetPersistState() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstIndex)
	state := w.Bytes()
	return state
}

func (rf *Raft) persistStatesAndSnapshot(rawSnapshot []byte){
	state := rf.GetPersistState()
	rf.persister.SaveStateAndSnapshot(state, rawSnapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var firstIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&firstIndex) != nil{
		panic("decode error!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.firstIndex = firstIndex
		rf.mu.Unlock()
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	OldTerm int
	Term int
	VoteGrantd bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get RequestVote from %v",rf.me,args.CandidateId)
	reply.OldTerm = args.Term
	reply.Term = rf.currentTerm
	reply.VoteGrantd = false

	if args.Term > rf.currentTerm{
		DPrintf("{%v} become follower in 190",rf.me)
		rf.state = follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}

	if args.Term >= rf.currentTerm && (rf.votedFor == -1||rf.votedFor == args.CandidateId){
		if (args.LastLogTerm > rf.lastTerm()) || (args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIdx()){
			reply.VoteGrantd = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.timer.last_time = time.Now()
		}	
	}
	DPrintf("%v vote for %v {%v}\n",rf.me, args.CandidateId, reply.VoteGrantd)
	if !reply.VoteGrantd{
		DPrintf("CandidateId:%v me:%v, votedFor:%v, args.Term >= rf.currentTerm:{%v}, args.LastLogTerm > rf.lastTerm(){%v}, args.LastLogTerm{%v}, rf.lastTerm(){%v}, args.LastLogIndex{%v},idx{%v}",args.CandidateId,rf.me,rf.votedFor,args.Term >= rf.currentTerm,args.LastLogTerm > rf.lastTerm(), args.LastLogTerm, rf.lastTerm(), args.LastLogIndex, rf.lastIdx())
	}
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
	//DPrintf("{{%v} want to start command {%v}}",rf.me, command)
	rf.mu.Lock()
	//DPrintf("{{%v} finished want to start command {%v}}",rf.me, command)
	defer rf.mu.Unlock()
	if rf.state == leader{
		DPrintf("{leader {%v} get New command {%v}}",rf.me, command)
		rf.log = append(rf.log,LogEntry{Term:rf.currentTerm,Command:command,})
		index = rf.lastIdx()
		term = rf.currentTerm
		rf.persist()
	}else{
		isLeader = false
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("%v is killed",rf.me)
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
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.state = follower
	rf.timer.last_time = time.Now()
	rf.timer.election_timeout = time.Millisecond*(time.Duration(300+rand.Int31()%500))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.firstIndex
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	go rf.FollowerRun()
	go rf.Apply()

	return rf
}
func (rf *Raft) Apply(){
	for !rf.killed(){
		rf.mu.Lock()
		// DPrintf("{%v} wait to apply",rf.me)
		rf.applyCond.Wait()
		// DPrintf("{%v} finish wait to apply",rf.me)
		var log []LogEntry
		lastApplied := rf.lastApplied
		for rf.lastApplied < rf.commitIndex{
			rf.lastApplied++
			log = append(log, rf.getLog(rf.lastApplied))
		}
		rf.mu.Unlock()
		//DPrintf("{%v} Apply Log:%v",rf.me,log)
		for idx, entry := range log{
			rf.applyCh <- ApplyMsg{
				CommandValid:true,
				Command:entry.Command,
				CommandIndex:idx+1+lastApplied,
			}
		}
		//time.Sleep(10*time.Millisecond)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries[]    LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	OldTerm int
	Term    int
	Success bool
	ConflictTerm int
	ConflictIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v receive AppendEntries from %v:%v,%v", rf.me, rf.votedFor,args,reply)
	reply.OldTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term >= rf.currentTerm{
		if args.LeaderId != rf.votedFor{
			DPrintf("{%v} become follower in 560",rf.me)
		}
		rf.state = follower
		rf.votedFor = args.LeaderId //shoud not be -1, or may cause more than one leader
		rf.timer.last_time = time.Now()
		rf.currentTerm = args.Term
		rf.persist()
	}

	if args.Term < rf.currentTerm{
		DPrintf("%v get heartbeat from %v {args.Term < rf.currentTerm, %v %v}",rf.me, args.LeaderId,args.Term,rf.currentTerm)
		return
	}
	if args.PreLogIndex < rf.firstIndex{
		reply.ConflictIndex = rf.firstIndex
		reply.ConflictTerm = -1
		return
	}

	if rf.lastIdx() < args.PreLogIndex{
		DPrintf("%v get heartbeat from %v {len(rf.log)-1 < args.PreLogIndex, %v %v}",rf.me, args.LeaderId,len(rf.log)-1,args.PreLogIndex)
		reply.ConflictIndex = rf.addIdx(len(rf.log))
		reply.ConflictTerm = -1
		return
	}
	if rf.getLog(args.PreLogIndex).Term != args.PreLogTerm{
		DPrintf("%v get heartbeat from %v {rf.log[args.PreLogIndex].Term != args.PreLogTerm, %v %v}",rf.me, args.LeaderId,rf.getLog(args.PreLogIndex).Term ,args.PreLogTerm)
		reply.ConflictTerm = rf.getLog(args.PreLogIndex).Term
		for idx,entry := range rf.log{
			if entry.Term == rf.getLog(args.PreLogIndex).Term{
				reply.ConflictIndex = rf.addIdx(idx)
				break
			}
		}
		return
	}

	for idx, entry := range args.Entries{
		cur := args.PreLogIndex+idx+1
		if cur<rf.addIdx(len(rf.log)) && rf.getLog(cur).Term != entry.Term{
			rf.log = rf.log[:rf.subIdx(cur)]
			rf.log = append(rf.log,entry)
			//rf.log[cur] = entry
		}else if cur>=rf.addIdx(len(rf.log)){
			rf.log = append(rf.log,entry)
		}
	}
	rf.persist()
	idxOfLastNewEntry := args.PreLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex{
		if args.LeaderCommit < idxOfLastNewEntry{
			rf.commitIndex = args.LeaderCommit
		}else{
			rf.commitIndex = idxOfLastNewEntry
		}
		DPrintf("{%v} commitindex {%v},idxOfLastNewEntry:%v,args.LeaderCommit:%v",rf.me, rf.commitIndex, idxOfLastNewEntry,args.LeaderCommit)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	DPrintf("%v get heartbeat from %v {%v, LeaderCommit:%v,commitindex:%v,log:%v}",rf.me, args.LeaderId, reply.Success, args.LeaderCommit, rf.commitIndex, rf.log)
}

func (rf *Raft) SaveSnapshot(index int, rawSnapshot []byte){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.firstIndex{
		return
	}
	DPrintf("{%v} %v %v",rf.me, index, rf.firstIndex)
	rf.log = rf.log[rf.subIdx(index):]
	rf.firstIndex = index
	DPrintf("{%v} %v %v",rf.me, index, rf.firstIndex)
	rf.persister.SaveStateAndSnapshot(rf.GetPersistState(),rawSnapshot)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data      []byte
}

type InstallSnapshotReply struct {
	Term 		int
	OldTerm 	int
}