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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Payload ApplyMsg
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm  int           // latest term seen by server
	votedFor     int           // voted for whom
	state        int           // am I follower or candidate or leader?
	clockCounter int           // for timing out for elections
	log          []LogEntry    // log of entries
	commitIndex  int           // last commited entry
	nextIndex    []int         // index of next entry to send
	matchIndex   []int         // index where commit is known
	applyCh      chan ApplyMsg // to send up
	HBL          int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == 2)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("OnO")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	theirTerm := args.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if theirTerm > rf.currentTerm {
		rf.currentTerm = theirTerm
		rf.state = 0
		rf.votedFor = -1
		rf.HBL = 0
	}
	if (rf.state == 0) && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (theirTerm >= rf.currentTerm) && ((len(rf.log) == 0) || (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (args.LastLogIndex >= len(rf.log)))) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the same for appendEntries...
type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	HBL          int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	theirTerm := args.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if theirTerm >= rf.currentTerm {
		rf.currentTerm = theirTerm
		if rf.state != 0 {
			rf.state = 0
			rf.HBL = 0
		}
		if theirTerm != rf.currentTerm {
			rf.votedFor = -1
		}
		if (args.PrevLogIndex > len(rf.log)) || (args.PrevLogIndex > 0) && (args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
			reply.Success = false
		} else {
			var newlog []LogEntry
			newlog = append(newlog, rf.log...)
			newlog = append(newlog[:args.PrevLogIndex], args.Entries...)
			// if len(newlog) < len(rf.log) {
			// 	fmt.Printf("id = %v\nOLDLOG : ", rf.me)
			// 	for _, b := range rf.log {
			// 		fmt.Printf("%v (t=%v), ", b.Payload.Command, b.Term)
			// 	}
			// 	fmt.Print("\nNEWLOG : ")
			// 	for _, b := range newlog {
			// 		fmt.Printf("%v (t=%v), ", b.Payload.Command, b.Term)
			// 	}
			// 	fmt.Printf("\n")
			// 	fmt.Printf("heartbeatlevels: old %v new %v")
			// }
			if rf.HBL <= args.HBL {
				rf.log = newlog
				rf.HBL = args.HBL
			}
			rf.persist()
			for (rf.commitIndex < args.LeaderCommit) && (rf.commitIndex < len(rf.log)) {
				rf.commitIndex++
				rf.applyCh <- rf.log[rf.commitIndex-1].Payload
				if rf.commitIndex != rf.log[rf.commitIndex-1].Payload.CommandIndex {
					fmt.Printf("HUH??(follower)\n")
				}
				rf.persist()
			}
			reply.Success = true
		}
	} else {
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	rf.clockCounter = (60 + rand.Int()%20)
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = (rf.state == 2)
	if isLeader {
		rf.log = append(rf.log, LogEntry{ApplyMsg{true, command, index}, term})
		rf.clockCounter = 20
		// fmt.Printf("Log of id leader %v --> ", rf.me)
		// for _, x := range rf.log {
		// 	fmt.Printf("[%v __ %v] ", x.Payload.Command, x.Term)
		// }
		// fmt.Print("\n")
		rf.persist()
		rf.HBL++
		go rf.sendHeartbeat(rf.HBL)
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) doElectcion() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	votes := make([]int, len(rf.peers))
	votes[rf.me] = 1
	abortElection := false
	rfct := rf.currentTerm
	rfme := rf.me
	rflli := len(rf.log)
	var rfllt int
	if rflli > 0 {
		rfllt = rf.log[rflli-1].Term
	} else {
		rfllt = 0
	}
	electionDone := false
	for i := range rf.peers {
		if i == rfme {
			continue
		}
		go func(i int) {
			ok := false
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()
			args := &RequestVoteArgs{rfct, rfme, rflli, rfllt}
			reply := &RequestVoteReply{0, false}
			for !ok {
				if rf.killed() {
					return
				}
				if abortElection || (rf.state != 1) || (rfct != rf.currentTerm) {
					return
				}
				rf.mu.Unlock()
				ok = rf.sendRequestVote(i, args, reply)
				rf.mu.Lock()
			}
			if reply.VoteGranted {
				votes[i] = 1
				vtc := 0
				for _, x := range votes {
					vtc += x
				}
				if (2*vtc > len(rf.peers)) && (rf.state == 1) && (rfct == rf.currentTerm) && !electionDone {
					// fmt.Printf("Leader, id = %v, term = %v\n", rf.me, rf.currentTerm)
					rf.state = 2
					rf.clockCounter = 1
					rf.HBL = 0
					rf.nextIndex = make([]int, len(rf.peers))
					for k := range rf.nextIndex {
						rf.nextIndex[k] = len(rf.log) + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for k := range rf.nextIndex {
						rf.matchIndex[k] = 0
					}
					electionDone = true
					// fmt.Printf("%v win, li %v, lt %v, voted by : ", rf.me, rflli, rfllt)
					// for a, b := range votes {
					// 	if b == 1 {
					// 		fmt.Printf("%v, ", a)
					// 	}
					// }
					// fmt.Print("\n")
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = 0
				rf.HBL = 0
				abortElection = true
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeat(rfhbl int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rfct := rf.currentTerm
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()
			accepted := false
			for !accepted {
				var args *AppendEntriesArgs
				if rf.nextIndex[i] > 1 {
					args = &AppendEntriesArgs{rfct, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-2].Term, rf.log[rf.nextIndex[i]-1:], rf.commitIndex, rfhbl}
				} else {
					args = &AppendEntriesArgs{rfct, 0, 0, rf.log, rf.commitIndex, rfhbl}
				}
				reply := &AppendEntriesReply{0, false}
				// fmt.Printf("sending append request from %v to %v and leCo %v with entries length %v\n", rf.me, i, rf.commitIndex, len(args.Entries))
				// for _, b := range args.Entries {
				// 	fmt.Printf("%v, ", b.Payload.Command)
				// }
				// fmt.Printf("\n")
				ok := false
				for !ok {
					if rf.killed() {
						return
					}
					if rfct != rf.currentTerm {
						return
					}
					rf.mu.Unlock()
					ok = rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()
				}
				accepted = reply.Success
				// fmt.Printf("%v\n", accepted)
				// fmt.Printf("%v\n", reply.Term)
				if len(args.Entries) > 0 {
					// fmt.Printf("__append entries reply__\nFrom id %v to id %v with size %v and leaderCommit %v had success %v\n", rf.me, i, len(args.Entries), args.LeaderCommit, reply.Success)
				}
				if !accepted {
					rf.nextIndex[i]--
				} else {
					if x := len(args.Entries) + args.PrevLogIndex + 1; x > rf.nextIndex[i] {
						rf.nextIndex[i] = x
					}
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					N := rf.commitIndex + 1
					for N <= len(rf.log) {
						// fmt.Printf("THE N : %v\n", N)
						majcount := 1
						for a, b := range rf.matchIndex {
							if a == rf.me {
								continue
							}
							// fmt.Printf("id = %v, b = %v, term = %v\n", a, b, rf.log[N-1].Term)
							if (b >= N) && (rf.log[N-1].Term == rf.currentTerm) {
								majcount++
							}
						}
						// fmt.Printf("MAJ::%v\n", majcount)
						if 2*majcount > len(rf.peers) {
							for rf.commitIndex < N {
								rf.applyCh <- rf.log[rf.commitIndex].Payload
								rf.commitIndex++
								// fmt.Printf("command = %v at index = %v on id = %v (leader)\n", rf.log[rf.commitIndex-1].Payload.Command, rf.log[rf.commitIndex-1].Payload.CommandIndex, rf.me)
								if rf.commitIndex != rf.log[rf.commitIndex-1].Payload.CommandIndex {
									fmt.Printf("HUH??(leader)\n")
								}
								rf.persist()
							}
						}
						N++
					}
				}
				if reply.Term > rf.currentTerm {
					rf.state = 0
					rf.votedFor = -1
					rf.HBL = 0
					rf.currentTerm = reply.Term
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) mainLoop() {
	for {
		time.Sleep(5 * time.Millisecond)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.clockCounter--
		if rf.clockCounter == 0 {
			if rf.state == 2 {
				rf.clockCounter = 20
				rf.HBL++
				go rf.sendHeartbeat(rf.HBL)
			} else {
				rf.state = 1
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.clockCounter = (60 + rand.Int()%20)
				rf.persist()
				// fmt.Printf("I am candidate, id = %v, term = %v\n", rf.me, rf.currentTerm)
				// fmt.Printf("State = 1, id = %v, term = %v\n", rf.me, rf.currentTerm)
				go rf.doElectcion()
			}
		}
		// if rf.state == 2 {
		// 	fmt.Printf("Am leader::")
		// }
		// fmt.Printf("len %v, committed %v, id %v\n", len(rf.log), rf.commitIndex, rf.me)
		rf.mu.Unlock()
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
	rf.clockCounter = (60 + rand.Int()%20)
	rf.currentTerm = 1
	rf.state = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.mainLoop()

	return rf
}
