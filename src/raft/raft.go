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

import "sync"
import "sync/atomic"
import "../labrpc"

import "time"
import "math/rand"
import "math"
//import "os"
import "log"

// import "bytes"
// import "../labgob"



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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers (used by server that restarts)
	currentTerm int // last seen term by the server, seen/changed either by receiving requestVote RPC or by receiving appendEnrties RPC who has higher term
	votedFor int // what this server voted for in the "current term"
	logs map[int]*LogEntry // list of log entries since the last snapshot takes
	logStartIndex int // where the current log starts, used for future log compaction where some logs of small index are deleted and saved in snapshot
	logEndIndex int //  where the current log ends, used for future log compaction where some logs of small index are deleted and saved in snapshot

	// volatile state on all servers
	role int // role the server takes at this moment
	commitIndex int // the index of last log the server has commited. 
	lastApplied int // the index of last log the server has applied to its state machine
	
	// volatile states on leaders
	nextIndex []int // on each server, index of the next log entry the leader's new entry, if successfully appended, will be appended to, init as leader logEndIndex + 1 for each server
	matchIndex []int// on each server, index of highest log entry known to match that of leader's 
	
	//volatile states on follower
	timeLastHeartBeat time.Time
	electionTimeOutMilliSecond int

	quorum int
	killedMessagePrinted int

}

type LogEntry struct {
	Term int
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
	if rf.role == leader_role {
		isleader = true
	} 
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


type AppendEntriesArgs struct {
	Term int // leader’s term
	LeaderId int // so follower can redirect clients (used in labs after 2)
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry
	Entry *LogEntry // log entries to store, for now I just send one for simplicity, scale later
	LeaderCommit int // leader’s commitIndex

	IsHeartBeat bool // so follower can quickly return for heart beat
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself that 
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetElectionTimeOut()
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		if rf.role == candidate_role || rf.role == leader_role {
			//rf.role == candidate_role || (rf.role == leader_role && agrs.Term > rf.currentTerm)
			rf.role = follower_role

			if (rf.role == candidate_role) {
				log.Printf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			} else {
				log.Printf("this server %d was a leader in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			}
			rf.currentTerm = args.Term
 
			// if a candidate received entry from some leader of higher or equal term, 
			// we know election should terminate and candidate goes to follower mode

			// if a leader entry from some leader of higher term, 
			// we know the leader should become a follower
			//defer rf.cond.Broadcast()
		}
		if args.IsHeartBeat {
			reply.Term = args.Term
			reply.Success = true
			return
		}

		// add more logics for log sync later on

	}
	
	/*
	entryAtPrevLogIndex, ok := rf.logs[args.prevLogIndex]
	if !ok {
		reply.term = rf.currentTerm
		reply.success = false
		return
	}*/

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate’s term
	CandidateId int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry 
	LastLogTerm int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}


func candidateIsMoreUpdated(candidateLastLogTerm int, candidateLastLogIndex int, followerLastLogTerm int, followerLastLogIndex int) bool{
	return (candidateLastLogTerm > followerLastLogTerm || (candidateLastLogTerm == followerLastLogTerm && (candidateLastLogIndex >= followerLastLogIndex)))
}

func voteForIsNull(argsTerm int, thisServerTerm int, thisServerVotedFor int) bool{
	if (argsTerm > thisServerTerm) {
		return true
	}
	if (thisServerVotedFor == not_voted) {
		return true
	}
	return false
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimeOut()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// follower send most up to date global term so that 
		// candidate update itself upon follower receiving requestVoteRPC from an outdated condidate
		reply.VoteGranted = false
		return
	} 

	if args.Term > rf.currentTerm && (rf.role == candidate_role || rf.role == leader_role) {
		rf.role = follower_role
		if (rf.role == candidate_role) {
			log.Printf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			log.Printf("this server %d was a leader in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		}
		rf.currentTerm = args.Term
	}
	
	if voteForIsNull(args.Term, rf.currentTerm, rf.votedFor) || rf.votedFor == args.CandidateId {
		var lastEntryTerm int = default_start_term
		if rf.logEndIndex != sentinel_index {
			lastEntryTerm = rf.logs[rf.logEndIndex].Term
		}
		if candidateIsMoreUpdated(args.LastLogTerm, args.LastLogIndex, lastEntryTerm, rf.logEndIndex) {
			rf.currentTerm = int(math.Max(float64(args.Term), float64(rf.currentTerm)))

			reply.Term = args.Term
			reply.VoteGranted = true

			rf.timeLastHeartBeat = time.Now()
			rf.electionTimeOutMilliSecond = generateElectionTimeoutMilliSecond()
		}
	} else {
		rf.votedFor = not_voted
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}


	return
	
	
	/*else if args.Term == rf.currentTerm {
		if rf.votedFor == not_voted || rf.votedFor == args.CandidateId {
			var lastEntryTerm int = default_start_term
			if rf.logEndIndex != sentinel_index {
				lastEntryTerm = rf.logs[rf.lastLogIndex].term
			}
			if candidateIsMoreUpdated(args.LastLogTerm, args.LastLogIndex, lastEntryTerm, rf.logEndIndex) {
				reply.Term = rf.currentTerm
				reply.VoteGranted = true

				rf.timeLastHeartBeat = time.Now()
				rf.electionTimeOutMilliSecond = generateElectionTimeoutMilliSecond()
				return;
			}
		}
	} else {
		var lastEntryTerm int = default_start_term
		if rf.logEndIndex != sentinel_index {
			lastEntryTerm = rf.logs[rf.lastLogIndex].term
		}
		if candidateIsMoreUpdated(args.LastLogTerm, args.LastLogIndex, lastEntryTerm, rf.logEndIndex) {
			rf.currentTerm = args.Term

			reply.Term = args.Term
			reply.VoteGranted = true

			rf.timeLastHeartBeat = time.Now()
			rf.electionTimeOutMilliSecond = generateElectionTimeoutMilliSecond()
			return;
		}
	}*/
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimeOut() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.timeLastHeartBeat = time.Now()
	rf.electionTimeOutMilliSecond = generateElectionTimeoutMilliSecond()
	return
}

func generateElectionTimeoutMilliSecond() int{
	rand.Seed(time.Now().UnixNano())
	return election_time_out_lower_bound_millisecond + rand.Intn(election_time_out_range_millisecond)
}

func (rf *Raft) syncCommitIndexAndLastApplied() {
	if rf.commitIndex > rf.lastApplied {
		for rf.lastApplied < rf.commitIndex {
			// apply actions until match commit index

			// will be implemented to modify state machine later on
			rf.lastApplied += 1
		}
	}
}

func (rf *Raft) actAsLeader() {
	rf.mu.Lock()
	if rf.killed(){
		if rf.killedMessagePrinted == 0 {
			rf.killedMessagePrinted = 1
			log.Printf("the server %d as leader has been killed...", rf.me)
		}
		
		defer rf.mu.Unlock()
		
		return
	}
	log.Printf("this server %d is now a leader of term %d", rf.me, rf.currentTerm)
	rf.resetElectionTimeOut()
	rf.mu.Unlock()

	for {

		rf.mu.Lock()

		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				log.Printf("the server %d as leader has been killed...", rf.me)
			}
			defer rf.mu.Unlock()
			
			return
		}
		leaderTerm := rf.currentTerm
		leaderId := rf.me
	
		leaderCommitIndex := rf.commitIndex
		numberOfPeers := len(rf.peers)

		leaderLastHeartBeatTime := rf.timeLastHeartBeat
		leaderElectionTimeOutMilliSecond := rf.electionTimeOutMilliSecond
		rf.mu.Unlock()

		cond := sync.NewCond(&rf.mu)
		finished := 0
		for i := 0; i < numberOfPeers; i++ {
			index := i
			if (index != leaderId) {
				go func(serverIndex int, leaderTerm int, leaderId int, leaderCommitIndex int, leaderLastHeartBeatTime time.Time, leaderElectionTimeOutMilliSecond int, rf *Raft) {
					entry := LogEntry{}
					entry.Term = leaderTerm	
					
					args := AppendEntriesArgs{}

					args.Term = leaderTerm
					args.LeaderId = leaderId
					args.Entry = &entry
					args.LeaderCommit = leaderCommitIndex

					args.IsHeartBeat = true

					reply := AppendEntriesReply{}

					receivedReply := rf.sendAppendEntries(serverIndex, &args, &reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()
					for timeToCheck, currentTime := (leaderLastHeartBeatTime).Add(time.Duration(leaderElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == leader_role && !currentTime.After(timeToCheck); {
						// retry if 
						// (1) the reply of heart beat was unsuccessful and
						// (2) server is still a leader (receiving AppendEntried RPC from server of higher term will terminate the current leadership) and
						// (3) election timeout when server initiate heart beat does not pass
						rf.mu.Unlock()
						log.Printf("this server %d as leader (term %d) did not received heartbeat reply from server %d, initiate retry", rf.me, leaderTerm, serverIndex)
						receivedReply = rf.sendAppendEntries(serverIndex, &args, &reply)
						rf.mu.Lock()
					}

					if rf.role != leader_role {
						cond.Broadcast()
						return
					}
					
					if receivedReply {
						log.Printf("this server %d as leader (term %d) received heartbeat reply from server %d", rf.me, leaderTerm, serverIndex)
						if reply.Term > leaderTerm {
							rf.currentTerm = reply.Term
							rf.role = follower_role
							log.Printf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", rf.me, leaderTerm, reply.Term, serverIndex)
						} else {
							log.Printf("this server %d as leader (term %d) received heart beat reply from server %d and remain a leader", rf.me, leaderTerm, serverIndex)
						}		
						// tell the condition variable to stop waiting and acquire lock 
					} else {
						log.Printf("this server %d as leader (term %d) does not received heart beat reply from server %d ", rf.me, leaderTerm, serverIndex)
					}
					finished++
					cond.Broadcast()	
				}(index, leaderTerm, leaderId, leaderCommitIndex, leaderLastHeartBeatTime, leaderElectionTimeOutMilliSecond, rf)
			}
		}

		rf.mu.Lock()
		for finished < numberOfPeers - 1 && rf.role == leader_role {
			cond.Wait()
		}
		rf.syncCommitIndexAndLastApplied()
		if rf.role != leader_role {
			defer rf.mu.Unlock()
			return
		}
		
		rf.mu.Unlock()
		time.Sleep(time.Duration(leader_heartbeat_millisecond) * time.Millisecond)

	}
}

func (rf *Raft) actAsFollower() {
	rf.mu.Lock()
	if rf.killed() {
		if rf.killedMessagePrinted == 0 {
			rf.killedMessagePrinted = 1
			log.Printf("the server %d as leader has been killed...", rf.me)
		}
	
		defer rf.mu.Unlock()
		return
	}
	log.Printf("this server %d is now a follower of term %d", rf.me, rf.currentTerm)
	rf.resetElectionTimeOut()
	rf.mu.Unlock()

	for {
		rf.mu.Lock()

		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				log.Printf("the server %d as leader has been killed...", rf.me)
			}
			defer rf.mu.Unlock()
			return
		}

		timeToCheck := (rf.timeLastHeartBeat).Add(time.Duration(rf.electionTimeOutMilliSecond) * time.Millisecond)	
		currentTime := time.Now()
		if currentTime.After(timeToCheck) {
			defer rf.mu.Unlock()	
			log.Printf("the server %d as follower in term %d has not heard heart beat from leader after election timeout expire, switch to candidate role", rf.me, rf.currentTerm)
			rf.role = candidate_role
			//rf.resetElectionTimeOut()
			return
		}
		rf.syncCommitIndexAndLastApplied()
		rf.mu.Unlock()
		time.Sleep(time.Duration(follower_loop_wait_time_millisecond) * time.Millisecond)
	}
}

func (rf *Raft) actAsCandidate() {
	rf.mu.Lock()
	log.Printf("this server %d is now a candidate for term %d", rf.me, rf.currentTerm + 1)

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimeOut()

	termThisServer := rf.currentTerm
	candidateIdThisServer := rf.me
	lastLogIndexThisServer := rf.logEndIndex
	lastLogTermThisServer := default_start_term
	if lastLogIndexThisServer != sentinel_index {
		lastLogTermThisServer = rf.logs[lastLogIndexThisServer].Term
	}
	candidateLastHeartBeatTime := rf.timeLastHeartBeat
	candidateElectionTimeOutMilliSecond := rf.electionTimeOutMilliSecond

	voteCount := 1
	finished := 1

	numberOfPeers := len(rf.peers)


	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	for i := 0; i < numberOfPeers; i++ {
		index := i
		if (i != candidateIdThisServer) {

			go func(serverIndex int, termThisServer int, candidateIdThisServer int, lastLogIndexThisServer int, lastLogTermThisServer int, candidateLastHeartBeatTime time.Time, candidateElectionTimeOutMilliSecond int, rf *Raft) {
				args := RequestVoteArgs{}
				args.Term = termThisServer
				args.CandidateId = candidateIdThisServer
				args.LastLogIndex = lastLogIndexThisServer
				args.LastLogTerm = lastLogTermThisServer
				
				reply := RequestVoteReply{}
	
				receivedReply := rf.sendRequestVote(serverIndex, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				for timeToCheck, currentTime := (candidateLastHeartBeatTime).Add(time.Duration(candidateElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == candidate_role && !currentTime.After(timeToCheck); {
					// retry if 
					// (1) the reply of requestVote was unsuccessful and
					// (2) server is still a candidate (receiving AppendEntried RPC from server of higher term will terminate the current candidateship) and
					// (3) election timeout when server initiate heart beat does not pass
					rf.mu.Unlock()
					log.Printf("this server %d as candidate (term %d) did not received requestVote reply from server %d, initiate retry", rf.me, termThisServer, serverIndex)
					receivedReply = rf.sendRequestVote(serverIndex, &args, &reply)
					rf.mu.Lock()
				}

				if rf.role != candidate_role {
					cond.Broadcast()
					return
				}

				if receivedReply {
					log.Printf("this server %d as candidate (term %d) received heartbeat reply from server %d", rf.me, termThisServer, serverIndex)
					if reply.Term > termThisServer {
						rf.currentTerm = reply.Term
						rf.role = follower_role
						log.Printf("this server %d as candidate (term %d) received higher term %d from server %d, switch to follower mode", rf.me, termThisServer, reply.Term, serverIndex)
					} else {
						if reply.VoteGranted {
							log.Printf("this server %d as candidate (term %d) received vote from server %d", rf.me, termThisServer, serverIndex)
							voteCount++
						} else {
							log.Printf("this server %d as candidate (term %d) did not received vote from server %d", rf.me, termThisServer, serverIndex)
						}
					}		
					// tell the condition variable to stop waiting and acquire lock 

				}
				finished++
				cond.Broadcast()
				
			}(index, termThisServer, candidateIdThisServer, lastLogIndexThisServer, lastLogTermThisServer ,candidateLastHeartBeatTime, candidateElectionTimeOutMilliSecond, rf)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for voteCount < rf.quorum && finished < numberOfPeers && rf.role == candidate_role {
		timeToCheck := (rf.timeLastHeartBeat).Add(time.Duration(rf.electionTimeOutMilliSecond) * time.Millisecond)	
		currentTime := time.Now()
		if currentTime.After(timeToCheck) {
			log.Printf("this server %d as candidate did not get reply from all servers during election timeout, restart election", rf.me)
			rf.role = candidate_role
			//rf.resetElectionTimeOut()
			return
		}

		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				log.Printf("the server %d as leader has been killed...", rf.me)
			}
			return
		}

		cond.Wait()
	}
	rf.syncCommitIndexAndLastApplied()
	if rf.role == follower_role {
		rf.resetElectionTimeOut()
		log.Printf("this server %d as candidate has been turned to a follower upon receiving AppendEntries RPC from a leader of greater or equal term", rf.me)
		return
	}
	if voteCount >= rf.quorum {
		rf.resetElectionTimeOut()
		rf.role = leader_role
		log.Printf("this server %d as candidate has been turned to a leader upon receiving vote from a group of quorum", rf.me)
		return
	} else {
		rf.resetElectionTimeOut()
		rf.role = follower_role
		log.Printf("this server %d as candidate has been turned to a follower upon not receiving vote from a group of quorum", rf.me)
		return
	}

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

	// persistent state on all servers (used by server that restarts)
	rf.currentTerm = default_start_term
	rf.votedFor = not_voted
	rf.logs = make(map[int]*LogEntry)
	rf.logStartIndex = sentinel_index
	rf.logEndIndex = sentinel_index


	// volatile state on all servers
	rf.role = follower_role
	rf.commitIndex = sentinel_index
	rf.lastApplied = sentinel_index
	
	//ijnitialize timeLastHeartBeat and electionTimeOutMilliSecond on followers, all servers start as followers
	//rf.initTimeLastHeartBeat := time.Now()
	//rf.initElectionTimeOutMilliSecond := generateElectionTimeoutMilliSecond()
	rf.resetElectionTimeOut()

	rf.quorum = int(math.Ceil(float64(len(rf.peers)) / 2))
	rf.killedMessagePrinted = 0;
	go func(rf *Raft){
		for {
			if rf.role == follower_role {
				rf.actAsFollower()
			} else if  rf.role == candidate_role {
				rf.actAsCandidate()
			} else {
				rf.actAsLeader()
			}
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
