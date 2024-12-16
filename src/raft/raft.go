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
//import "log"

import "bytes"
import "../labgob"
import "errors"
import "sort"



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

	last_entry_index int // the index of last entry the leader of most recent term that initiate RPC call to the server sent to the server
	last_entry_term int // the most recent term in which the server receives entries from  AppendEntries call from leader corresponding to last_entry_index
	
	// volatile states on leaders
	nextIndex []int 
	// on each server, index of the next log entry the leader's new entry, if successfully appended, will be appended to, init as leader logEndIndex + 1 for each server
	// decrement upon every failed AppendEntries RPC 
	matchIndex []int
	// on each server, index of highest log entry known to match that of leader's 
	// update upon every successful AppendEntries RPC 
	
	//volatile states on follower
	timeLastHeartBeat time.Time
	electionTimeOutMilliSecond int

	quorum int
	killedMessagePrinted int

	applyChRaft chan ApplyMsg

}

type LogEntry struct {
	Term int
	Command interface{}
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.logStartIndex)
	e.Encode(rf.logEndIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) error {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return errors.New("could not read persistent state for this server. Server boot straping")
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs map[int]*LogEntry
	var logStartIndex int
	var logEndIndex int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil ||
	   d.Decode(&logStartIndex) != nil ||
	   d.Decode(&logEndIndex) != nil {
		
		////log.Printntf("could not read persistent state for this server. Either there has been no persistent state or there is error in reading.")
		return errors.New("could not read persistent state for this server. Either there has been no persistent state or there is error in reading.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.logStartIndex = logStartIndex
		rf.logEndIndex = logEndIndex
		return nil
	}
}

func (rf *Raft) applyMessage(applyMsg ApplyMsg) {
	rf.applyChRaft <- applyMsg
}


type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int // so follower can redirect clients (used in labs after 2)
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry

	Entries map[int]*LogEntry // map of entries to append to follower logs starting from matchedIndex + 1 to end of leader log
	EntriesStart int // start of entries appended from leader to follower
	EntriesEnd int // end of entries appended from leader to follower
	
	LeaderCommit int // leaders commitIndex

	EmptyRPC bool // so that follower can quickly return for heart beat

	EntriesAppended bool // given not heartbeat, so that follower can quickly decide if it will append entries or not to its local log

}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself that 
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Followers (�5.2) 1
	// Respond to RPCs from candidates and leaders
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//AppendEntries 1. Reply false if term < currentTerm (�5.1)
		return
	} else {
		rf.resetElectionTimeOut() 

		// the below block is designed to conform with 
		// All Servers 1. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		// Candidates (5.2) 2. If AppendEntries RPC received from new leader as a candidate: convert to follower

		// the below is an updated version that works identically as the one above and satisifies raft
		// rules in a more readable manner

		// rule 1
		if args.Term > rf.currentTerm {
			// if a server receives requestVote rpc from any candidate of higher term, change role to follower, update term, and set voteFor to null
			if (rf.role == candidate_role) {
				//log.Printntf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			} else if rf.role == leader_role{
				//log.Printntf("this server %d was a leader in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			} else {
				//log.Printntf("this server %d was a follower in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			}
			rf.role = follower_role
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()

		}

		// rule 2, which is triggered for candidate_role if terms are equal, which also indecates there is a new leader
		if rf.role == candidate_role {
			//log.Printntf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			rf.role = follower_role
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}

		// briefly sumarize it
		// follower updates its term if receiving rpc from leader of higher term
		// leader updates its term if receiving rpc from leader of higher term (which is guaranteed due to safe election property)
		// and switch to follower
		// candidate updates upon either receiving rpc from leader of higher term or same term (someone else becomes leader before you)
	

		
		if args.EmptyRPC {
			reply.Term = args.Term
			reply.Success = true
		} else {
			if !args.EntriesAppended {
				if args.PrevLogIndex > rf.logEndIndex {
					reply.Term = args.Term
					reply.Success = false
					//AppendEntries 2. Reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm (�5.3)
				} else {
					if args.PrevLogIndex == sentinel_index && args.PrevLogTerm == default_start_term {
						reply.Term = args.Term
						reply.Success = true
					} else {
						entriesToCheck := *(rf.logs[args.PrevLogIndex])
						if entriesToCheck.Term != args.PrevLogTerm {
							reply.Term = args.Term
							reply.Success = false
							
							for i := args.PrevLogIndex; i <= rf.logEndIndex; i++ {
								delete(rf.logs, i)
							}
							rf.logEndIndex = args.PrevLogIndex - 1
							// Append Entries 3. If an existing entry conflicts with a new one 
							// (same index but different terms), delete the existing entry and all that
							// follow it (�5.3)
							rf.persist()
						} else {
							reply.Term = args.Term
							reply.Success = true
						}
					}
				}
			} else {
				for i := args.EntriesStart; i<= args.EntriesEnd; i++ {
					logToAppend := LogEntry{}
					logToAppend.Term = args.Entries[i].Term
					logToAppend.Command = args.Entries[i].Command
					rf.logs[i] = &logToAppend
					//log.Printntf("this server %d as follower (term %d), attempting appending log at index %d with log term %d", rf.me, rf.currentTerm, i, args.Entries[i].Term)
				}
				// AppendEntrries 4. Append any new entries not already in the log
				rf.logEndIndex = args.EntriesEnd

				rf.last_entry_index = int(math.Max(float64(args.EntriesEnd), float64(rf.last_entry_index)))
				rf.last_entry_term = args.Term

				reply.Term = args.Term
				reply.Success = true
				rf.persist()
			}
		}
	}

	//## need to refine on "last new entry" index. and figure out reasoning in step 5...
	if args.LeaderCommit > rf.commitIndex && args.Term == rf.last_entry_term{
		// if LeaderCommit <= commitIndex, there is nothing new we need to perform
		// also we want to make sure the leader term is same as last_entry term before update commit index
		// because commitIndex of the current server should be strictly following the current leader
		// and it is possible that current server has entries different from that of the leader after matched index and before Leadercommit
		// and we do not want to commit those different entries since that could cause inconsistent state between current server and the current leader 
		// once entries are applies in current server.
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.last_entry_index)))
		rf.syncCommitIndexAndLastApplied()
		//log.Printntf("this server %d has updated its commit index to %d", rf.me, rf.commitIndex)
		// AppendEntries 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

		// first, if leadercommit > rf.commitIndex, we know there are commited entries beyong current server's
		// commit index so we need to update
		// however, it is possible that, due to interleaving nature of rpc call that leader could have 
		// commited some entries before current rpc call is handled and earlier calls from leader to this server to append
		// entries have not been completed since leader is still probing for matchedIndex
		// so that, currently, leaderCommit is beyond entries already available in this server
		// such that we cannot use leaderCommit right away as sync entries in log index beyond the end of current server
		// log will cause problem

		// second, it is possible that our log matched that of leader in prevLogIndex but we have some logs after
		// the end of log the leader sends to us as RPC calls with more up to date logs from leader could have been 
		// handled before this one and
		
	}

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
	Term int // candidates term
	CandidateId int // candidate requesting vote
	LastLogIndex int // index of candidates last log entry 
	LastLogTerm int // term of candidates last log entry
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
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs.
	if candidateLastLogTerm != followerLastLogTerm {
		// If the logs have last entries with different terms, then
		// the log with the later term is more up-to-date.
		return candidateLastLogTerm > followerLastLogTerm
	} else {
		// If the logs
		// end with the same term, then whichever log is longer is
		// more up-to-date.
		return candidateLastLogIndex >= followerLastLogIndex
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// Followers (�5.2) 1
	// Respond to RPCs from candidates and leaders
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// RequestVote 1
	// Reply false if term < currentTerm (�5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// follower send most up to date global term so that 
		// candidate update itself upon follower receiving requestVoteRPC from an outdated condidate
		reply.VoteGranted = false
		//log.Printntf("this server %d (term %d) received requestVote from server %d of lower term %d, vote not granted", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		rf.persist()
		return
	} 
	
	// All Servers 2
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (�5.1)
	if args.Term > rf.currentTerm {
		// if a server receives requestVote rpc from any candidate of higher term, change role to follower, update term, and set voteFor to null
		if (rf.role == candidate_role) {
			//log.Printntf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else if rf.role == leader_role{
			//log.Printntf("this server %d was a leader in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			//log.Printntf("this server %d was a follower in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		}
		rf.role = follower_role
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// note we don't do Candidates (�5.2) 3:
	// If AppendEntries RPC received from new leader: convert to follower
	// in requestVote because this is not AppendEntries and this server should not give up
	// its campaign when facing an opponent in the same term.
	// so if I am already a candidate for current term the I have already voted for myself
	// so implement RequestVote 2 directly will not violate any rule

	// RequestVote 2
	// If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (�5.2, �5.4)
	if rf.votedFor == not_voted || rf.votedFor == args.CandidateId {
		// now for the term candidate has, if this server has voted for someone, then it does not vote for anyone else
		// if I have not voted for anyone, examine the log of candidate
		// if I have voted for the same candidate and term is not higher than the term I voted it for
		// then it is possible the candidate fired retry and I need to make sure the log is still as up to date
		// if not as up to date, I will revoke my vote, which still conforms with "at most one vote per term" property
		var logEndTerm int = default_start_term
		if rf.logEndIndex != sentinel_index {
			logEndTerm = rf.logs[rf.logEndIndex].Term
		}
		if candidateIsMoreUpdated(args.LastLogTerm, args.LastLogIndex, logEndTerm, rf.logEndIndex) {
			
			// if the candidate has higher or same term and is more updated
			// vote for it
			reply.Term = args.Term
			reply.VoteGranted = true

			rf.votedFor = args.CandidateId
			rf.resetElectionTimeOut() 
			// only reset election timeout when grant vote to a server
			// this way if current server has more up to date log, it will less frequently 
			// reset its election timeout, and will be more likely to start new election
			// and become legit leader in future term
			rf.persist()
			return
		}
	} 
	
	reply.Term = args.Term
	reply.VoteGranted = false

	return
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

func (rf *Raft) obtainMatchIndex(serverIndex int, term int, leaderId int, prevLogIndex int, prevLogTerm int, index int, leaderCommit int, leaderLastHeartBeatTime time.Time, leaderElectionTimeOutMilliSecond int) (int, int, bool) {
	//log.Printntf("this server %d as leader (term %d) now initiate replicating log at %d with server %d with prevLogIndex %d and prevLogTerm %d", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
	for {
		args := AppendEntriesArgs{}
		args.Term = term
		args.LeaderId = leaderId
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm

		args.LeaderCommit = leaderCommit

		args.EmptyRPC = false

		args.EntriesAppended = false

		reply := AppendEntriesReply{}

		receivedReply := false
		rf.mu.Lock()
		
		for timeToCheck, currentTime := (leaderLastHeartBeatTime).Add(time.Duration(leaderElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == leader_role && !currentTime.After(timeToCheck); {
			// retry if 
			// (1) the reply of requestVote was unsuccessful and
			// (2) server is still a candidate (receiving AppendEntried RPC from server of higher term will terminate the current candidateship) and
			// (3) election timeout when server initiate requestVote does not expire
			rf.mu.Unlock()
			receivedReply = rf.sendAppendEntries(serverIndex, &args, &reply)
			if !receivedReply {
				//log.Printntf("this server %d as leader (term %d), attempting replicating log at index %d, did not receive reply from follower %d with prevLogIndex %d and prevLogTerm %d, initiate retry", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
			} else {
				//log.Printntf("this server %d as leader (term %d), attempting replicating log at index %d, successfully received reply from follower %d with prevLogIndex %d and prevLogTerm %d, now we examine the reply", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
			}
			rf.mu.Lock()
		}

		if rf.role != leader_role {
			//log.Printntf("this server %d was leader (term %d) and its tenure has been terminated and has been switched to follower mode", leaderId, term)
			defer rf.mu.Unlock()

			return rf.currentTerm, sentinel_index, false
		} 

		if receivedReply {
			replyTerm := reply.Term
			replySuccess := reply.Success
			if (replyTerm > term) {
				//log.Printntf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", leaderId, term, replyTerm, serverIndex)
				defer rf.mu.Unlock()
				return replyTerm, invalid_index, false
			} else {
				if !replySuccess {
					// Leaders 3.2
					// " If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (�5.3)
					//log.Printntf("this server %d as leader (term %d) fail to replicate log at index %d with server %d with prevLogIndex %d and prevLogTerm %d, initiate retry with decrement", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
					
					rf.nextIndex[serverIndex] = int(math.Min(float64(rf.nextIndex[serverIndex]), float64(prevLogIndex)))
					prevLogIndex = int(math.Min(float64(rf.nextIndex[serverIndex] - 1), float64(prevLogIndex - 1)))
					prevLogTerm = default_start_term
					if prevLogIndex != sentinel_index {
						prevLogTerm = rf.logs[prevLogIndex].Term
					} 
					rf.mu.Unlock()
				
					args.PrevLogIndex = prevLogIndex
					args.PrevLogTerm = prevLogTerm
					// Leaders 3.1
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (�5.3)
	
					//log.Printntf("this server %d as leader (term %d) now retries finding matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d did no receive reply, initiate retry with decrement", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
				} else {
					//log.Printntf("this server %d as leader (term %d) has successfully found matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d, so the matched index is %d", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm, prevLogIndex)
					defer rf.mu.Unlock()
					return term, prevLogIndex, true
				}
			}
		} else {
			//log.Printntf("this server %d as leader (term %d) did not find matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d within election timeout, return sentinel_index", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
			defer rf.mu.Unlock()
			return term, invalid_index, true
		}
	} 
}

func (rf *Raft) appendNewEntriesFromMatchedIndex(serverIndex int, term int, leaderId int, entriesStart int, entriesEnd int, leaderCommit int, leaderLastHeartBeatTime time.Time, leaderElectionTimeOutMilliSecond int) (int, bool, bool) {
	args := AppendEntriesArgs{}
	args.Term = term
	args.LeaderId = leaderId

	args.Entries =  make(map[int]*LogEntry)
	args.EntriesStart = entriesStart
	args.EntriesEnd = entriesEnd

	rf.mu.Lock()
	for i := args.EntriesStart; i <= args.EntriesEnd; i++ {
		entryToAppend := LogEntry{}
		entryToAppend.Term = rf.logs[i].Term
		entryToAppend.Command = rf.logs[i].Command
		args.Entries[i] = &entryToAppend
	}
	rf.mu.Unlock()

	args.LeaderCommit = leaderCommit

	args.EmptyRPC = false

	args.EntriesAppended = true

	reply := AppendEntriesReply{}

	receivedReply := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for timeToCheck, currentTime := (leaderLastHeartBeatTime).Add(time.Duration(leaderElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == leader_role && !currentTime.After(timeToCheck); {
		// retry if 
		// (1) the reply of requestVote was unsuccessful and
		// (2) server is still a candidate (receiving AppendEntried RPC from server of higher term will terminate the current candidateship) and
		// (3) election timeout when server initiate requestVote does not expire
		rf.mu.Unlock()
		receivedReply = rf.sendAppendEntries(serverIndex, &args, &reply)
		if !receivedReply {
			//log.Printntf("this server %d as leader (term %d), successfully received reply when attempting append log to follower %d from entriesStart %d to entriesEnd %d, now examine reply", leaderId, term, serverIndex, entriesStart, entriesEnd)
		} else {
			//log.Printntf("this server %d as leader (term %d), did not receive reply when attempting append log to follower %d from entriesStart %d to entriesEnd %d, initiate retry", leaderId, term, serverIndex, entriesStart, entriesEnd)
		}
		rf.mu.Lock()
	}

	if rf.role != leader_role {
		//log.Printntf("this server %d was leader (term %d) and its tenure has been terminated and has been switched to follower mode", leaderId, term)
		return rf.currentTerm, false, false
	} 

	if receivedReply {
		replyTerm := reply.Term
		replySuccess := reply.Success
		if (replyTerm > term) {
			//log.Printntf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", leaderId, term, replyTerm, serverIndex)
			return replyTerm, false, false
		} else {
			if !replySuccess {
				//log.Printntf("this server %d as leader (term %d) did not successfully append log to follower %d from entriesStart %d to entriesEnd %d, and I have no idea what the bloody hell just happened", leaderId, term, serverIndex, entriesStart, entriesEnd)
				return term, false, true
			} else {
				// Leaders 3.1
				// " If successful: update nextIndex and matchIndex for
				// follower (�5.3)
		
				rf.nextIndex[serverIndex] = int(math.Max(float64(rf.nextIndex[serverIndex]), float64(entriesEnd + 1)))
				rf.matchIndex[serverIndex] = int(math.Max(float64(rf.matchIndex[serverIndex]), float64(entriesEnd)))
	
				//log.Printntf("this server %d as leader (term %d) successfully appends log to follower %d from entriesStart %d to entriesEnd %d", leaderId, term, serverIndex, entriesStart, entriesEnd)
				return term, true, true
			}
		}
	} else {
		//log.Printntf("this server %d as leader (term %d) did not successfully append log to follower %d from entriesStart %d to entriesEnd %d within election timeout", leaderId, term, serverIndex, entriesStart, entriesEnd)
		return term, false, true
	}

}

func (rf *Raft) updateCommitIndex() {
	numberOfPeers := len(rf.peers)
	matchIndexList := make([]int, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		matchIndexList[i] = rf.matchIndex[i]
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexList)))

	// Leaders 4
	// (2)
	// If there exists an N such that N > commitIndex(1), a majority
	// of matchIndex[i] e N(2), and log[N].term == currentTerm(3):
	// set commitIndex = N (�5.3, �5.4).

	//(2)
	for j := rf.quorum - 1; j < numberOfPeers; j++ {
		if matchIndexList[j] > rf.commitIndex && rf.logs[matchIndexList[j]].Term == rf.currentTerm {
			// (1) and (3)
			rf.commitIndex = matchIndexList[j]
			//log.Printntf("this server %d as leader (term %d) successfully commited entry at index %d", rf.me, rf.currentTerm, rf.commitIndex)
			return
		}
	}
	return
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
	rf.mu.Lock()
	index := rf.logEndIndex + 1
	term := rf.currentTerm
	leaderId := rf.me
	defer rf.mu.Unlock()

	if rf.role == leader_role {
		
		rf.matchIndex[leaderId] = index
		rf.logEndIndex = index
		rf.nextIndex[leaderId] = index + 1

		entryToAppend := LogEntry{}
		entryToAppend.Term = term
		entryToAppend.Command = command
		// Leaders 2
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine (�5.3)
		rf.logs[index] = &entryToAppend
		rf.persist()
		// we do not send RPC to followers immediately because we want to save band width
		// and we shoot in every heart beat cycle to send entries to followers in batch
		return index, term, true
	} else {
		return index, term, false
	}

	// Your code here (2B).
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
	rf.timeLastHeartBeat = time.Now()
	rf.electionTimeOutMilliSecond = generateElectionTimeoutMilliSecond()
	return
}

func generateElectionTimeoutMilliSecond() int{
	//rand.Seed(time.Now().UnixNano())
	return election_time_out_lower_bound_millisecond + rand.Intn(election_time_out_range_millisecond)
}

func (rf *Raft) syncCommitIndexAndLastApplied() {
	
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.Command = rf.logs[rf.lastApplied].Command
		applyMsg.CommandIndex = rf.lastApplied
		rf.applyMessage(applyMsg)

	}
	
}

func (rf *Raft) initLeader() {
	numberOfPeers := len(rf.peers)

	rf.matchIndex = make([]int, numberOfPeers)
	rf.nextIndex = make([]int, numberOfPeers)

	for i := 0; i < numberOfPeers; i++ {
		if i == rf.me {
			rf.matchIndex[i] = rf.logEndIndex
			rf.nextIndex[i] = rf.logEndIndex + 1
		} else {
			rf.matchIndex[i] = sentinel_index // init to sentinel_index and update upon handling incoming command
			rf.nextIndex[i] = rf.logEndIndex + 1
		}
	}
}

func (rf *Raft) actAsLeader() {
	rf.mu.Lock()
	if rf.killed(){
		if rf.killedMessagePrinted == 0 {
			rf.killedMessagePrinted = 1
			//log.Printntf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
		}
		
		defer rf.mu.Unlock()
		
		return
	}
	//log.Printntf("this server %d is now a leader of term %d", rf.me, rf.currentTerm)

	rf.mu.Unlock()
	cond := sync.NewCond(&rf.mu)

	for {
		rf.mu.Lock()
		leaderTerm := rf.currentTerm
		leaderId := rf.me
		if rf.role != leader_role {
			defer rf.mu.Unlock()
			//log.Printntf("this server %d as leader (term %d) is no longer a leader", rf.me, leaderTerm)
			return
		}
		
		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				//log.Printntf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
			}
			defer rf.mu.Unlock()
			return
		}
	
		leaderCommitIndex := rf.commitIndex
		numberOfPeers := len(rf.peers)

		rf.resetElectionTimeOut()
		leaderLastHeartBeatTime := rf.timeLastHeartBeat
		leaderElectionTimeOutMilliSecond := rf.electionTimeOutMilliSecond
		rf.persist()
		rf.mu.Unlock()

		finished := 1
		numHeartBeatReplyReceived := 1
		// Leaders 1
		// Upon election: send initial empty AppendEntries RPCs
		// (heartbeat) to each server; repeat during idle periods to
		// prevent election timeouts (�5.2)
		for i := 0; i < numberOfPeers; i++ {
			serverIndex := i
			// Leaders 3
			// If last log index e nextIndex for a follower: send
			// AppendEntries RPC with log entries starting at nextIndex
			if (serverIndex != leaderId) {
				rf.mu.Lock()
				serverNextIndex := rf.nextIndex[serverIndex]
				prevLogIndex := sentinel_index
				prevLogTerm := default_start_term
				if serverNextIndex - 1 != sentinel_index {
					prevLogIndex = serverNextIndex - 1
					prevLogTerm = (rf.logs[serverNextIndex - 1]).Term
				}
				leaderLogEndIndex := rf.logEndIndex
				rf.mu.Unlock()

				if leaderLogEndIndex < serverNextIndex {
					go func(serverIndex int, leaderTerm int, leaderId int, leaderCommitIndex int, leaderLastHeartBeatTime time.Time, leaderElectionTimeOutMilliSecond int, rf *Raft) {
						args := AppendEntriesArgs{}
	
						args.Term = leaderTerm
						args.LeaderId = leaderId
						args.LeaderCommit = leaderCommitIndex
	
						args.EmptyRPC = true
	
						reply := AppendEntriesReply{}
						//log.Printntf("this server %d as leader (term %d) send heart beat to %d", leaderId, leaderTerm, serverIndex)
						
						receivedReply := false
	
						rf.mu.Lock()
						defer rf.mu.Unlock()
	
						for timeToCheck, currentTime := (leaderLastHeartBeatTime).Add(time.Duration(leaderElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == leader_role && !currentTime.After(timeToCheck); {
							// retry if 
							// (1) the reply of requestVote was unsuccessful and
							// (2) server is still a candidate (receiving AppendEntried RPC from server of higher term will terminate the current candidateship) and
							// (3) election timeout when server initiate requestVote does not expire
							rf.mu.Unlock()
							receivedReply = rf.sendAppendEntries(serverIndex, &args, &reply)
							if !receivedReply {
								//log.Printntf("this server %d as leader (term %d) did not receive heartbeat reply from server %d, initiate retry", leaderId, leaderTerm, serverIndex)
							} else {
								//log.Printntf("this server %d as leader (term %d) did received heartbeat reply from server %d, now examine output", leaderId, leaderTerm, serverIndex)
							}
							rf.mu.Lock()
						}
	
						if rf.role != leader_role {
							rf.persist()
							cond.Broadcast()
							return
						}
						
						if receivedReply {
							//log.Printntf("this server %d as leader (term %d) received heartbeat reply from server %d", leaderId, leaderTerm, serverIndex)
							if reply.Term > leaderTerm {
								rf.currentTerm = reply.Term
								rf.role = follower_role
								rf.votedFor = not_voted
								rf.persist()
								//log.Printntf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", rf.me, leaderTerm, reply.Term, serverIndex)
							} else {
								//log.Printntf("this server %d as leader (term %d) received heart beat reply from server %d and remain a leader", rf.me, leaderTerm, serverIndex)
								numHeartBeatReplyReceived++
							}	
						} else {
							//log.Printntf("this server %d as leader (term %d) does not received heart beat reply from server %d within election timeout", rf.me, leaderTerm, serverIndex)
						}
						finished++
						cond.Broadcast()	
					}(serverIndex, leaderTerm, leaderId, leaderCommitIndex, leaderLastHeartBeatTime, leaderElectionTimeOutMilliSecond,rf)
				} else {
					go func(serverIndex int, term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommitIndex int, leaderLogEndIndex int, leaderLastHeartBeatTime time.Time, leaderElectionTimeOutMilliSecond int, rf *Raft) {
						serverTerm, currentMatchedIndex, isLeader := rf.obtainMatchIndex(serverIndex, term, leaderId, prevLogIndex, prevLogTerm, leaderLogEndIndex, leaderCommitIndex, leaderLastHeartBeatTime, leaderElectionTimeOutMilliSecond)
						if !isLeader {
							rf.mu.Lock()
							rf.currentTerm = int(math.Max(float64(serverTerm), float64(rf.currentTerm)))
							rf.role = follower_role
							rf.mu.Unlock()
							rf.persist()
							finished++
							
						} else {
							if currentMatchedIndex == invalid_index {
								finished++
							} else {
								serverTerm, appendSuccessful, isLeader := rf.appendNewEntriesFromMatchedIndex(serverIndex, term, leaderId, currentMatchedIndex + 1, leaderLogEndIndex, leaderCommitIndex, leaderLastHeartBeatTime, leaderElectionTimeOutMilliSecond)
								if !isLeader {
									rf.mu.Lock()
									rf.currentTerm = int(math.Max(float64(serverTerm), float64(rf.currentTerm)))
									rf.role = follower_role
									rf.mu.Unlock()
									rf.persist()
									finished++
								} else {
									if !appendSuccessful {
										finished++
									} else {
										finished++
										numHeartBeatReplyReceived++
									}
								}

							}
						}
						//log.Printntf("this server %d as leader (term %d) attempts to append logs to followers %d from entriesStart %d to entriesEnd %d", leaderId, term, serverIndex, currentMatchedIndex + 1, leaderLogEndIndex)
						cond.Broadcast()
						return
					}(serverIndex, leaderTerm, leaderId, prevLogIndex, prevLogTerm, leaderCommitIndex, leaderLogEndIndex, leaderLastHeartBeatTime, leaderElectionTimeOutMilliSecond, rf)
				}
			}
		}

		rf.mu.Lock()
		
		for finished < numberOfPeers && numHeartBeatReplyReceived < rf.quorum && rf.role == leader_role{
			timeToCheck := (rf.timeLastHeartBeat).Add(time.Duration(rf.electionTimeOutMilliSecond) * time.Millisecond)	
			currentTime := time.Now()
			if currentTime.After(timeToCheck) {
				//log.Printntf("this server %d as leader (term %d) did not get heartbeat reply from majority servers during election timeout, switch to follower in higher term", rf.me, rf.currentTerm)
				rf.role = follower_role
				rf.currentTerm = rf.currentTerm + 1
				rf.persist()
				defer rf.mu.Unlock()
				return
			}
	
			if rf.killed() {
				if rf.killedMessagePrinted == 0 {
					rf.killedMessagePrinted = 1
					//log.Printntf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
				}
				rf.persist()
				defer rf.mu.Unlock()
				return
			}
			rf.updateCommitIndex()
			cond.Wait()
		}

		rf.syncCommitIndexAndLastApplied()
		
		if rf.role != leader_role {
			//log.Printntf("this server %d as leader (term %d) is no longer a leader, and is now a follower of term %d", rf.me, leaderTerm, rf.currentTerm)
			rf.persist()
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
			//log.Printntf("the server %d as follower has been killed...", rf.me)
		}
		rf.persist()
		defer rf.mu.Unlock()
		return
	}
	//log.Printntf("this server %d is now a follower of term %d", rf.me, rf.currentTerm)
	rf.votedFor = not_voted
	rf.resetElectionTimeOut()
	rf.mu.Unlock()

	for {
		rf.mu.Lock()

		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				//log.Printntf("the server %d as follower of term %d has been killed...", rf.me, rf.currentTerm)
			}
			rf.persist()
			defer rf.mu.Unlock()
			return
		}
		if rf.role != follower_role {
			defer rf.mu.Unlock()
			rf.persist()
			return;
		}

		timeToCheck := (rf.timeLastHeartBeat).Add(time.Duration(rf.electionTimeOutMilliSecond) * time.Millisecond)	
		currentTime := time.Now()
		rf.syncCommitIndexAndLastApplied()
		if currentTime.After(timeToCheck) {
			// Followers (�5.2) 2
			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
			defer rf.mu.Unlock()	
			//log.Printntf("the server %d as follower in term %d has not heard heart beat from leader after election timeout expire, switch to candidate role", rf.me, rf.currentTerm)
			rf.role = candidate_role
			rf.persist()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(follower_loop_wait_time_millisecond) * time.Millisecond)
	}
}

func (rf *Raft) actAsCandidate() {
	rf.mu.Lock()
	//log.Printntf("this server %d is now a candidate for term %d", rf.me, rf.currentTerm + 1)
	// Candidates (�5.2) 1.1
	// " Increment currentTerm
	rf.currentTerm += 1
	// Candidates (�5.2) 1.2
	// " Vote for self
	rf.votedFor = rf.me
	// Candidates (�5.2) 1.3
	// " Reset election timer
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

	rf.persist()
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)

	// Candidates (�5.2) 1.4
	// " Send RequestVote RPCs to all other servers

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
				//log.Printntf("this server %d as candidate (term %d) send requestVote to %d", candidateIdThisServer, termThisServer, serverIndex)
				receivedReply := rf.sendRequestVote(serverIndex, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				for timeToCheck, currentTime := (candidateLastHeartBeatTime).Add(time.Duration(candidateElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == candidate_role && !currentTime.After(timeToCheck); {
					// retry if 
					// (1) the reply of requestVote was unsuccessful and
					// (2) server is still a candidate (receiving AppendEntried RPC from server of higher term will terminate the current candidateship) and
					// (3) election timeout when server initiate requestVote does not expire
					rf.mu.Unlock()
					//log.Printntf("this server %d as candidate (term %d) did not received requestVote reply from server %d, initiate retry", candidateIdThisServer, termThisServer, serverIndex)
					receivedReply = rf.sendRequestVote(serverIndex, &args, &reply)
					rf.mu.Lock()
				}

				if rf.role != candidate_role {
					cond.Broadcast()
					return
				}

				if receivedReply {
					//log.Printntf("this server %d as candidate (term %d) received requestVote reply from server %d", rf.me, termThisServer, serverIndex)
					if reply.Term > termThisServer {
						rf.currentTerm = reply.Term
						rf.role = follower_role
						//log.Printntf("this server %d as candidate (term %d) received higher term %d from server %d, switch to follower mode", rf.me, termThisServer, reply.Term, serverIndex)
					} else {
						if reply.VoteGranted {
							//log.Printntf("this server %d as candidate (term %d) received vote from server %d", rf.me, termThisServer, serverIndex)
							voteCount++
						} else {
							//log.Printntf("this server %d as candidate (term %d) did not received vote from server %d", rf.me, termThisServer, serverIndex)
						}
					}		
				} else {
					//log.Printntf("this server %d as candidate (term %d) did not receive requestVote reply from server %d", rf.me, termThisServer, serverIndex)
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
		// Candidates (�5.2) 4:
		// " If election timeout elapses: start new election
		if currentTime.After(timeToCheck) {
			//log.Printntf("this server %d as candidate did not get reply from all servers during election timeout, restart election", rf.me)
			rf.role = candidate_role
			return
		}

		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				//log.Printntf("the server %d as candidate of term %d has been killed...", rf.me, rf.currentTerm)
			}
			return
		}
		cond.Wait()
	}
	rf.syncCommitIndexAndLastApplied()
	if rf.role == follower_role {
		//log.Printntf("this server %d as candidate has been turned to a follower upon receiving AppendEntries RPC from a leader of greater or equal term", rf.me)
		return
	}
	// Candidates (�5.2) 2:
	// If votes received from majority of servers: become leader
	if voteCount >= rf.quorum {
		rf.role = leader_role
		rf.initLeader()

		//log.Printntf("this server %d as candidate has been turned to a leader upon receiving vote from a group of quorum", rf.me)
		return
	} else {
		rf.role = follower_role
		//log.Printntf("this server %d as candidate has been turned to a follower upon not receiving vote from a group of quorum", rf.me)
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

	// initialize from state persisted before a crash
	if rf.readPersist(persister.ReadRaftState()) != nil {
		rf.currentTerm = default_start_term
		rf.votedFor = not_voted
		rf.logs = make(map[int]*LogEntry)
		rf.logStartIndex = sentinel_index
		rf.logEndIndex = sentinel_index
		rf.persist()
	}
	


	// volatile state on all servers
	rf.role = follower_role
	rf.commitIndex = sentinel_index
	rf.lastApplied = sentinel_index
	rf.last_entry_index = sentinel_index
	rf.last_entry_term = default_start_term
	
	// initialize timeLastHeartBeat and electionTimeOutMilliSecond on followers, all servers start as followers
	rf.resetElectionTimeOut()

	rf.quorum = int(math.Ceil(float64(len(rf.peers)) / 2))
	rf.killedMessagePrinted = 0;

	rf.applyChRaft = applyCh

	go func(rf *Raft){
		for {
			rf.mu.Lock()
			rfRole := rf.role
			rfKilled := rf.killed()
			rf.mu.Unlock()
			if (rfKilled) {
				time.Sleep(time.Duration(killed_server_busywait_avoid_time_millisecond) * time.Millisecond)
			} else {
				if rfRole == follower_role {
					rf.actAsFollower()
				} else if rfRole == candidate_role {
					rf.actAsCandidate()
				} else {
					rf.actAsLeader()
				}
			}
		}
	}(rf)

	
	return rf
}




