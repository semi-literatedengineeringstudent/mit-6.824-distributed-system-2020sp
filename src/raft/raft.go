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

	applyMsgCond *sync.Cond // Lock triggered whenever commit index is updated

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
	
	currentLeaderId int // the most current known leader Id, used in lab 3 so each server can redirect client rpc call to leader
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
		
		//log.Printf("could not read persistent state for this server. Either there has been no persistent state or there is error in reading.")
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

func(rf *Raft) GetCurrentLeaderIdAndTerm() (int, int){
	defer rf.mu.Unlock()

	rf.mu.Lock()
	return rf.currentLeaderId, rf.currentTerm
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

	XTerm int // the term of the conflicting entry in follower
	XIndex int // the first index corresponding to the conflicting term in the follower
	XLength int // the length of the entire follower log. In case follower does not have entry in given index, the leader can simply jump the nextIndex to end of follower log and then do probing
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
				//log.Printf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			} else if rf.role == leader_role{
				//log.Printf("this server %d was a leader in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			} else {
				//log.Printf("this server %d was a follower in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			}
			rf.role = follower_role
			rf.currentTerm = args.Term
			rf.votedFor = not_voted

			rf.currentLeaderId = args.LeaderId
		}

		// rule 2, which is triggered for candidate_role if terms are equal, which also indecates there is a new leader
		if rf.role == candidate_role {
			//log.Printf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing AppendEntries RPC from leader server %d of term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			rf.role = follower_role
			rf.currentTerm = args.Term
			rf.votedFor = not_voted

			rf.currentLeaderId = args.LeaderId

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

					reply.XTerm = invalid_term
					reply.XLength = rf.logEndIndex

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

							reply.XTerm = entriesToCheck.Term
							reply.XIndex = args.PrevLogIndex

							if rf.logs[rf.logStartIndex].Term == entriesToCheck.Term {
								reply.XIndex = rf.logStartIndex
							} else {
								for j := args.PrevLogIndex - 1; j >= rf.logStartIndex; j-- {
									entry := *(rf.logs[j])
									if entry.Term != entriesToCheck.Term {
										reply.XIndex = j + 1
										break
									}
								}
							}
							
							rf.logEndIndex = args.PrevLogIndex - 1
							// no need to delete physical log, just decrement logEndIndex
							// we prioritize time over space LOL
							if rf.logEndIndex == sentinel_index {
								// be careful if leader deletes all entries in the follower's log
								// change logStartIndex accordingly
								
								rf.logStartIndex = sentinel_index
							}
							// Append Entries 3. If an existing entry conflicts with a new one 
							// (same index but different terms), delete the existing entry and all that
							// follow it (�5.3)
						} else {
							reply.Term = args.Term
							reply.Success = true
						}
					}
				}
			} else {
				if rf.logStartIndex == sentinel_index {
					rf.logStartIndex = rf.logStartIndex + 1
				}
				
				/*conflictIndex := args.EntriesStart
				for i := args.EntriesStart; i <= int(math.Min(float64(args.EntriesEnd), float64(rf.logEndIndex))); i++ {
					if args.Entries[i].Term != rf.logs[i].Term {
						conflictIndex = i
						break
					}
				}*/
				// find first conflict index because it is possible that there are
				// some logs from leader appended to this server between when server
				// informs leader of its matchIndex and when it current server
				// handles actual appendEntries call, and we do not want to
				// override existing entries whose index and term conforms with the 
				// leader's new entry.
				// but I don't think this extra check is necessary since
				// leader will never modify the logs it already has during its tenure

				for j := args.EntriesStart; j <= args.EntriesEnd; j++ {
					logToAppend := LogEntry{}
					logToAppend.Term = args.Entries[j].Term
					logToAppend.Command = args.Entries[j].Command
					rf.logs[j] = &logToAppend
				}
 				//log.Printf("this server %d as follower (term %d), successfully appended log from startIndex %d to endIndex %d from leader %d (term %d)", rf.me, rf.currentTerm, args.EntriesStart, args.EntriesEnd, args.LeaderId, args.Term)
				// AppendEntrries 4. Append any new entries not already in the log
				if rf.last_entry_term == args.Term {
					rf.last_entry_index = int(math.Max(float64(args.EntriesEnd), float64(rf.last_entry_index)))
					rf.logEndIndex = int(math.Max(float64(args.EntriesEnd), float64(rf.last_entry_index)))
				} else {
					rf.last_entry_index = args.EntriesEnd
					rf.logEndIndex = args.EntriesEnd
				}
				
				rf.last_entry_term = args.Term

				reply.Term = args.Term
				reply.Success = true
				
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
		rf.applyMsgCond.Broadcast()
		//log.Printf("this server %d as follower (term %d) now has commitIndex %d from leader %d (term %d), and logEndIndex of this server is %d", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderId, args.Term, rf.logEndIndex)

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

	rf.persist()

	return
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
		//log.Printf("this server %d (term %d) received requestVote from server %d of lower term %d, vote not granted", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		return
	} 
	
	// All Servers 2
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (�5.1)
	if args.Term > rf.currentTerm {
		// if a server receives requestVote rpc from any candidate of higher term, change role to follower, update term, and set voteFor to null
		if (rf.role == candidate_role) {
			//log.Printf("this server %d was a candidate in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else if rf.role == leader_role{
			//log.Printf("this server %d was a leader in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			//log.Printf("this server %d was a follower in term %d, and is now becoming a follower upon receicing RequestVote RPC from candidate server %d of term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		}
		rf.role = follower_role
		rf.currentTerm = args.Term
		rf.votedFor = not_voted

		rf.currentLeaderId = invalid_leader
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
		logEndTerm := default_start_term
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
	rf.persist()
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

func (rf *Raft) obtainMatchIndex(serverIndex int, term int, leaderId int, prevLogIndex int, prevLogTerm int, index int, leaderCommit int) (int, int, bool) {
	//log.Printf("this server %d as leader (term %d) now initiate replicating log at %d with server %d with prevLogIndex %d and prevLogTerm %d", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)

	prevLogIndexRpc :=  prevLogIndex
	prevLogTermRpc := prevLogTerm
	for {
		args := AppendEntriesArgs{}
		args.Term = term
		args.LeaderId = leaderId
		args.PrevLogIndex = prevLogIndexRpc
		args.PrevLogTerm = prevLogTermRpc

		args.LeaderCommit = leaderCommit

		args.EmptyRPC = false

		args.EntriesAppended = false

		reply := AppendEntriesReply{}

		receivedReply := rf.sendAppendEntries(serverIndex, &args, &reply)

		rf.mu.Lock()
		if rf.role != leader_role {
			//log.Printf("this server %d was leader (term %d) and its tenure has been terminated and has been switched to follower mode", leaderId, term)
			defer rf.mu.Unlock()
			return rf.currentTerm, invalid_index, false
		} 

		if receivedReply {
			replyTerm := reply.Term
			replySuccess := reply.Success
			if (replyTerm > term) {
				//log.Printf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", leaderId, term, replyTerm, serverIndex)
				defer rf.mu.Unlock()
				rf.currentLeaderId = invalid_leader
				return replyTerm, invalid_index, false
			} else {
				if !replySuccess {
					// Leaders 3.2
					// " If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (�5.3)

					//log.Printf("this server %d as leader (term %d) fail to replicate log at index %d with server %d with prevLogIndex %d and prevLogTerm %d, initiate retry with decrement", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
					
					if reply.XTerm == invalid_term {
						// case 3, the follower simply does not have any entry at prevLogIndex
						// we update nextIndex to end of follower log + 1 so we just skip all unnecessary empty entries
						
						rf.nextIndex[serverIndex] = int(math.Min(float64(rf.nextIndex[serverIndex]), float64(reply.XLength + 1)))
						
						prevLogIndex = int(math.Min(float64(rf.nextIndex[serverIndex] - 1), float64(prevLogIndex - 1)))
						
						prevLogTermRpc = default_start_term
						if prevLogIndexRpc != sentinel_index {
							prevLogTermRpc= rf.logs[prevLogIndexRpc].Term
						}
					} else {
						if rf.logs[reply.XIndex].Term != reply.XTerm {
							// case 1, where leader simply misses entire terms of entries in follower, 
							// the leader simply start appending from XIndex to alter all follower entries from that diverging XIndex
							
							rf.nextIndex[serverIndex] = int(math.Min(float64(rf.nextIndex[serverIndex]), float64(reply.XIndex)))
							
							// since follower does not agree with leader on the term on the XIndex where follower has its first entry 
							// corresponding to that term, we cans simply try start appending from XIndex
							// since it is a known inconsistency
							
							prevLogIndex = int(math.Min(float64(rf.nextIndex[serverIndex] - 1), float64(prevLogIndex - 1)))
							prevLogTermRpc = default_start_term
							if prevLogIndexRpc != sentinel_index {
								prevLogTermRpc= rf.logs[prevLogIndexRpc].Term
							}
						} else {
							// case 2, leader and follower agree on the term in XIndex where follower has its first entry in conflicting
							// term. Given we know there is a conflict, the follower must have more entries that belongs to
							// conflicting term than leader as leader's term in conflicting index (prevLogIndex in last iteration) must be strictly higher.
							// If this is the case, the follower and leader will agree
							// on all entries up to the last entry the leader has that belongs to the follower's conflicting term.
							// then we just start from the entry 1 higher than index of leader's last entry in given conflicting term
							// because that is known to be the first index of inconsistency.
							for i := reply.XIndex + 1; i <= rf.logEndIndex; i++ {
								entry := rf.logs[i]
								if entry.Term != reply.XTerm {
									rf.nextIndex[serverIndex] = int(math.Min(float64(rf.nextIndex[serverIndex]), float64(i)))
									break;
								}
							}
							prevLogIndexRpc = int(math.Min(float64(rf.nextIndex[serverIndex] - 1), float64(prevLogIndex - 1)))
							prevLogTermRpc = default_start_term
							if prevLogIndexRpc != sentinel_index {
								prevLogTermRpc= rf.logs[prevLogIndexRpc].Term
							}
						}
					}
					rf.mu.Unlock()
				
					// Leaders 3.1
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (�5.3)
	
					//log.Printf("this server %d as leader (term %d) now retries finding matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d did no receive reply, initiate retry with decrement", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
				} else {
					//log.Printf("this server %d as leader (term %d) has successfully found matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d, so the matched index is %d", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm, prevLogIndex)
					defer rf.mu.Unlock()
					return term, prevLogIndexRpc, true
				}
			}
		} else {
			//log.Printf("this server %d as leader (term %d) did not find matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d for some reason, maybe network disconnection, return invalid_index", leaderId, term, index, serverIndex, prevLogIndex, prevLogTerm)
			defer rf.mu.Unlock()
			return term, invalid_index, true
		}
	} 
}

func (rf *Raft) appendNewEntriesFromMatchedIndex(serverIndex int, term int, leaderId int, entriesStart int, entriesEnd int, leaderCommit int) (int, bool, bool) {
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
	
	receivedReply := rf.sendAppendEntries(serverIndex, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != leader_role {
		//log.Printf("this server %d was leader (term %d) and its tenure has been terminated and has been switched to follower mode", leaderId, term)
		return rf.currentTerm, false, false
	} 

	if receivedReply {
		replyTerm := reply.Term
		replySuccess := reply.Success
		if (replyTerm > term) {
			//log.Printf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", leaderId, term, replyTerm, serverIndex)
			rf.currentLeaderId = invalid_leader
			return replyTerm, false, false
		} else {
			if !replySuccess {
				//log.Printf("this server %d as leader (term %d) did not successfully append log to follower %d from entriesStart %d to entriesEnd %d, and I have no idea what the bloody hell just happened", leaderId, term, serverIndex, entriesStart, entriesEnd)
				return term, false, true
			} else {
				// Leaders 3.1
				// " If successful: update nextIndex and matchIndex for
				// follower (�5.3)
		
				rf.nextIndex[serverIndex] = int(math.Max(float64(rf.nextIndex[serverIndex]), float64(entriesEnd + 1)))
				
				rf.matchIndex[serverIndex] = int(math.Max(float64(rf.matchIndex[serverIndex]), float64(entriesEnd)))
				//log.Printf("this server %d as leader (term %d) successfully appends log to follower %d from entriesStart %d to entriesEnd %d", leaderId, term, serverIndex, entriesStart, entriesEnd)
				go rf.updateCommitIndex()

				return term, true, true
			}
		}
	} else {
		////log.Printf("this server %d as leader (term %d) did not successfully append log to follower %d from entriesStart %d to entriesEnd %d may be due to network disconnection", leaderId, term, serverIndex, entriesStart, entriesEnd)
		return term, false, true
	}

}

func (rf *Raft) updateCommitIndex() {

	rf.mu.Lock()

	//log.Printf("this server %d as leader (term %d) attempts to update commitIndex", rf.me, rf.currentTerm)
	
	
	defer rf.mu.Unlock()
	numberOfPeers := len(rf.peers)
	matchIndexList := make([]int, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		matchIndexList[i] = rf.matchIndex[i]
		//log.Printf("matchIndex of server %d is %d", i, rf.matchIndex[i])
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexList)))

	// Leaders 4
	// If there exists an N such that N > commitIndex(1), a majority
	// of matchIndex[i] e N(2), and log[N].term == currentTerm(3):
	// set commitIndex = N (�5.3, �5.4).

	//(2)
	for j := rf.quorum - 1; j < numberOfPeers; j++ {
		if matchIndexList[j] > rf.commitIndex && rf.logs[matchIndexList[j]].Term == rf.currentTerm {
			// (1) and (3)
			rf.commitIndex = matchIndexList[j]
			//log.Printf("this server %d as leader (term %d) successfully commited entry at index %d", rf.me, rf.currentTerm, rf.commitIndex)
			rf.applyMsgCond.Broadcast()
			return
		}
	}
	//log.Printf("this server %d as leader (term %d) did not update its commitIndex", rf.me, rf.currentTerm)
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

	if rf.killed() {
		return invalid_index, invalid_term, false
	}

	if rf.role == leader_role {
		if rf.logStartIndex == sentinel_index {
			rf.logStartIndex = index
		}
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
		//log.Printf("the server %d as leader(term %d) has appended new entry with index %d and term %d", leaderId, term, index, term)
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
	//close(rf.applyChRaft)
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
	rand.Seed(time.Now().UnixNano())
	return election_time_out_lower_bound_millisecond + rand.Intn(election_time_out_range_millisecond)
}

func (rf *Raft) syncCommitIndexAndLastApplied() {
	for {
		rf.applyMsgCond.L.Lock()
		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				//log.Printf("the server %d as candidate of term %d has been killed...", rf.me, rf.currentTerm)
			}
			rf.persist()
			defer rf.applyMsgCond.L.Unlock()
			return
		}

		for !(rf.lastApplied < rf.commitIndex) {
			// while the index of next command to apply
			// which is lastApplied + 1, is less then commitIndex
			// we wait for commit index to be at least rf.lastApplied + 1 so that we have something to commit
			rf.applyMsgCond.Wait()
		}

		if rf.role == leader_role {
			//log.Printf("this server %d as leader (term %d) attempts to apply entries with lastApplied %d and commitIndex %d", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		} else if rf.role == candidate_role {
			//log.Printf("this server %d as candidate (term %d) attempts to apply entries with lastApplied %d and commitIndex %d", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		} else {
			//log.Printf("this server %d as follower (term %d) attempts to apply entries with lastApplied %d and commitIndex %d", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = rf.logs[i].Command
			applyMsg.CommandIndex = i
			rf.mu.Unlock()
			rf.applyMessage(applyMsg)
			rf.mu.Lock()
			
		}
		rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(rf.commitIndex)))
		rf.applyMsgCond.L.Unlock()
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
			//log.Printf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
		}
		defer rf.mu.Unlock()
		return
	}
	//log.Printf("this server %d is now a leader of term %d", rf.me, rf.currentTerm)
	leaderId := rf.me
	leaderTerm := rf.currentTerm
	numberOfPeers := len(rf.peers)

	rf.currentLeaderId = rf.me

	rf.persist()
	rf.mu.Unlock()
	for {
		for i := 0; i < numberOfPeers; i++ {
			serverIndex := i
			if serverIndex != leaderId {
				// Leaders 1
				//• Upon election: send initial empty AppendEntries RPCs
				//	(heartbeat) to each server; repeat during idle periods to
				//	prevent election timeouts (§5.2)
					
				rf.mu.Lock()
				
				if rf.role != leader_role {
					//log.Printf("this server %d as leader (term %d) is no longer a leader", rf.me, leaderTerm)
					rf.persist()
					defer rf.mu.Unlock()
					return
				
				}
				
				if rf.killed() {
					if rf.killedMessagePrinted == 0 {
						rf.killedMessagePrinted = 1
						//log.Printf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
					}
					rf.persist()
					defer rf.mu.Unlock()
					return
				}
			
				leaderCommitIndex := rf.commitIndex
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
					go func(serverIndex int, leaderTerm int, leaderId int, leaderCommitIndex int, rf *Raft) {
						args := AppendEntriesArgs{}
	
						args.Term = leaderTerm
						args.LeaderId = leaderId
						args.LeaderCommit = leaderCommitIndex
	
						args.EmptyRPC = true
						time.Sleep(time.Duration(leader_heartbeat_millisecond) * time.Millisecond)
						reply := AppendEntriesReply{}
						//log.Printf("this server %d as leader (term %d) send heart beat to %d", leaderId, leaderTerm, serverIndex)
						
						receivedReply := rf.sendAppendEntries(serverIndex, &args, &reply)
						
						rf.mu.Lock()
						defer rf.mu.Unlock()
	
						if rf.role != leader_role {
							rf.role = follower_role
							//log.Printf("this server %d as leader (term %d) is no longer a leader, switch to follower mode", rf.me, leaderTerm)
						} else if rf.killed() {
							if rf.killedMessagePrinted == 0 {
								rf.killedMessagePrinted = 1
								//log.Printf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
							}
						
						} else if receivedReply {
							if reply.Term > leaderTerm {
								rf.currentTerm = reply.Term
								rf.role = follower_role
								rf.votedFor = not_voted
								rf.currentLeaderId = invalid_leader
								rf.persist()
								//log.Printf("this server %d as leader (term %d) received higher term %d from server %d, switch to follower mode", rf.me, leaderTerm, reply.Term, serverIndex)
							} else {
								//log.Printf("this server %d as leader (term %d) received heart beat reply from server %d and remain a leader", rf.me, leaderTerm, serverIndex)
							}	
						} else {
							//log.Printf("this server %d as leader (term %d) does not received heart beat reply from server %d within election timeout", rf.me, leaderTerm, serverIndex)
						}
						return
					}(serverIndex, leaderTerm, leaderId, leaderCommitIndex, rf)
				} else {
					go func(serverIndex int, term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommitIndex int, leaderLogEndIndex int, rf *Raft) {
						serverTerm, currentMatchedIndex, isLeader := rf.obtainMatchIndex(serverIndex, term, leaderId, prevLogIndex, prevLogTerm, leaderLogEndIndex, leaderCommitIndex)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if !isLeader {
							rf.currentTerm = int(math.Max(float64(serverTerm), float64(rf.currentTerm)))
							rf.role = follower_role
						
							rf.persist()
							//log.Printf("this server %d as leader (term %d) is no longer a leader, switch to follower mode of term %d", rf.me, leaderTerm, rf.currentTerm)
						} else if rf.killed() {
							if rf.killedMessagePrinted == 0 {
								rf.killedMessagePrinted = 1
								//log.Printf("the server %d as leader of term %d has been killed...", rf.me, term)
							}	
						} else if currentMatchedIndex == invalid_index {
							//log.Printf("this server %d as leader (term %d) did not find matched index for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d for some reason, maybe network disconnection, return invalid_index", leaderId, term, leaderLogEndIndex, serverIndex, prevLogIndex, prevLogTerm)
						} else {
							//log.Printf("this server %d as leader (term %d) has found matched index, which is %d, for appending log at index %d with server %d with prevLogIndex %d and prevLogTerm %d, now try to append", leaderId, term, currentMatchedIndex, leaderLogEndIndex, serverIndex, prevLogIndex, prevLogTerm)
							rf.mu.Unlock()
							serverTerm, appendSuccessful, isLeader := rf.appendNewEntriesFromMatchedIndex(serverIndex, term, leaderId, currentMatchedIndex + 1, leaderLogEndIndex, leaderCommitIndex)
							rf.mu.Lock()
							if !isLeader {
								rf.currentTerm = int(math.Max(float64(serverTerm), float64(rf.currentTerm)))
								rf.role = follower_role
				
								rf.persist()
								//log.Printf("this server %d as leader (term %d) is no longer a leader, switch to follower mode", rf.me, leaderTerm)
							} else if rf.killed(){
								if rf.killedMessagePrinted == 0 {
									rf.killedMessagePrinted = 1
									//log.Printf("the server %d as leader of term %d has been killed...", rf.me, rf.currentTerm)
								}
							} else if !appendSuccessful {
								//log.Printf("this server %d as leader (term %d) did not successfully append log to follower %d from entriesStart %d to entriesEnd %d may be due to network disconnection, retry in next heartBeat cycle", leaderId, term, serverIndex, currentMatchedIndex + 1, leaderLogEndIndex)
							} else	{
								//log.Printf("this server %d as leader (term %d) successfully appends log to follower %d from entriesStart %d to entriesEnd %d", leaderId, term, serverIndex, currentMatchedIndex + 1, leaderLogEndIndex)
							}
						}
						return
					}(serverIndex, leaderTerm, leaderId, prevLogIndex, prevLogTerm, leaderCommitIndex, leaderLogEndIndex, rf)
				}
			}
		}
		//rf.updateCommitIndex()
		time.Sleep(time.Duration(leader_heartbeat_millisecond) * time.Millisecond)
	}
}

func (rf *Raft) actAsFollower() {
	rf.mu.Lock()
	if rf.killed() {
		if rf.killedMessagePrinted == 0 {
			rf.killedMessagePrinted = 1
			//log.Printf("the server %d as follower has been killed...", rf.me)
		}
		rf.persist()
		defer rf.mu.Unlock()
		return
	}
	//log.Printf("this server %d is now a follower of term %d", rf.me, rf.currentTerm)
	rf.votedFor = not_voted
	rf.resetElectionTimeOut()
	rf.persist()
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				//log.Printf("the server %d as follower of term %d has been killed...", rf.me, rf.currentTerm)
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

		if currentTime.After(timeToCheck) {
			// Followers (�5.2) 2
			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate:
			// convert to candidate
			defer rf.mu.Unlock()	
			//log.Printf("the server %d as follower in term %d has not heard heart beat from leader after election timeout expire, switch to candidate role", rf.me, rf.currentTerm)
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
	//log.Printf("this server %d is now a candidate for term %d", rf.me, rf.currentTerm + 1)
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

	rf.currentLeaderId = invalid_leader

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
				//log.Printf("this server %d as candidate (term %d) send requestVote to %d", candidateIdThisServer, termThisServer, serverIndex)
				receivedReply := rf.sendRequestVote(serverIndex, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				for timeToCheck, currentTime := (candidateLastHeartBeatTime).Add(time.Duration(candidateElectionTimeOutMilliSecond) * time.Millisecond), time.Now(); !receivedReply && rf.role == candidate_role && !currentTime.After(timeToCheck); {
					// retry if 
					// (1) the reply of requestVote was unsuccessful and
					// (2) server is still a candidate (receiving AppendEntried RPC from server of higher term will terminate the current candidateship) and
					// (3) election timeout when server initiate requestVote does not expire
					rf.mu.Unlock()
					//log.Printf("this server %d as candidate (term %d) did not received requestVote reply from server %d, initiate retry", candidateIdThisServer, termThisServer, serverIndex)
					receivedReply = rf.sendRequestVote(serverIndex, &args, &reply)
					rf.mu.Lock()
				}

				if rf.role != candidate_role {
					cond.Broadcast()
					return
				}

				if receivedReply {
					//log.Printf("this server %d as candidate (term %d) received requestVote reply from server %d", rf.me, termThisServer, serverIndex)
					if reply.Term > termThisServer {
						rf.currentTerm = reply.Term
						rf.role = follower_role

						//log.Printf("this server %d as candidate (term %d) received higher term %d from server %d, switch to follower mode", rf.me, termThisServer, reply.Term, serverIndex)
					} else {
						if reply.VoteGranted {
							//log.Printf("this server %d as candidate (term %d) received vote from server %d", rf.me, termThisServer, serverIndex)
							voteCount++
						} else {
							//log.Printf("this server %d as candidate (term %d) did not received vote from server %d", rf.me, termThisServer, serverIndex)
						}
					}		
				} else {
					//log.Printf("this server %d as candidate (term %d) did not receive requestVote reply from server %d", rf.me, termThisServer, serverIndex)
				}
				finished++
				cond.Broadcast()
				
			}(index, termThisServer, candidateIdThisServer, lastLogIndexThisServer, lastLogTermThisServer ,candidateLastHeartBeatTime, candidateElectionTimeOutMilliSecond, rf)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for voteCount < rf.quorum && finished < numberOfPeers && rf.role == candidate_role {
		timeToCheck := (candidateLastHeartBeatTime).Add(time.Duration(candidateElectionTimeOutMilliSecond) * time.Millisecond)	
		currentTime := time.Now()
		// Candidates (�5.2) 4:
		// " If election timeout elapses: start new election
		if currentTime.After(timeToCheck) {
			//log.Printf("this server %d as candidate did not get reply from all servers during election timeout, restart election", rf.me)
			rf.role = candidate_role
			return
		}

		if rf.killed() {
			if rf.killedMessagePrinted == 0 {
				rf.killedMessagePrinted = 1
				//log.Printf("the server %d as candidate of term %d has been killed...", rf.me, rf.currentTerm)
			}
			rf.persist()
			return
		}
		cond.Wait()
	}
	if rf.role == follower_role {
		//log.Printf("this server %d as candidate has been turned to a follower upon receiving AppendEntries RPC from a leader of greater or equal term", rf.me)
		return
	}
	// Candidates (�5.2) 2:
	// If votes received from majority of servers: become leader
	if voteCount >= rf.quorum {
		rf.role = leader_role
		rf.initLeader()
		//log.Printf("this server %d as candidate has been turned to a leader upon receiving vote from a group of quorum", rf.me)
		return
	} else {
		rf.role = follower_role
		//log.Printf("this server %d as candidate has been turned to a follower upon not receiving vote from a group of quorum", rf.me)
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

	rf.applyMsgCond = sync.NewCond(&rf.mu)

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

	rf.currentLeaderId = invalid_leader
	
	// initialize timeLastHeartBeat and electionTimeOutMilliSecond on followers, all servers start as followers
	rf.resetElectionTimeOut()

	rf.quorum = int(math.Ceil(float64(len(rf.peers)) / 2))
	rf.killedMessagePrinted = 0;

	rf.applyChRaft = applyCh

	go func(rf *Raft){
		go rf.syncCommitIndexAndLastApplied()
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