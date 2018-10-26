package main

import (
	"log"
	rand "math/rand"
	"sync"
	"time"

	context "golang.org/x/net/context"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-ywng/pb"
)

const (
	follower  = 1
	candidate = 2
	leader    = 3
	// for cluster membership change
	//shutdown  = 4

	//different timeout in ms
	ELECTION_TIMEOUT_LOWER_BOUND = 1000
	ELECTION_TIMEOUT_UPPER_BOUND = 4000
	HEARTBEAT_TIMEOUT            = 500
)

type AppendResponse struct {
	ret  *pb.AppendEntriesRet
	err  error
	peer string
}

type VoteResponse struct {
	ret  *pb.RequestVoteRet
	err  error
	peer string
}

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput

	//lock to protect shared access to this raft server state
	//though in our lab exercise, this shouldn't be a concern
	//as only one main go routine is accessing the state at any time
	mu     sync.Mutex
	me     string
	leader string
	//TO DO: we need persister for shutdown and recovery
	//persister 	*Persister

	state      int64
	quorumSize int64

	//this raft server persistent states
	currentTerm  int64
	votedFor     string
	lastVoteTerm int64
	log          []*pb.LogEntry

	//this raft server volatile states
	commitIndex int64
	lastApplied int64

	//leader's volatile states
	nextIndex  map[string]int64
	matchIndex map[string]int64
	//map of logIndex -> client response ch
	clientsResponse map[int64]chan pb.Result

	//timer & ticker for election timeout and heartbeat
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	randSeed       *rand.Rand

	//peers
	peers *arrayPeers

	//for snapshot
	//lastIncludedLogEntry *pb.LogEntry

	//TO DO: for memershutdown
	//killServer chan int64
}

func (r *Raft) leaderStatePrep() {
	r.state = leader
	r.leader = r.me
	// reset the heartbeat timer & stop election timer
	restartTimer(r.heartBeatTimer, HEARTBEAT_TIMEOUT*time.Millisecond)
	stopTimer(r.electionTimer)

	//initialise leader's volatile state
	r.nextIndex = make(map[string]int64)
	r.matchIndex = make(map[string]int64)
	r.clientsResponse = make(map[int64]chan pb.Result)

	index := r.getLastLogIndex() + 1
	for _, peer := range *r.peers {
		r.nextIndex[peer] = index
		r.matchIndex[peer] = 0
	}
}

func (r *Raft) fallbackToFollower() {
	r.state = follower
	// reset the election timer & stop heartbeat timer
	restartTimer(r.electionTimer, randomDuration(r.randSeed))
	stopTimer(r.heartBeatTimer)
}

func (r *Raft) deleteEntryFrom(index int64) {
	firstIndex := r.log[0].Index
	if r.getLastLogIndex() < index {
		return
	} else {
		sliceIndex := index - firstIndex
		r.log = r.log[:sliceIndex]
	}
}

func (r *Raft) getLastLogIndex() int64 {
	return r.log[len(r.log)-1].Index
}

func (r *Raft) getLastLogTerm() int64 {
	return r.log[len(r.log)-1].Term
}

func (r *Raft) getLogLen() int64 {
	return int64(len(r.log))
}

func (r *Raft) addLogEntry(entry *pb.LogEntry) {
	r.log = append(r.log, entry)
}

// the logic of index-firstIndex is for snapshot logic
// after snapshot, the entry.Index is not necessarily the index of the log array
func (r *Raft) getLogEntry(index int64) (*pb.LogEntry, bool) {
	var entry *pb.LogEntry
	if r.getLogLen() == 0 {
		return entry, false
	}
	firstIndex := r.log[0].Index
	if r.getLastLogIndex() < index || firstIndex > index {
		return entry, false
	} else {
		return r.log[index-firstIndex], true
	}
}

func (r *Raft) getEntryFrom(index int64) []*pb.LogEntry {
	firstIndex := r.log[0].Index
	sliceIndex := index - firstIndex
	return r.log[sliceIndex:]
}

// this check the raft server's log if any committed but unhandled commands
// after the command is handled, it will response to the client by HandleCommand function
func (r *Raft) ProcessLogs(s *KVStore) {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry, _ := r.getLogEntry(r.lastApplied)

		//only leader reply to client's request
		//if not leader, just output to a dummy channel / nil channel
		var responseChan chan pb.Result
		if r.state == leader {
			responseChan = r.clientsResponse[entry.Index]
		} else {
			responseChan = nil
		}
		op := InputChannelType{command: *entry.Command, response: responseChan}
		s.HandleCommand(op)

		log.Printf("Applied committed log to the state machine. Index: %d, Command: %s.", entry.Index, entry.Command.Operation)
	}
}

// this is used to construct and send a vote request to all peers
func (r *Raft) sendVoteRequests(peerClients map[string]pb.RaftClient, voteResponseChan chan VoteResponse) {
	r.state = candidate
	r.currentTerm++
	r.votedFor = r.me
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := int64(0)
	if lastLogIndex != 0 {
		lastLogTerm = r.getLastLogTerm()
	}

	for p, c := range peerClients {
		// Send in parallel so we don't wait for each client.
		log.Printf("Send vote request to %s, currentTerm: %d, lastLogIndex: %d, lastLogTerm: %d",
			p, r.currentTerm, lastLogIndex, lastLogTerm)
		go func(c pb.RaftClient, p string) {
			ret, err := c.RequestVote(context.Background(),
				&pb.RequestVoteArgs{Term: r.currentTerm,
					CandidateID:  r.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm})
			voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
		}(c, p)
	}
}

// this is used to construct and send an append entry request to all peers
func (r *Raft) sendApeendEntries(peerClients map[string]pb.RaftClient, appendResponseChan chan AppendResponse) {
	for p, c := range peerClients {
		r.sendApeendEntriesTo(p, c, appendResponseChan)
	}
}

// this is used to construct and send an append entry request to given peer (var p)
func (r *Raft) sendApeendEntriesTo(p string, c pb.RaftClient, appendResponseChan chan AppendResponse) {
	var isHeartBeat bool
	if r.getLastLogIndex() >= r.nextIndex[p] {
		isHeartBeat = false
	} else {
		isHeartBeat = true
	}

	prevLogTerm := int64(0)
	prevLogIndex := r.nextIndex[p] - 1

	if prevLogIndex != 0 {
		entry, ok := r.getLogEntry(prevLogIndex)
		if ok {
			prevLogTerm = entry.Term
		} else {
			//cannot get the  prevLogIndex,
			//it is snapshot... sned install snapshot to peer

		}
	}

	var args *pb.AppendEntriesArgs
	if isHeartBeat {
		args = &pb.AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: r.commitIndex,
			Entries:      nil}
	} else {
		if _, ok := r.getLogEntry(prevLogIndex + 1); !ok {
			//cannot get the  prevLogIndex,
			//it is snapshot... sned install snapshot to peer

		}
		entries := r.getEntryFrom(prevLogIndex + 1)
		args = &pb.AppendEntriesArgs{Term: r.currentTerm,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: r.commitIndex,
			Entries:      entries}
	}

	// Send in parallel so we don't wait for each client.
	log.Printf("Sent append entry request to %s, prevLogIndex: %d, prevLogTerm: %d, commitIndex: %d, entriesLen: %d.",
		p, prevLogIndex, prevLogTerm, r.commitIndex, int64(len(args.Entries)))
	go func(c pb.RaftClient, p string) {
		ret, err := c.AppendEntries(context.Background(), args)
		appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
	}(c, p)
}

// put an append entry request to the given raft server's (var r) Append Entry Channel
// this is used/called to make an append entry request to given peer
func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// put a vote request to the given raft server's (var r) Vote Request Channel
// this is used/called to make a vote request to given peer
func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}
