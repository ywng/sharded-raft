package main

import "sync"
import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-ywng/pb"
)

const (
	follower  = 1
	candidate = 2
	leader    = 3
	quit      = 4
)

type logEntry struct {
	Term    int64
	Index   int64
	Command *pb.Command
}

type voteInfo struct {
	mu         sync.Mutex
	voteRecord map[string]bool
	voteCount  int64
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput

	mu sync.Mutex //lock to protect shared access to this peer's state
	//persister 	*Persister

	state int64
	quorumSize int64

	//persistent states
	currentTerm int64
	votedFor string
	lastVoteTerm int64
	log []logEntry

	//volatile states
	commitIndex int64
	lastApplied int64

	//volatile states on leader
	nextIndex map[string] int64
	matchIndex map[string] int64

	// Snapshot
	lastIncludedLogEntry logEntry

	killServer chan int64
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

func (r *Raft) addLogEntry(entry logEntry) {
	r.log = append(r.log, entry)
}

func (r *Raft) getLogEntry(index int64) (logEntry, bool) {
	var le logEntry
	if r.getLogLen() == 0 {
		return le, false
	}
	firstIndex := r.log[0].Index
	if r.getLastLogIndex() < index || firstIndex > index {
		return le, false
	} else {
		return r.log[index-firstIndex], true
	}
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

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 4000
	const DurationMin = 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

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
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))
	heartBeatTicker := time.NewTicker(100 * time.Millisecond)
	if raft.state != leader {
		heartBeatTicker.Stop()
	}

	raft.mu.Lock()
	raft.quorumSize = int64(len(peerClients))/2 + 1
	raft.state = follower
	raft.currentTerm = 0
	raft.votedFor = ""
	raft.addLogEntry(logEntry{0, 0, nil})
	raft.mu.Unlock()


	var vote voteInfo

	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// The timer went off.
			log.Printf("Timeout, becomes a candidate requesting vote...")
			heartBeatTicker.Stop()

			raft.mu.Lock()
			raft.state = candidate
			raft.currentTerm++
			raft.votedFor = id
			lastLogIndex := raft.getLastLogIndex()
			lastLogTerm := int64(0)
			if lastLogIndex != 0 {
				lastLogTerm = raft.getLastLogTerm()
			}
			raft.mu.Unlock()

			vote = voteInfo{} //initialize vote info every time it becomes candidate
			vote.mu.Lock()
			vote.voteRecord = make(map[string]bool)
			for _, peer := range *peers {
				vote.voteRecord[peer] = false
			}
			vote.voteCount = 1
			vote.mu.Unlock()

			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					ret, err := c.RequestVote(context.Background(), 
								&pb.RequestVoteArgs{Term: raft.currentTerm, 
													CandidateID: id, 
													LastLogIndex: lastLogIndex, 
													LastLogTerm: lastLogTerm})
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			// this also means within timeout period without receiving majority votes, split votes etc... 
			// it will trigger the election process again
			restartTimer(timer, r)
		case op := <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command.
			s.HandleCommand(op) //do it only after commit
		case <- heartBeatTicker.C:
			//send heartbeat
			log.Printf("Heartbeat timeout ...")
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					ret, err := c.AppendEntries(context.Background(), 
								&pb.AppendEntriesArgs{Term: raft.currentTerm,
												  	  LeaderID: id,
												      PrevLogIndex: 0, 
												      PrevLogTerm: 0,
												      LeaderCommit: 0,
												      Entries: nil})
					appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}

		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			log.Printf("Received append entry from %v", ae.arg.LeaderID)
			ae.response <- pb.AppendEntriesRet{Term: 1, Success: true}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case vreq := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// TODO: Fix this.
			raft.mu.Lock()
			log.Printf("Received vote request from %v", vreq.arg.CandidateID)
			resp := pb.RequestVoteRet{
						Term: raft.currentTerm, 
						VoteGranted: false,
						CandidateID: id,
					}

			if vreq.arg.Term < raft.currentTerm {
				log.Printf("Rejecting vote request from %v since current term is greater than request vote term (%d, %d)", 
					vreq.arg.CandidateID, raft.currentTerm, vreq.arg.Term)
			} else if vreq.arg.Term > raft.currentTerm {
				resp.Term = vreq.arg.Term
				resp.VoteGranted = true
				//TO DO: if is leader, step down process
				if raft.state == leader {

				}
				raft.currentTerm = vreq.arg.Term
				raft.lastVoteTerm = vreq.arg.Term
				raft.votedFor = vreq.arg.CandidateID
			} else if raft.lastVoteTerm == vreq.arg.Term && raft.votedFor != "" {
				if raft.votedFor == vreq.arg.CandidateID {
					log.Printf("Duplicated vote request from %v", vreq.arg.CandidateID)
					resp.VoteGranted = true
				}
			} else {
				lastLogIndex := raft.getLastLogIndex()
				lastLogTerm := int64(0)
				if lastLogIndex != 0 {
					lastLogTerm = raft.getLastLogTerm()
				}

				if lastLogTerm > vreq.arg.LastLogTerm {
					log.Printf("Rejecting vote request from %v since our last term is greater (%d, %d)", 
						vreq.arg.CandidateID, lastLogTerm, vreq.arg.LastLogTerm)
				} else if lastLogTerm == vreq.arg.LastLogTerm  && lastLogIndex > vreq.arg.LastLogIndex {
					log.Printf("Rejecting vote request from %v since our last index is greater (%d, %d)", 
						vreq.arg.CandidateID, lastLogIndex, vreq.arg.LastLogIndex)
				} else {
					resp.VoteGranted = true
					raft.lastVoteTerm = vreq.arg.Term
					raft.votedFor = vreq.arg.CandidateID
				}
			}
			raft.mu.Unlock()

			vreq.response <- resp
			restartTimer(timer, r)

		case vres := <-voteResponseChan:
			// We received a response to a previou vote request.
			// TODO: Fix this

			// If this peer is elected as leader, should stop the timer?
			// When the leader crash-> restart / revert back to follower, need to restartTimer immediately.
			if vres.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vres.err)
			} else {
				log.Printf("Got response to vote request from %v", vres.peer)
				log.Printf("Peers %s granted %v term %v", vres.peer, vres.ret.VoteGranted, vres.ret.Term)

				raft.mu.Lock()
				//if not candidate state => already reached majority / reverted to follower
				if raft.state == candidate {
					//check if the term is greater than candidate's term
					if vres.ret.Term > raft.currentTerm {
						log.Printf("Newer term discovered, fallback to follower state.")
						//fallback to follower
						raft.currentTerm = vres.ret.Term
						raft.state = follower
					} else {
						vote.mu.Lock()
						if vote.voteRecord[vres.ret.CandidateID] == false {
							vote.voteRecord[vres.ret.CandidateID] = true
							vote.voteCount++
							if vote.voteCount >= raft.quorumSize {
								log.Printf("Won election. Granted votes: %d", vote.voteCount)
								raft.state = leader
								heartBeatTicker = time.NewTicker(100 * time.Millisecond)
								//initialise peer log status tracking
								raft.nextIndex = make(map[string]int64)
								raft.matchIndex = make(map[string]int64)
								index := raft.getLastLogIndex() + 1
								for _, peer := range *peers {
									raft.nextIndex[peer] = index
									raft.matchIndex[peer] = 0
								}
								timer.Stop()
							}
						}
						vote.mu.Unlock()
					}
				}
				raft.mu.Unlock()
			}

		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			log.Printf("Got append entries response from %v", ar.peer)
		}
	}
	log.Printf("Strange to arrive here")
}
