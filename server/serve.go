package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"
	//"sync/atomic"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-ywng/pb"
)

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




	//vote count
	//var isLeader bool = false
	var votesGranted int = 0
	var quorumSize int = len(peerClients)/2 + 1

	var currentTerm int64 = 0
	var votedFor string
	var lastVoteTerm int64 = 0
	var lastTerm, lastIndex int64 = 0, 0


	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// The timer went off.
			log.Printf("Timeout, becomes a candiate requesting vote...")
			votesGranted = 1 //vote for itself
			currentTerm++
			lastVoteTerm = currentTerm
			votedFor = id
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					ret, err := c.RequestVote(context.Background(), 
								&pb.RequestVoteArgs{Term: currentTerm, CandidateID: id, LastLogIndex: lastIndex, LastLogTerm: lastTerm})
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
			s.HandleCommand(op)
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
			log.Printf("Received vote request from %v", vreq.arg.CandidateID)
			resp := pb.RequestVoteRet{
						Term: currentTerm, 
						VoteGranted: false,
					}

			if vreq.arg.Term < currentTerm {
				log.Printf("Rejecting vote request from %v since current term is greater than request vote term (%d, %d)", 
					vreq.arg.CandidateID, currentTerm, vreq.arg.Term)
			} else if vreq.arg.Term > currentTerm {
				resp.Term = vreq.arg.Term
				resp.VoteGranted = true
				//isLeader = false
				//if is leader, step down
				currentTerm = vreq.arg.Term
				lastVoteTerm = vreq.arg.Term
				votedFor = vreq.arg.CandidateID
			} else if lastVoteTerm == vreq.arg.Term && votedFor != "" {
				if votedFor == vreq.arg.CandidateID {
					log.Printf("Duplicated vote request from %v", vreq.arg.CandidateID)
					resp.VoteGranted = true
				}
			} else {
				if lastTerm > vreq.arg.LastLogTerm {
					log.Printf("Rejecting vote request from %v since our last term is greater (%d, %d)", 
						vreq.arg.CandidateID, lastTerm, vreq.arg.LastLogTerm)
				} else if lastTerm == vreq.arg.LastLogTerm  && lastIndex > vreq.arg.LastLogIndex {
					log.Printf("Rejecting vote request from %v since our last index is greater (%d, %d)", 
						vreq.arg.CandidateID, lastIndex, vreq.arg.LastLogIndex)
				} else {
					resp.VoteGranted = true
					lastVoteTerm = vreq.arg.Term
					votedFor = vreq.arg.CandidateID
				}
			}

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

				//check if the term is greater than candidate's term
				if vres.ret.Term > currentTerm {
					log.Printf("Newer term discovered, fallback to follower state.")
					//fallback to follower
					votesGranted = 0
					currentTerm = vres.ret.Term
					//isLeader = false
				} else {
					if vres.ret.VoteGranted {
						votesGranted++;
					}

					if votesGranted >= quorumSize {
						log.Printf("Won election. Granted votes: %d", votesGranted)
						timer.Stop()
						//isLeader = true
					}
				}
			}

		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			log.Printf("Got append entries response from %v", ar.peer)
		}
	}
	log.Printf("Strange to arrive here")
}
