package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-ywng/pb"
)

type voteInfo struct {
	mu         sync.Mutex
	voteRecord map[string]bool
	voteCount  int64
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// launch a GRPC service for this Raft peer
func RunRaftServer(r *Raft, port int) {
	portString := fmt.Sprintf(":%d", port)
	// create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		log.Fatalf("Could not create listening socket %v", err)
	}
	// create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func getPeerClients(peers *arrayPeers) map[string]pb.RaftClient {
	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}
	return peerClients
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

func newVoteCounter(peers *arrayPeers) voteInfo {
	vote := voteInfo{}
	vote.mu.Lock()
	vote.voteRecord = make(map[string]bool)
	for _, peer := range *peers {
		vote.voteRecord[peer] = false
	}
	vote.voteCount = 1
	vote.mu.Unlock()
	return vote
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)
	peerClients := getPeerClients(peers)

	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	raft.mu.Lock()
	raft.randSeed = r
	raft.peers = peers
	raft.timer = time.NewTimer(randomDuration(r))
	raft.heartBeatTicker = time.NewTicker(100 * time.Millisecond) //100ms heartbeat interval
	raft.me = id
	if len(peerClients)%2 == 0 {
		raft.quorumSize = int64(len(peerClients))/2 + 1
	} else {
		raft.quorumSize = int64(len(peerClients))/2 + 2 //e.g. 4 total nodes will need 3 to form a quorum
	}
	raft.currentTerm = 0
	raft.lastApplied = int64(-1)
	raft.votedFor = ""
	raft.addLogEntry(&pb.LogEntry{Term: 0, Index: 0, Command: nil})
	// to start as follower
	raft.fallbackToFollower()
	raft.mu.Unlock()

	// to track voting count
	var vote voteInfo

	// Run forever handling inputs from various channels
	for {
		select {
		/** election timeout -> candidate **/
		case <-raft.timer.C:
			log.Printf("Election timeout: %s becomes a candidate requesting vote.", raft.me)

			//initialize vote info every time it becomes candidate
			vote = newVoteCounter(peers)

			//send a vote request to all peers
			raft.sendVoteRequests(peerClients, voteResponseChan)

			// This will also take care of any pesky timeouts that happened while processing the operation.
			// this also means within timeout period without receiving majority votes, split votes etc...
			// it will trigger the election process again
			raft.restartTimer()

		/** client request handling **/
		case op := <-s.C:
			raft.mu.Lock()
			if raft.state == leader {
				index := raft.getLastLogIndex() + 1
				//add the client request to the leader's log first (but it is not yet committed)
				raft.addLogEntry(&pb.LogEntry{Term: raft.currentTerm, Index: index, Command: &op.command})
				raft.clientsResponse[index] = op.response
				//instantly send append entry after receiving client request and added to leader's log
				raft.sendApeendEntries(peerClients, appendResponseChan)
			} else {
				//redirect result to send the client to the right leader
				op.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: raft.leader}}}
			}
			raft.mu.Unlock()

		/** send heartbeats to followers to maintain authority **/
		case <-raft.heartBeatTicker.C:
			log.Printf("Heartbeat timeout ...")

			//the sendApeendEntries function will determine if the message
			//will be heartbeat or carring a log to be replicated
			raft.sendApeendEntries(peerClients, appendResponseChan)

		/** handle append entry request from other raft peers **/
		case ae := <-raft.AppendChan:
			log.Printf("Received append entry from %v.", ae.arg.LeaderID)

			raft.mu.Lock()
			res := pb.AppendEntriesRet{Term: raft.currentTerm, Success: false}
			//reject appendEntries if our current term is larger
			if ae.arg.Term < raft.currentTerm {
				res.Term = raft.currentTerm
				res.Success = false
				ae.response <- res
				raft.mu.Unlock()
				break
			}

			//increase the term if we see a newer one,
			//and transit to follower if we ever get an appendEntries call & the term is >= ours
			if ae.arg.Term > raft.currentTerm || raft.state != follower {
				raft.fallbackToFollower()
				raft.currentTerm = ae.arg.Term

				res.Term = ae.arg.Term
			}

			//save the current leader
			raft.leader = ae.arg.LeaderID

			//Verify the last log entry
			if ae.arg.PrevLogIndex > 0 {
				lastLogIndex := raft.getLastLogIndex()
				lastLogTerm := raft.getLastLogTerm()

				var prevLogTerm int64
				if ae.arg.PrevLogIndex == lastLogIndex {
					prevLogTerm = lastLogTerm
				} else {
					entry, _ := raft.getLogEntry(ae.arg.PrevLogIndex)
					prevLogTerm = entry.Term
				}

				if ae.arg.PrevLogTerm != prevLogTerm {
					log.Printf("Previous log term mis-match: ours: %d remote: %d",
						prevLogTerm, ae.arg.PrevLogTerm)
					res.Success = false
				}
			}

			//process any new entries
			if len(ae.arg.Entries) > 0 {
				//delete any conflicting entries, skip duplicates
				lastLogIndex := raft.getLastLogIndex()
				var newEntries []*pb.LogEntry
				for i, entry := range ae.arg.Entries {
					if entry.Index > lastLogIndex {
						newEntries = ae.arg.Entries[i:1]
						break
					}
					storeEntry, _ := raft.getLogEntry(entry.Index)
					if entry.Term != storeEntry.Term {
						log.Printf("Clearing log suffix from %d to %d", entry.Index, lastLogIndex)
						raft.deleteEntryFrom(entry.Index)
						break
					}
					newEntries = ae.arg.Entries[i:]
				}

				if n := len(newEntries); n > 0 {
					//append the new entries
					for _, entry := range newEntries {
						raft.addLogEntry(entry)
					}
				}
			}

			//update the commit index
			if ae.arg.LeaderCommit > 0 && ae.arg.LeaderCommit > raft.commitIndex {
				index := min(raft.getLastLogIndex(), ae.arg.LeaderCommit)
				raft.commitIndex = index

				//process the committed log entries if any
				raft.ProcessLogs(s)
			}

			res.Success = true
			ae.response <- res

			raft.mu.Unlock()

			// This will also take care of any pesky timeouts that happened while processing the operation.
			raft.restartTimer()

		/** handle vote request from other raft peers **/
		case vreq := <-raft.VoteChan:
			raft.mu.Lock()
			log.Printf("Received vote request from %v", vreq.arg.CandidateID)
			resp := pb.RequestVoteRet{
				Term:        raft.currentTerm,
				VoteGranted: false,
				CandidateID: id,
			}

			if vreq.arg.Term < raft.currentTerm {
				log.Printf("Rejecting vote request from %v since current term is greater than request vote term (%d, %d)",
					vreq.arg.CandidateID, raft.currentTerm, vreq.arg.Term)
			} else if vreq.arg.Term > raft.currentTerm {
				resp.Term = vreq.arg.Term
				resp.VoteGranted = true
				// if is leader, step down process
				if raft.state == leader {
					raft.fallbackToFollower()
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
				} else if lastLogTerm == vreq.arg.LastLogTerm && lastLogIndex > vreq.arg.LastLogIndex {
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
			raft.restartTimer()

		/** handle vote response from other raft peers **/
		case vres := <-voteResponseChan:
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
						raft.fallbackToFollower()
					} else {
						vote.mu.Lock()
						if vote.voteRecord[vres.ret.CandidateID] == false {
							vote.voteRecord[vres.ret.CandidateID] = true
							vote.voteCount++
							if vote.voteCount >= raft.quorumSize {
								log.Printf("Won election. Granted votes: %d", vote.voteCount)
								// to be a leader, and leader state prep
								raft.leaderStatePrep()
							}
						}
						vote.mu.Unlock()
					}
				}
				raft.mu.Unlock()
			}

		/** handle append entry response from other raft peers **/
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", ar.err)
			} else {
				raft.mu.Lock()
				if raft.state == leader {
					if ar.ret.Success {
						log.Printf("Got success append entries response from %v", ar.peer)

						prevLogIndex := raft.nextIndex[ar.peer] - 1
						raft.nextIndex[ar.peer] = prevLogIndex + raft.appendedEntriesLen[ar.peer] + 1
						raft.matchIndex[ar.peer] = prevLogIndex + raft.appendedEntriesLen[ar.peer]
						n := raft.matchIndex[ar.peer]
						//the matched index is beyond leader's commitIndex and it is in leader's current term
						//if majority is reached, it is saved to commit that matchedIndex
						if entry, _ := raft.getLogEntry(n); n > raft.commitIndex && entry.Term == raft.currentTerm {
							matchCount := int64(1)
							for _, peer := range *peers {
								if raft.matchIndex[peer] >= n {
									matchCount++
								}
							}
							if matchCount > raft.quorumSize {
								raft.commitIndex = n
								//apply to State Machine
								raft.ProcessLogs(s)
							}
						}
					} else {
						log.Printf("Got failed append entries response from %v", ar.peer)

						//if fail, decrement nextIndex for that peer
						//and retry append entry
						raft.nextIndex[ar.peer]--
						raft.sendApeendEntriesTo(ar.peer, peerClients[ar.peer], appendResponseChan)
					}
				}

				raft.mu.Unlock()
			}
		}
	}

	log.Printf("Strange to arrive here")
}
