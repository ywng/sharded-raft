package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/sharded-raft/pb"
)

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

// The main service loop. All modifications to the KV store are run through here.
func serve(sm *ShardMaster, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput),
		VoteChan:            make(chan VoteInput),
		InstallSnapshotChan: make(chan InstallSnapshotInput)}
	// start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)
	//peerClients := getPeerClients(peers)

	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)
	snapshotResponseChan := make(chan InstallSnapshotResponse)

	//sm server starts with a dummy invalid entry
	var shardMapping []int64
	dummyShardConfig := &pb.ShardConfig{Num: 0, ShardsGroupMap: &pb.ShardsMapping{Gids: shardMapping}}
	for shardId := 0; shardId < NShards; shardId++ {
		dummyShardConfig.ShardsGroupMap.Gids = append(dummyShardConfig.ShardsGroupMap.Gids, 0)
	}
	serverMap := make(map[int64]*pb.ServerList)
	dummyShardConfig.Servers = &pb.GroupServersMap{Map: serverMap}
	sm.configs = append(sm.configs, dummyShardConfig)

	raft.mu.Lock()
	raft.randSeed = r
	raft.peers = peers
	raft.persister = MakePersister()
	raft.killServer = make(chan int64)
	raft.electionTimer = time.NewTimer(randomDuration(r))
	raft.heartBeatTimer = time.NewTimer(HEARTBEAT_TIMEOUT * time.Millisecond)
	raft.me = id
	raft.currentTerm = 0
	raft.commitIndex = 0
	raft.lastApplied = 0
	raft.votedFor = ""

	serverList := peers.Clone()
	serverList.Set(id) // the configuration should include the current server itself
	startupConfig := Configuration{servers: serverList}
	raft.configurations = Configurations{config: startupConfig, lastConfigLogIndex: 0, stable: true}
	oldConfigCmd := &pb.Command{Operation: pb.Op_CONFIG_CHG,
		Arg: &pb.Command_Servers{Servers: &pb.Servers{CurrList: startupConfig.servers.String()}}}
	raft.addLogEntry(&pb.Entry{Term: 0, Index: 0, Cmd: oldConfigCmd})
	//first dummy term for indexing convenience
	//raft.addLogEntry(&pb.Entry{Term: 0, Index: 0, Cmd: nil})

	log.Printf("Current configuration servers: %v", raft.getServerList())
	raft.updateConfiguration()
	raft.updatePeerClients()
	raft.updateQuorumSize()

	//start as follower with an election timeout
	raft.fallbackToFollower()

	raft.mu.Unlock()

	// to track voting count
	var vote voteInfo

	// Run forever handling inputs from various channels
	for {
		select {
		/** election timeout -> candidate **/
		case <-raft.electionTimer.C:
			log.Printf("Election timeout: %s becomes a candidate requesting vote.", raft.me)

			//initialize vote info every time it becomes candidate
			vote = raft.newVoteCounter()

			//send a vote request to all peers
			raft.sendVoteRequests(raft.peerClients, voteResponseChan)

			// This will also take care of any pesky timeouts that happened while processing the operation.
			// this also means within timeout period without receiving majority votes, split votes etc...
			// it will trigger the election process again
			restartTimer(raft.electionTimer, randomDuration(raft.randSeed))
		/** client request handling **/
		case op := <-sm.C:
			//raft.mu.Lock()
			if raft.state == leader {
				index := raft.getLastLogIndex() + 1
				log.Printf("Receive client request, command: %s, assignedIndex: %v.", op.command.Operation, index)

				raft.mu.Lock()

				if op.command.Operation == pb.Op_CONFIG_CHG {

					log.Printf("Change configuration request. %v", op.command.GetServers())

					if !raft.isEqualToCurrentServerList(op.command.GetServers().CurrList) {
						//First, verify the provided currList servers is matching
						log.Printf("The provided current list of servers is not matching the record.")
						op.response <- pb.Result{Result: &pb.Result_Failure{Failure: &pb.Failure{Msg: "The provided current list of servers is not matching the record"}}}

					} else if !raft.configurations.stable {
						//should reject client's config changes request if we are currently having one
						log.Printf("There is already a pending change configurations request.")
						op.response <- pb.Result{Result: &pb.Result_Failure{Failure: &pb.Failure{Msg: "There is already a pending change configurations request."}}}

					} else {
						//var servers arrayPeers
						//servers.SetArray(strings.Split(op.command.GetServers().ServerList, ","))
						//raft.configurations.new = Configuration{servers: &servers}
						//raft.configurations.genMergedConfiguration()
						//cmdOfMergedConfig := &pb.Command{Operation: pb.Op_CONFIG_CHG,
						//Arg: &pb.Command_Servers{Servers: &pb.Servers{ServerList: raft.configurations.new.servers.String()}}}
						raft.addLogEntry(&pb.Entry{Term: raft.currentTerm, Index: index, Cmd: &op.command})
						raft.clientsResponse[index] = op.response
						//raft.configurations.oldNewLogIndex = index
						raft.configurations.lastConfigLogIndex = index
						raft.configurations.stable = false

						raft.updateConfiguration()
						raft.updatePeerClients()
						raft.updateQuorumSize()
						raft.updateLeaderVolatileStatesAfterConfigChange()
					}

				} else {
					//add the client request to the leader's log first (but it is not yet committed)
					raft.addLogEntry(&pb.Entry{Term: raft.currentTerm, Index: index, Cmd: &op.command})
					raft.clientsResponse[index] = op.response
				}

				raft.persist()
				raft.mu.Unlock()

				//instantly send append entry after receiving client request and added to leader's log
				log.Printf("Trigger append entries request to peers immediately after receving the client request.")
				raft.sendApeendEntries(raft.peerClients, appendResponseChan, snapshotResponseChan)

				//log.Printf("raft.state: %d", raft.state)
			} else {
				//redirect result to send the client to the right leader
				log.Printf("Peer %s is not leader, redirecting client request to leader %s.", raft.me, raft.leader)
				op.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: strings.Split(raft.leader, ":")[0]}}}
			}
			//raft.mu.Unlock()

		/** send heartbeats to followers to maintain authority **/
		case <-raft.heartBeatTimer.C:
			log.Printf("Heartbeat timeout ...")

			//log.Printf("raft.state: %d", raft.state)

			//the sendApeendEntries function will determine if the message
			//will be heartbeat or carring a log to be replicated
			raft.sendApeendEntries(raft.peerClients, appendResponseChan, snapshotResponseChan)

			restartTimer(raft.heartBeatTimer, HEARTBEAT_TIMEOUT*time.Millisecond)

		/** handle append entry request from other raft peers **/
		case ae := <-raft.AppendChan:
			raft.mu.Lock()
			log.Printf("Received append entry from %v.", ae.arg.LeaderID)

			if !raft.isPeer(ae.arg.LeaderID) { //ignore request from non peer
				raft.mu.Unlock()
				break
			}

			res := pb.AppendEntriesRet{
				Term:    raft.currentTerm,
				Success: true, //first default it to true
			}

			//reject appendEntries if our current term is larger
			//not from current leader, should NOT reset election timer
			if ae.arg.Term < raft.currentTerm {
				res.Term = raft.currentTerm
				res.Success = false
			} else {
				//increase the term if we see a newer one,
				//and transit to follower if we ever get an appendEntries call & the term is >= ours
				if ae.arg.Term > raft.currentTerm || raft.state != follower {
					log.Printf("Append Entry Request from %v: current term is older (%d vs %d) or it is not follower, fall back to follower.",
						ae.arg.LeaderID, raft.currentTerm, ae.arg.Term)
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
					//log.Printf("Peer: %s, lastLogIndex: %d, lastLogTerm: %d, commitIndex: %d.", raft.me, lastLogIndex, lastLogTerm, raft.commitIndex)

					var prevLogTerm int64 = -1
					if ae.arg.PrevLogIndex == lastLogIndex {
						prevLogTerm = lastLogTerm
					} else if ae.arg.PrevLogIndex > lastLogIndex {
						//if get an AppendEntries with a prevLogIndex beyond th end of the log
						//same as the term did not match
						res.Success = false
						prevLogTerm = lastLogTerm
					} else if entry, ok := raft.getLogEntry(ae.arg.PrevLogIndex); ok {
						//if the PrevLogIndex < lastSnapshotLogEntry.Index,
						//we just not process this append entry request, and assume fail
						prevLogTerm = entry.Term
					}

					if ae.arg.PrevLogTerm != prevLogTerm {
						log.Printf("Previous log term mis-match: ours: %d remote: %d",
							prevLogTerm, ae.arg.PrevLogTerm)
						res.Success = false
					}
				}

				//process any new entries if we haven't failed any check
				//only if an existing entry conflicts with a new one (non-heartbeat),
				//delete the existing entry and all that follow it
				if res.Success && len(ae.arg.Entries) > 0 {
					//delete any conflicting entries, skip duplicates
					lastLogIndex := raft.getLastLogIndex()
					var newEntries []*pb.Entry
					for i, entry := range ae.arg.Entries {
						if entry.Index > lastLogIndex {
							newEntries = ae.arg.Entries[i:]
							break
						}
						storeEntry, _ := raft.getLogEntry(entry.Index)
						if entry.Term != storeEntry.Term {
							log.Printf("Clearing log suffix from %d to %d", entry.Index, lastLogIndex)
							raft.deleteEntryFrom(entry.Index)
							newEntries = ae.arg.Entries[i:]
							break
						}
					}

					if n := len(newEntries); n > 0 {
						//append the new entries
						for _, entry := range newEntries {
							raft.addLogEntry(entry)
							if entry.Cmd.Operation == pb.Op_CONFIG_CHG {
								if entry.Cmd.GetServers().GetNewList() != "" {
									raft.configurations.stable = false
								} else {
									raft.configurations.stable = true
								}
								raft.configurations.lastConfigLogIndex = entry.Index
								raft.updateConfiguration()
								raft.updatePeerClients()
								raft.updateQuorumSize()
							}

							log.Printf("Entry appended to peer: %s, index: %d, command: %s.", raft.me, entry.Index, entry.Cmd.Operation)
						}
					}
				}

				//update the commit index if we haven't failed any check
				if res.Success && ae.arg.LeaderCommit > raft.commitIndex {
					index := min(raft.getLastLogIndex(), ae.arg.LeaderCommit)
					raft.commitIndex = index
					//log.Printf("Peer: %s, commitIndex: %d.", raft.me, raft.commitIndex)

					//process the committed log entries if any
					raft.ProcessLogs(sm)
				}

				//received AppendEntries RPC from current leader, restart election timer
				if res.Success {
					restartTimer(raft.electionTimer, randomDuration(raft.randSeed))
				}

				raft.persist()
			}

			raft.mu.Unlock()
			ae.response <- res

		/** handle vote request from other raft peers **/
		case vreq := <-raft.VoteChan:
			raft.mu.Lock()
			if !raft.isPeer(vreq.arg.CandidateID) { //ignore request from non peer
				raft.mu.Unlock()
				break
			}
			log.Printf("Received vote request from %v", vreq.arg.CandidateID)

			resp := pb.RequestVoteRet{
				Term:        raft.currentTerm,
				VoteGranted: false,
			}

			if vreq.arg.Term < raft.currentTerm {
				log.Printf("Rejecting vote request from %v since current term is greater than request vote term (%d vs %d)",
					vreq.arg.CandidateID, raft.currentTerm, vreq.arg.Term)
			} else if raft.lastVoteTerm == vreq.arg.Term && raft.votedFor != vreq.arg.CandidateID {
				log.Printf("Rejecting vote request from %v since already voted for %s for vote term %d.",
					vreq.arg.CandidateID, raft.votedFor, vreq.arg.Term)
			} else {
				lastLogIndex := raft.getLastLogIndex()
				lastLogTerm := int64(0)
				if lastLogIndex != 0 {
					lastLogTerm = raft.getLastLogTerm()
				}

				if lastLogTerm > vreq.arg.LasLogTerm {
					log.Printf("Rejecting vote request from %v since our last term is greater (%d vs %d)",
						vreq.arg.CandidateID, lastLogTerm, vreq.arg.LasLogTerm)
				} else if lastLogTerm == vreq.arg.LasLogTerm && lastLogIndex > vreq.arg.LastLogIndex {
					log.Printf("Rejecting vote request from %v since our last index is greater (%d vs %d)",
						vreq.arg.CandidateID, lastLogIndex, vreq.arg.LastLogIndex)
				} else {
					resp.VoteGranted = true
					raft.votedFor = vreq.arg.CandidateID
					raft.lastVoteTerm = vreq.arg.Term

					if vreq.arg.Term > raft.currentTerm {
						log.Printf("Vote Request from %v: current term is older (%d vs %d), fall back to follower.",
							vreq.arg.CandidateID, raft.currentTerm, vreq.arg.Term)
						resp.Term = vreq.arg.Term
						raft.currentTerm = vreq.arg.Term
						// if is leader/candidate, step down process
						if raft.state == leader || raft.state == candidate {
							raft.fallbackToFollower()
						}
					}

					//vote granted to candidate, only then reset election timer
					//so servers with the more up-to-datelogs won't be interrupted by outdated servers' elections
					//less likely of live locks
					restartTimer(raft.electionTimer, randomDuration(raft.randSeed))

					raft.persist()
				}
			}

			raft.mu.Unlock()
			vreq.response <- resp

		/** handle install snapshot request from other raft peers **/
		case installSnapshotReq := <-raft.InstallSnapshotChan:
			raft.mu.Lock()
			if !raft.isPeer(installSnapshotReq.arg.LeaderID) { //ignore request from non peer
				raft.mu.Unlock()
				break
			}
			log.Printf("Received install snapshot request from %v", installSnapshotReq.arg.LeaderID)

			resp := pb.InstallSnapshotRet{
				Term:    raft.currentTerm,
				Success: true, //first default it to true
			}

			//reject appendEntries if our current term is larger
			//not from current leader, should NOT reset election timer
			if installSnapshotReq.arg.Term < raft.currentTerm {
				resp.Success = false
			} else if installSnapshotReq.arg.LastLogEntry.Index <= raft.getFirstLogIndex() ||
				installSnapshotReq.arg.LastLogEntry.Index <= raft.lastApplied {
				//peer itself already did the compaction, ignore the installsnapshot request
				//or, the snapshot content is already included
				//we will return success to signal leader to update nextIndex, but no log update is required here.
				log.Printf("Install snapshot ignored, lastIncludedIndex: %v, firstLogIndex: %v, lastApplied: %v.",
					installSnapshotReq.arg.LastLogEntry.Index, raft.getFirstLogIndex(), raft.lastApplied)
			} else {
				//increase the term if we see a newer one,
				//and transit to follower if we ever get an installsnapshot call & the term is >= ours
				if installSnapshotReq.arg.Term > raft.currentTerm || raft.state != follower {
					log.Printf("InstallSnapshot Request from %v: current term is older (%d vs %d) or it is not follower, fall back to follower.",
						installSnapshotReq.arg.LeaderID, raft.currentTerm, installSnapshotReq.arg.Term)
					raft.fallbackToFollower()
					raft.currentTerm = installSnapshotReq.arg.Term
					resp.Term = installSnapshotReq.arg.Term
				}

				//install snapshot
				log.Printf("Installing snapshot, lastIncludedIndex: %v", installSnapshotReq.arg.LastLogEntry.Index)
				raft.persister.SaveSnapshot(installSnapshotReq.arg.Data)
				raft.lastSnapshotLogEntry = installSnapshotReq.arg.LastLogEntry

				entry, ok := raft.getLogEntry(raft.lastSnapshotLogEntry.Index)
				//if existing log entry has same index and term as snapshot's last included entry,
				//retain log entries following it
				if ok && entry.Term == raft.lastSnapshotLogEntry.Term {
					raft.log = raft.getEntryFrom(entry.Index)
				} else {
					raft.deleteAllEntries()
				}

				sm.ApplySnapshot(installSnapshotReq.arg.Data)
				raft.lastApplied = raft.lastSnapshotLogEntry.Index
				raft.persist()

				//save the current leader
				raft.leader = installSnapshotReq.arg.LeaderID
			}

			//received valid install snapshot RPC from current leader, restart election timer
			if resp.Success {
				restartTimer(raft.electionTimer, randomDuration(raft.randSeed))
			}

			raft.mu.Unlock()
			installSnapshotReq.response <- resp

		/** handle vote response from other raft peers **/
		case vres := <-voteResponseChan:
			if vres.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Vote request RPC call error (%s): %v", vres.peer, vres.err)
			} else {
				raft.mu.Lock()
				if !raft.isPeer(vres.peer) { //ignore request from non peer
					raft.mu.Unlock()
					break
				}
				log.Printf("Got response to vote request from %v", vres.peer)
				log.Printf("Peers %s granted %v. The peer's current term is %v", vres.peer, vres.ret.VoteGranted, vres.ret.Term)

				//if not candidate state => already reached majority / reverted to follower
				if raft.state == candidate {
					//Term confusion: drop any reply that the request was in an older term
					if vres.requestTerm < raft.currentTerm {
						raft.mu.Unlock()
						break
					}

					//check if the term is greater than candidate's term
					if vres.ret.Term > raft.currentTerm {
						log.Printf("Vote Response from %v: current term is older (%d vs %d), fall back to follower.",
							vres.peer, raft.currentTerm, vres.ret.Term)
						//fallback to follower
						raft.currentTerm = vres.ret.Term
						raft.fallbackToFollower()

						raft.persist()
					} else if vres.ret.Term == raft.currentTerm && vres.ret.VoteGranted {
						vote.mu.Lock()
						if vote.voteRecord[vres.peer] == false {
							vote.voteRecord[vres.peer] = true
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

				//log.Printf("raft.state: %d", raft.state)
				raft.mu.Unlock()
			}

		/** handle append entry response from other raft peers **/
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Append entry request RPC call error (%s): %v", ar.peer, ar.err)
			} else {
				raft.mu.Lock()
				if !raft.isPeer(ar.peer) { //ignore request from non peer
					raft.mu.Unlock()
					break
				}
				if raft.state == leader {
					//Term confusion: drop any reply that the request was in an older term
					if ar.requestTerm < raft.currentTerm {
						raft.mu.Unlock()
						break
					}

					//if replied term > leader current term, fall back to follower
					if ar.ret.Term > raft.currentTerm {
						log.Printf("Append Entry Response from %v: current term is older (%d vs %d), fall back to follower.",
							ar.peer, raft.currentTerm, ar.ret.Term)
						raft.currentTerm = ar.ret.Term
						raft.fallbackToFollower()

						raft.persist()

						raft.mu.Unlock()
						break
					}

					if ar.ret.Success {
						log.Printf("Got success append entries response from %v", ar.peer)

						raft.nextIndex[ar.peer] = max(raft.nextIndex[ar.peer], ar.matchIndex+1)
						raft.matchIndex[ar.peer] = max(raft.matchIndex[ar.peer], ar.matchIndex)
						n := raft.matchIndex[ar.peer]
						log.Printf("peer: %s, peer_matchIndex: %d, peer_nextIndex: %d, leaderCommitIndex: %d.",
							ar.peer, raft.matchIndex[ar.peer], raft.nextIndex[ar.peer], raft.commitIndex)
						//the matched index is beyond leader's commitIndex and it is in leader's current term (Figure 8 in the paper)
						//if majority is reached, it is safe to commit that matchedIndex
						if entry, _ := raft.getLogEntry(n); n > raft.commitIndex &&
							(entry.Term == raft.currentTerm || entry.Cmd.Operation == pb.Op_CONFIG_CHG) {

							matchCount := int64(0)
							if raft.isPeer(raft.me) { //only if the leader is in current config, count itself
								matchCount = int64(1)
							}

							log.Printf("entry: %s.", entry)
							for _, peer := range *raft.getServerList() {
								if raft.matchIndex[peer] >= n {
									matchCount++
								}
							}

							log.Printf("matchCount: %d, quorumSize: %d.", matchCount, raft.quorumSize)
							if matchCount >= raft.quorumSize {
								raft.commitIndex = n
								//apply to state machine
								raft.ProcessLogs(sm)
							}
						}
					} else {
						log.Printf("Got failed append entries response from peer:%v, peer's term: %d", ar.peer, ar.ret.Term)

						//if fail, decrement nextIndex for that peer
						//and retry append entry
						raft.nextIndex[ar.peer]--
						raft.sendApeendEntriesTo(ar.peer, raft.peerClients[ar.peer], appendResponseChan, snapshotResponseChan)
					}
				}
				//log.Printf("raft.state: %d", raft.state)

				raft.mu.Unlock()
			}

		/** handle install snapshot response from other raft peers **/
		case installSnapshotResp := <-snapshotResponseChan:
			if installSnapshotResp.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Install snapshot request RPC call error (%s): %v", installSnapshotResp.peer, installSnapshotResp.err)
			} else {
				raft.mu.Lock()
				if !raft.isPeer(installSnapshotResp.peer) { //ignore request from non peer
					raft.mu.Unlock()
					break
				}
				if raft.state == leader {
					//Term confusion: drop any reply that the request was in an older term
					if installSnapshotResp.requestTerm < raft.currentTerm {
						raft.mu.Unlock()
						break
					}

					//if replied term > leader current term, fall back to follower
					if installSnapshotResp.ret.Term > raft.currentTerm {
						log.Printf("Install Snapshot Response from %v: current term is older (%d vs %d), fall back to follower.",
							installSnapshotResp.peer, raft.currentTerm, installSnapshotResp.ret.Term)
						raft.currentTerm = installSnapshotResp.ret.Term
						raft.fallbackToFollower()

						raft.persist()

						raft.mu.Unlock()
						break
					}

					if installSnapshotResp.ret.Success {
						log.Printf("Successfully install snapshot for peer %v", installSnapshotResp.peer)

						raft.nextIndex[installSnapshotResp.peer] = max(raft.nextIndex[installSnapshotResp.peer], raft.lastSnapshotLogEntry.Index+1)
						raft.matchIndex[installSnapshotResp.peer] = max(raft.matchIndex[installSnapshotResp.peer], raft.lastSnapshotLogEntry.Index)

					} else {
						log.Printf("Install snapshot failed for peer %v", installSnapshotResp.peer)

					}
				}

				raft.mu.Unlock()
			}

		/** the server should be shut down **/
		case <-raft.killServer:
			log.Printf("Shutting down server: %v", raft.me)

			time.Sleep(2 * time.Second)
			os.Exit(0)
		}
	}

	log.Printf("Strange to arrive here")
}
