/*
	Author: ywn202@nyu.edu
	Support Raft to save persistent states.
*/

package main

import (
	"sync"
)

type Persister struct {
	mu sync.Mutex

	//the persistent fields as stated in the paper:
	//currentTerm, votedFor, log[]
	raftState []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (p *Persister) SaveRaftState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = data
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raftState
}

func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func (p *Persister) SaveSnapshot(snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot = snapshot
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}

func (p *Persister) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}
