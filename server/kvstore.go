package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"time"

	context "golang.org/x/net/context"

	"github.com/sharded-raft/pb"
)

// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Result
}

// The struct for key value stores.
type KVStore struct {
	C     chan InputChannelType
	store map[string]string

	mu             sync.Mutex
	gid            int64
	config         *pb.ShardConfig
	migrateReceive map[int]bool
}

func (s *KVStore) ShardMigration(ctx context.Context, args *pb.ShardMigrationArgs) (*pb.ShardMigrationReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := &pb.ShardMigrationReply{Success: true}
	if args.ConfigNum > s.config.Num {
		reply.Success = false
	} else {
		reply.MigStore = make(map[string]string)
		for k, v := range s.store {
			if key2shard(k) == args.Shard {
				reply.MigStore[k] = v
			}
		}
	}

	return reply, nil
}

func (s *KVStore) Get(ctx context.Context, key *pb.Key) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for get response")
	select {
	case result := <-c:
		return &result, nil
	case <-time.After(5 * time.Second):
		return &pb.Result{Result: &pb.Result_Failure{Failure: &pb.Failure{Msg: "Request timeout."}}}, nil
	}
}

func (s *KVStore) Set(ctx context.Context, in *pb.KeyValue) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for set response")
	select {
	case result := <-c:
		return &result, nil
	case <-time.After(5 * time.Second):
		return &pb.Result{Result: &pb.Result_Failure{Failure: &pb.Failure{Msg: "Request timeout."}}}, nil
	}
}

func (s *KVStore) Clear(ctx context.Context, in *pb.Empty) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for clear response")
	select {
	case result := <-c:
		return &result, nil
	case <-time.After(5 * time.Second):
		return &pb.Result{Result: &pb.Result_Failure{Failure: &pb.Failure{Msg: "Request timeout."}}}, nil
	}
}

func (s *KVStore) CAS(ctx context.Context, in *pb.CASArg) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for CAS response")
	select {
	case result := <-c:
		return &result, nil
	case <-time.After(5 * time.Second):
		return &pb.Result{Result: &pb.Result_Failure{Failure: &pb.Failure{Msg: "Request timeout."}}}, nil
	}
}

func (s *KVStore) ChangeConfiguration(ctx context.Context, in *pb.Servers) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CONFIG_CHG, Arg: &pb.Command_Servers{Servers: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for CAS response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *KVStore) GetInternal(k string) pb.Result {
	if s.config.ShardsGroupMap.Gids[key2shard(k)] != s.gid {
		return pb.Result{Result: &pb.Result_WrongG{WrongG: &pb.ErrWrongGroup{Config: s.config}}}
	}
	v := s.store[k]
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *KVStore) SetInternal(k string, v string) pb.Result {
	if s.config.ShardsGroupMap.Gids[key2shard(k)] != s.gid {
		return pb.Result{Result: &pb.Result_WrongG{WrongG: &pb.ErrWrongGroup{Config: s.config}}}
	}
	s.store[k] = v
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally, this function clears a kv store. Assumes no racing calls.
func (s *KVStore) ClearInternal() pb.Result {
	s.store = make(map[string]string)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

// Used internally this function performs CAS assuming no races.
func (s *KVStore) CasInternal(k string, v string, vn string) pb.Result {
	if s.config.ShardsGroupMap.Gids[key2shard(k)] != s.gid {
		return pb.Result{Result: &pb.Result_WrongG{WrongG: &pb.ErrWrongGroup{Config: s.config}}}
	}
	vc := s.store[k]
	if vc == v {
		s.store[k] = vn
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vn}}}
	} else {
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vc}}}
	}
}

func (s *KVStore) HandleCommand(op InputChannelType, raft *Raft) {
	log.Printf("kv-store is handling committed command: %s", op.command.Operation)

	var result pb.Result
	var unrecognizedOp bool = false

	switch c := op.command; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result = s.GetInternal(arg.Key)
	case pb.Op_SET:
		arg := c.GetSet()
		result = s.SetInternal(arg.Key, arg.Value)
	case pb.Op_CLEAR:
		result = s.ClearInternal()
	case pb.Op_CAS:
		arg := c.GetCas()
		result = s.CasInternal(arg.Kv.Key, arg.Kv.Value, arg.Value.Value)
	case pb.Op_SHARD_CONFIG:
		//sharding logic
		newConfig := c.GetShardConfig()
		s.mu.Lock()
		if newConfig.Num == s.config.Num {
			s.mu.Unlock()
			break
		}
		log.Printf("kv-store group receives new sharding config, config: %v", newConfig)
		if newConfig.Num == 1 {
			s.config = newConfig
			if newConfig.Num < newConfig.MaxConfigNum {
				raft.migrating = true
			}
			s.mu.Unlock()
			break
		}
		s.migrateReceive = make(map[int]bool)
		for shard, gid := range s.config.ShardsGroupMap.Gids {
			newGid := newConfig.ShardsGroupMap.Gids[shard]
			//the shard was not in our storage and will need to be in our storage
			if gid != s.gid && newGid == s.gid {
				s.migrateReceive[shard] = false
			}
		}
		s.mu.Unlock()

		raft.migrating = true
		migrateDone := false
		for !migrateDone {
			migrateDone = true
			for shard, done := range s.migrateReceive {
				if !done {
					migrateDone = false
					args := &pb.ShardMigrationArgs{Shard: int64(shard), ConfigNum: newConfig.Num}
					gidOfShard := s.config.ShardsGroupMap.Gids[shard]
					log.Printf("shard: %v, gidOfShard: %v", shard, gidOfShard)
					//we probably need to make this time consuming part as go routine
					//so as not to block the raft main logic,
					//otherwise peer might timeout for election...
					for _, server := range s.config.Servers.Map[gidOfShard].List {
						kvc := establishConnectionKvStore(server)
						res, err := kvc.ShardMigration(context.Background(), args)
						if err == nil && res.Success {
							log.Printf("Received migrated data from: %v, res: %v", server, res)
							s.mu.Lock()
							if s.migrateReceive[shard] == false {
								for k, v := range res.MigStore {
									s.store[k] = v
								}
								s.migrateReceive[shard] = true
							}
							s.mu.Unlock()
							break
						} else if err != nil {
							log.Fatalf("Error when fetching migrated data from: %v, error: %v, res: %v", server, err, res)
						} else {
							log.Printf("Error when fetching migrated data from: %v, res: %v, success: %v, new config num: %v, max config num: %v",
								server, res, res.Success, newConfig.Num, newConfig.MaxConfigNum)
						}
					}
				}
			}
		}
		if newConfig.Num == newConfig.MaxConfigNum {
			raft.migrating = false
		}
		log.Printf("Done migration: %v, current config num: %v, max config num: %v", !raft.migrating, newConfig.Num, newConfig.MaxConfigNum)
		s.mu.Lock()
		s.config = newConfig
		s.mu.Unlock()

	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		result = pb.Result{}
		unrecognizedOp = true
	}

	//use select to do non-blocking send
	select {
	case op.response <- result:
		log.Printf("kv-store command completed and response is sent to client.")
	default:
		//no response is sent when non-leader is handling the command
		log.Printf("kv-store command completed and no response is sent to client.")
	}

	if unrecognizedOp {
		log.Fatalf("Unrecognized operation %v", op.command.Operation)
	}
}

func (s *KVStore) ApplySnapshot(snapshot []byte) {
	data := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(data)
	decoder.Decode(&s.store)
}
