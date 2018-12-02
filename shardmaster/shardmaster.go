package main

import (
	"bytes"
	"encoding/gob"
	"log"

	context "golang.org/x/net/context"

	"github.com/sharded-raft/pb"
)

const (
	NShards = 10
)

// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Result
}

// The struct for key value stores.
type ShardMaster struct {
	C       chan InputChannelType
	configs []*pb.ShardConfig // indexed by config num
}

func (sm *ShardMaster) Join(ctx context.Context, joinArgs *pb.JoinArgs) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_JOIN, Arg: &pb.Command_JoinArgs{JoinArgs: joinArgs}}
	sm.C <- InputChannelType{command: r, response: c}
	result := <-c
	return &result, nil
}

func (sm *ShardMaster) Leave(ctx context.Context, leaveArgs *pb.LeaveArgs) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_LEAVE, Arg: &pb.Command_LeaveArgs{LeaveArgs: leaveArgs}}
	sm.C <- InputChannelType{command: r, response: c}
	result := <-c
	return &result, nil
}

func (sm *ShardMaster) Move(ctx context.Context, moveArgs *pb.MoveArgs) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_MOVE, Arg: &pb.Command_MoveArgs{MoveArgs: moveArgs}}
	sm.C <- InputChannelType{command: r, response: c}
	result := <-c
	return &result, nil
}

func (sm *ShardMaster) Query(ctx context.Context, queryArgs *pb.QueryArgs) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_QUERY, Arg: &pb.Command_QueryArgs{QueryArgs: queryArgs}}
	sm.C <- InputChannelType{command: r, response: c}
	result := <-c
	return &result, nil
}

func (sm *ShardMaster) ChangeConfiguration(ctx context.Context, in *pb.Servers) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_CONFIG_CHG, Arg: &pb.Command_Servers{Servers: in}}
	sm.C <- InputChannelType{command: r, response: c}
	result := <-c
	return &result, nil
}

// Used internally to generate a result for a shard master request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (sm *ShardMaster) JoinInternal(args *pb.JoinArgs) pb.Result {
	newConfig := sm.CreateNewConfigFromLastest()

	//newly added gids -> servers from the request
	for k, v := range args.Servers.Map {
		newConfig.Servers.Map[k] = v
		average := float64(NShards) / float64(len(newConfig.Servers.Map))
		if len(newConfig.Servers.Map) == 1 {
			for shardId := range newConfig.ShardsGroupMap.Gids {
				newConfig.ShardsGroupMap.Gids[shardId] = k
			}
		} else {
			//rebalance whenever we add a new gid
			for {
				counts := map[int64]int64{}
				var minGroup, maxGroup int64
				for _, gid := range newConfig.ShardsGroupMap.Gids {
					counts[gid] += 1
				}
				min := int64(257)
				max := int64(0)
				for gid := range newConfig.Servers.Map {
					if counts[gid] > max {
						max = counts[gid]
						maxGroup = gid
					}
					if counts[gid] < min {
						min = counts[gid]
						minGroup = gid
					}
				}
				if average == float64(int64(average)) {
					if min == int64(average) && max == min {
						break
					}
					MoveShard(newConfig.ShardsGroupMap, maxGroup, minGroup)
				} else if min < int64(average) || max > int64(average)+1 {
					MoveShard(newConfig.ShardsGroupMap, maxGroup, minGroup)
				} else {
					break
				}
			}
		}
	}

	sm.configs = append(sm.configs, &newConfig)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

func (sm *ShardMaster) LeaveInternal(args *pb.LeaveArgs) pb.Result {
	newConfig := sm.CreateNewConfigFromLastest()

	for _, gid := range args.Gids.Ids {
		delete(newConfig.Servers.Map, gid)
		average := float64(NShards) / float64(len(newConfig.Servers.Map))
		n := 0
		for _, g := range newConfig.ShardsGroupMap.Gids {
			if g == gid {
				n++
			}
		}
		for {
			counts := map[int64]int64{}
			var minGroup, maxGroup int64
			for _, gid := range newConfig.ShardsGroupMap.Gids {
				counts[gid] += 1
			}
			min := int64(257)
			max := int64(0)
			for gid := range newConfig.Servers.Map {
				if counts[gid] > max {
					max = counts[gid]
					maxGroup = gid
				}
				if counts[gid] < min {
					min = counts[gid]
					minGroup = gid
				}
			}
			if n > 0 {
				MoveShard(newConfig.ShardsGroupMap, gid, minGroup)
				n--
			} else if average == float64(int64(average)) {
				if min == int64(average) && max == min {
					break
				}
				if n > 0 {
					MoveShard(newConfig.ShardsGroupMap, gid, minGroup)
					n--
				} else {
					MoveShard(newConfig.ShardsGroupMap, maxGroup, minGroup)
				}
			} else if min < int64(average) || max > int64(average)+1 {
				if n > 0 {
					MoveShard(newConfig.ShardsGroupMap, gid, minGroup)
					n--
				} else {
					MoveShard(newConfig.ShardsGroupMap, maxGroup, minGroup)
				}
			} else {
				break
			}
		}
	}

	sm.configs = append(sm.configs, &newConfig)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

func (sm *ShardMaster) MoveInternal(args *pb.MoveArgs) pb.Result {
	newConfig := sm.CreateNewConfigFromLastest()

	newConfig.ShardsGroupMap.Gids[args.Shard] = args.Gid
	sm.configs = append(sm.configs, &newConfig)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

func (sm *ShardMaster) QueryInternal(args *pb.QueryArgs) pb.Result {
	if args.Num == -1 || args.Num > int64(len(sm.configs))-1 {
		return pb.Result{Result: &pb.Result_Config{Config: sm.configs[len(sm.configs)-1]}}
	} else {
		return pb.Result{Result: &pb.Result_Config{Config: sm.configs[args.Num]}}
	}
}

func (sm *ShardMaster) HandleCommand(op InputChannelType) {
	log.Printf("shard-master-store is handling committed command: %s", op.command.Operation)

	var result pb.Result
	var unrecognizedOp bool = false

	switch c := op.command; c.Operation {
	case pb.Op_JOIN:
		arg := c.GetJoinArgs()
		result = sm.JoinInternal(arg)
	case pb.Op_LEAVE:
		arg := c.GetLeaveArgs()
		result = sm.LeaveInternal(arg)
	case pb.Op_MOVE:
		arg := c.GetMoveArgs()
		result = sm.MoveInternal(arg)
	case pb.Op_QUERY:
		arg := c.GetQueryArgs()
		result = sm.QueryInternal(arg)
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		result = pb.Result{}
		unrecognizedOp = true
	}

	//use select to do non-blocking send
	select {
	case op.response <- result:
		log.Printf("shard-master-store command completed and response is sent to client.")
	default:
		//no response is sent when non-leader is handling the command
		log.Printf("shard-master-store command completed and no response is sent to client.")
	}

	if unrecognizedOp {
		log.Fatalf("Unrecognized operation %v", op.command.Operation)
	}
}

func (sm *ShardMaster) ApplySnapshot(snapshot []byte) {
	data := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(data)
	decoder.Decode(&sm.configs)
}

func (sm *ShardMaster) CreateNewConfigFromLastest() pb.ShardConfig {
	nextIndex := int64(len(sm.configs))
	newConfig := pb.ShardConfig{Num: nextIndex, ShardsGroupMap: sm.configs[nextIndex-1].ShardsGroupMap}
	newConfig.Servers = &pb.GroupServersMap{Map: make(map[int64]*pb.ServerList)}

	//copy what prev config gids -> servers
	for k, v := range sm.configs[nextIndex-1].Servers.Map {
		newConfig.Servers.Map[k] = v
	}

	return newConfig
}

//move a shard from g1 -> g2
func MoveShard(shard *pb.ShardsMapping, src int64, dst int64) {
	for shardId, gid := range shard.Gids {
		if gid == src {
			shard.Gids[shardId] = dst
			return
		}
	}
}
