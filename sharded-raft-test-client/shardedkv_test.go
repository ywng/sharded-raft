/*
	Author: ywn202@nyu.edu

	The test cases of the fault tolerant sharded kv-store implemented in Raft.
	Below test cases are testing from the persepctives of clients.

	***** For the testing to run, it assumes we already have started the required kubernetes service pods. *****
	***** Or, the test case should have handled it by calling the launch tool py script in go test         *****
	*****                   launch-tool/launch.py boot-sm     NUM_RAFT_SERVERS_FOR_SHARD_MASTER            *****
	*****          launch-tool/launch.py boot GID   NUM_RAFT_SERVERS  NUM_RAFT_SERVERS_FOR_SHARD_MASTER    *****

	In case client is making the request to non-leader server, it will get a redirect result with leader server name.
	Client can lookup the leader's server ip and port with a call to the python script dealt with kubernetes.

*/

package main

import (
	"testing"

	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/sharded-raft/pb"
)

const (
	NUM_RAFT_SERVERS         = 3
	NUM_SHARD_MASTER_SERVERS = 3
	NShards                  = 10
)

type LockedWriter struct {
	m      sync.Mutex
	writer *bufio.Writer
}

func (lw *LockedWriter) Write(str string) (n int, err error) {
	lw.m.Lock()
	defer lw.m.Unlock()
	return lw.writer.WriteString(str)
}

func getKVConnectionToRaftLeader(t *testing.T, sg string) (string, pb.KvStoreClient) {
	leaderId := getCurrentLeaderIDByGetRequest(t, sg)
	endpoint := getKVServiceURL(t, sg, leaderId)
	kvc := establishConnection(t, endpoint)
	return leaderId, kvc
}

func establishConnection(t *testing.T, endpoint string) pb.KvStoreClient {
	t.Logf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		t.Fatalf("Failed to dial GRPC server %v", err)
	}
	t.Logf("Connected")

	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)

	return kvc
}

func getKVServiceURL(t *testing.T, sg string, peerNum string) string {
	cmd := exec.Command("../launch-tool/launch.py", "client-url", sg, peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot get the service URL. sg:%v, peer: %v, err:%v", sg, peerNum, err)
	}
	endpoint := strings.Trim(string(stdout), "\n")
	return endpoint
}

func failGivenRaftServer(t *testing.T, sg string, peerNum string) {
	cmd := exec.Command("../launch-tool/launch.py", "kill", sg, peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot kill given peer server. err: %v", err)
	}
	t.Logf("Killed raft peer%v-%v.", sg, peerNum)
	t.Logf(string(stdout))
}

func failGivenGroupOfRaftServer(t *testing.T, sg string) {
	cmd := exec.Command("../launch-tool/launch.py", "kill-sg", sg)
	stdout, err := cmd.Output()
	if err != nil {
		t.Logf("Cannot kill given server group. err: %v", err)
	}
	t.Logf("Killed raft peer%v-*.", sg)
	t.Logf(string(stdout))
}

func bootRaftServerGroup(t *testing.T, sg string, numRaftServers int, numShardMasterServers int) {
	cmd := exec.Command("../launch-tool/launch.py", "boot", sg, strconv.Itoa(numRaftServers), strconv.Itoa(numShardMasterServers))
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot boot given raft server group. err: %v", err)
	}
	t.Logf("Boot raft server group, peer%v-*.", sg)
	t.Logf(string(stdout))
}

func relaunchGivenRaftServer(t *testing.T, sg string, peerNum string, numRaftServers int, numShardMasterServers int) {
	cmd := exec.Command("../launch-tool/launch.py", "launch", sg, peerNum, strconv.Itoa(numRaftServers), strconv.Itoa(numShardMasterServers))
	stdout, err := cmd.Output()
	if err != nil {
		//t.Logf(string(stdout))
		t.Fatalf("Cannot re-launch given peer server. err: %v", err)
	}
	t.Logf(string(stdout))
}

func listAvailSgRaftServer(t *testing.T, sg string) []string {
	cmd := exec.Command("../launch-tool/launch.py", "list-sg", sg)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot list Raft servers.")
	}
	re := regexp.MustCompile("peer[0-9]+-[0-9]+")
	peers := re.FindAllString(string(stdout), -1)
	//t.Logf("Available servers: %v", peers)
	re_sg_peerNum := regexp.MustCompile("[0-9]+")
	var availServerNum = []string{}
	for _, severStr := range peers {
		availServerNum = append(availServerNum, re_sg_peerNum.FindAllString(severStr, -1)[1])
	}
	return availServerNum
}

func getCurrentLeaderIDByGetRequest(t *testing.T, sg string) string {
	redirected := true
	availRaftServer := listAvailSgRaftServer(t, sg)
	t.Logf("Available Raft servers for sg: %v, %v", sg, availRaftServer)
	endpoint := getKVServiceURL(t, sg, availRaftServer[0])
	var leaderId string = availRaftServer[0]

	for redirected {
		kvc := establishConnection(t, endpoint)

		// Request value for hello
		req := &pb.Key{Key: "hello"}
		res, err := kvc.Get(context.Background(), req)
		if err != nil {
			t.Fatalf("Request error %v", err)
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			//t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		}

		if redirected && res.GetRedirect().Server == "" {
			time.Sleep(2 * time.Second)
		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
			sg = re.FindAllString(serverName, -1)[0]
			leaderId = re.FindAllString(serverName, -1)[1]
			endpoint = getKVServiceURL(t, sg, leaderId)
		}
		time.Sleep(1 * time.Second)
	}

	return leaderId
}

func fireGetRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, w *LockedWriter, toVerify bool) *pb.Result {
	req := &pb.Key{Key: key}
	if w != nil {
		w.Write(getRequestObjFormatter(t, key))
	}

	res, err := kvc.Get(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	t.Logf("Got response: %v", res)

	if toVerify && (res.GetKv().Key != key || res.GetKv().Value != val) {
		t.Fatalf("We fail to get back what we expect.")
	}

	if w != nil {
		if res.GetKv() == nil {
			t.Fatalf("Res: %v", res)
		}
		w.Write(getResponseObjFormatter(t, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)

	return res
}

func fireSetRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, w *LockedWriter, toVerify bool) *pb.Result {
	//set a key
	putReq := &pb.KeyValue{Key: key, Value: val}
	if w != nil {
		w.Write(setRequestObjFormatter(t, key, val))
	}

	res, err := kvc.Set(context.Background(), putReq)
	if err != nil {
		t.Fatalf("Error while setting a key. err: %v", err)
	}
	t.Logf("Got response: %v", res)

	if toVerify && (res.GetKv().Key != key || res.GetKv().Value != val) {
		t.Fatalf("Set key returned the wrong response")
	}

	if w != nil {
		if res.GetKv() == nil {
			t.Fatalf("Res: %v", res)
		}
		w.Write(setResponseObjFormatter(t, res.GetKv().Key, res.GetKv().Value))
	}

	if toVerify {
		t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	}

	return res
}

func fireCasRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, expVal string,
	oldVal string, w *LockedWriter, toVerify bool) *pb.Result {
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: key, Value: oldVal}, Value: &pb.Value{Value: val}}
	if w != nil {
		w.Write(casRequestObjFormatter(t, key, val, oldVal))
	}

	res, err := kvc.CAS(context.Background(), casReq)
	if err != nil {
		t.Fatalf("Request error %v", err)
	}
	t.Logf("Got response: %v", res)

	//this cas success conjecture is not true if the old value is just the value we want to change to;
	//but we expect the old value to be another value.
	//but we can always avoid this case by test cases design.
	if w != nil {
		if res.GetKv() == nil {
			t.Fatalf("Res: %v", res)
		}
		success := res.GetKv().Value == val
		w.Write(casResponseObjFormatter(t, success, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if toVerify && (res.GetKv().Key != key || res.GetKv().Value != expVal) {
		t.Fatalf("Get returned the wrong response")
	}

	return res
}

func fireClearRequest(t *testing.T, kvc pb.KvStoreClient) *pb.Result {
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("Could not clear")
	}
	return res
}

func fireChangeConfigurationRequest(t *testing.T, kvc pb.KvStoreClient, currList string, newList string) {
	_, err := kvc.ChangeConfiguration(context.Background(), &pb.Servers{CurrList: currList, NewList: newList})
	if err != nil {
		t.Fatalf("Could not change configuration")
	}
}

func goId(t *testing.T) int {
	defer func() {
		if err := recover(); err != nil {
			t.Logf("panic recover:panic info:%v", err)
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		t.Fatalf("cannot get goroutine id: %v", err)
	}
	return id
}

//format the logs for linearizability test
func getRequestObjFormatter(t *testing.T, key string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :get, :key \"%v\", :value nil}\n", goId(t), key)
}
func getResponseObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :get, :key \"%v\", :value \"%v\"}\n", goId(t), key, val)

}
func setRequestObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :set, :key \"%v\", :value \"%v\"}\n", goId(t), key, val)
}
func setResponseObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :set, :key \"%v\", :value \"%v\"}\n", goId(t), key, val)
}
func casRequestObjFormatter(t *testing.T, key string, val string, oldVal string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :cas, :key \"%v\", :value \"%v\", :oldValue \"%v\"}\n", goId(t), key, val, oldVal)
}
func casResponseObjFormatter(t *testing.T, success bool, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :cas, :success \"%t\", :key \"%v\", :value \"%v\"}\n", goId(t), success, key, val)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func produceServersMapping(fromG int, toG int, num_raft int) *pb.GroupServersMap {
	num_servers := NUM_RAFT_SERVERS
	serverMap := make(map[int64]*pb.ServerList)
	for j := fromG; j <= toG; j++ {
		server_list := []string{}
		for i := 0; i < num_servers; i++ {
			server_list = append(server_list, fmt.Sprintf("peer%d-%d:3000", j, i))
		}
		serverMap[int64(j)] = &pb.ServerList{List: server_list}
	}
	return &pb.GroupServersMap{Map: serverMap}
}

func key2shard(key string) int64 {
	shard := int64(0)
	if len(key) > 0 {
		shard = int64(key[0])
	}
	shard %= NShards
	return shard
}

func checkIfWrongServerGroup(t *testing.T, res *pb.Result) bool {
	switch res.Result.(type) {
	case *pb.Result_WrongG:
		return true
	default:
		t.Fatalf("We expect the result returned is ErrWrongGroup.")
	}
	return false
}

func prepareShardMasterAndServerGroups(t *testing.T, numGroups int, numRaftServers int) (*pb.ShardConfig, pb.ShardMasterClient) {
	prepareFreshShardMaster(t, numRaftServers)
	prepareSeverGroupForTest(t, 0, numGroups-1, numRaftServers, numRaftServers)

	_, smc := getConnectionToShardMasterRaftLeader(t)
	fireJoinRequest(t, smc, &pb.JoinArgs{Servers: produceServersMapping(0, numGroups-1, numRaftServers)})
	config := fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
	return config, smc
}

func prepareFreshShardMaster(t *testing.T, numRaftServers int) {
	failAllShardMasterRaftServer(t)
	time.Sleep(30 * time.Second)
	bootShardMasterServerGroup(t, numRaftServers)
	time.Sleep(40 * time.Second)
}

func prepareSeverGroupForTest(t *testing.T, fromG int, toG int, numRaftServers int, numShardMasterServers int) {
	for i := fromG; i <= toG; i++ {
		failGivenGroupOfRaftServer(t, strconv.Itoa(i))
	}

	time.Sleep(60 * time.Second)

	for i := fromG; i <= toG; i++ {
		bootRaftServerGroup(t, strconv.Itoa(i), numRaftServers, numShardMasterServers)
	}

	time.Sleep(40 * time.Second)
}

func severGroupTearDown(t *testing.T, fromG int, toG int) {
	for i := fromG; i <= toG; i++ {
		failGivenGroupOfRaftServer(t, strconv.Itoa(i))
	}
	time.Sleep(30 * time.Second)
}

func fireSetRequestsToShardedKVStore(t *testing.T, keys []string, values []string, config *pb.ShardConfig, verify bool, w *LockedWriter) {
	for i, key := range keys {
		shard := key2shard(key)
		gid := config.ShardsGroupMap.Gids[shard]
		t.Logf("Setting key, key: %v, shard: %v, gid: %v", key, shard, gid)
		_, kvc := getKVConnectionToRaftLeader(t, strconv.Itoa(int(gid)))
		fireSetRequest(t, kvc, key, values[i], w, verify)
	}
}

func fireGetRequestsToShardedKVStore(t *testing.T, keys []string, values []string, config *pb.ShardConfig, verify bool, w *LockedWriter) {
	for i, key := range keys {
		shard := key2shard(key)
		gid := config.ShardsGroupMap.Gids[shard]
		t.Logf("Retriving key, key: %v, shard: %v, gid: %v", key, shard, gid)
		_, kvc := getKVConnectionToRaftLeader(t, strconv.Itoa(int(gid)))
		fireGetRequest(t, kvc, key, values[i], w, verify)
	}
}

func fireClearRequestsToShardedKVStore(t *testing.T, keys []string, config *pb.ShardConfig) {
	for _, key := range keys {
		shard := key2shard(key)
		gid := config.ShardsGroupMap.Gids[shard]
		t.Logf("Clearing the kv-store of the server group, key: %v, shard: %v, gid: %v", key, shard, gid)
		_, kvc := getKVConnectionToRaftLeader(t, strconv.Itoa(int(gid)))
		fireClearRequest(t, kvc)
	}
}

func fireCasRequestsToShardedKVStore(t *testing.T, keys []string, values []string,
	expectedVal []string, oldValues []string, config *pb.ShardConfig, verify bool, w *LockedWriter) {
	for i, key := range keys {
		shard := key2shard(key)
		gid := config.ShardsGroupMap.Gids[shard]
		t.Logf("CAS set key, key: %v, shard: %v, gid: %v", key, shard, gid)
		_, kvc := getKVConnectionToRaftLeader(t, strconv.Itoa(int(gid)))
		fireCasRequest(t, kvc, key, values[i], expectedVal[i], oldValues[i], w, verify)
	}
}

//******** Sharded Raft Test Cases ************//

/*
	Test make kv requests to the right group of the shard of key
*/
func TestCorrectShardGroup(t *testing.T) {
	config, _ := prepareShardMasterAndServerGroups(t, 3, NUM_RAFT_SERVERS)

	keys := []string{"x", "y"}
	values := []string{"1", "2"}

	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	severGroupTearDown(t, 0, 2)
}

/*
	Test make kv requests to the wrong group of the shard of key
*/
func TestWrongShardGroup(t *testing.T) {
	numGroup := 4
	config, _ := prepareShardMasterAndServerGroups(t, numGroup, NUM_RAFT_SERVERS)

	shard := key2shard("hello")
	gid := config.ShardsGroupMap.Gids[shard]

	//we have enumerate all the other gid, all except the correct one should return ErrWrongGroup
	for i := 1; i < numGroup; i++ {
		t.Logf("Test kv requests to wrong gid, shard: %v, gid: %v", shard, (int(gid)+i)%numGroup)
		_, kvc := getKVConnectionToRaftLeader(t, strconv.Itoa((int(gid)+i)%numGroup)) //just make it a call to the wrong server group
		checkIfWrongServerGroup(t, fireSetRequest(t, kvc, "hello", "world", nil, false))
	}

	severGroupTearDown(t, 0, numGroup-1)
}

/*
	Test information pertained after change of shard config
	After shard join, my previous key set should return the same value (no loss of information)
*/
func TestKVCorrectnessShardJoin(t *testing.T) {
	config, smc := prepareShardMasterAndServerGroups(t, 1, NUM_RAFT_SERVERS)
	time.Sleep(15 * time.Second)
	//make some set requests before the Join and Leave of shard master
	keys := []string{"hello", "Victor", "NYU", "Distributed Systems", "GoLang"}
	values := []string{"world", "is a name", "New York University", "is fun?", "???"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	//the old keys' value should preserved after new kv-store server group(s) join
	prepareSeverGroupForTest(t, 1, 2, NUM_RAFT_SERVERS, NUM_SHARD_MASTER_SERVERS)
	_, smc = getConnectionToShardMasterRaftLeader(t)
	fireJoinRequest(t, smc, &pb.JoinArgs{Servers: produceServersMapping(1, 2, NUM_RAFT_SERVERS)})
	time.Sleep(15 * time.Second)

	config = fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	severGroupTearDown(t, 0, 2)
}

/*
	Test information pertained after change of shard config
	After shard leave, my previous key set should return the same value (no loss of information)
*/
func TestKVCorrectnessShardLeave(t *testing.T) {
	config, smc := prepareShardMasterAndServerGroups(t, 3, NUM_RAFT_SERVERS)
	time.Sleep(15 * time.Second)
	//make some set requests before the Join and Leave of shard master
	keys := []string{"hello", "Victor", "NYU", "Distributed Systems", "GoLang"}
	values := []string{"world", "is a name", "New York University", "is fun?", "???"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	//the old keys' value should preserved after some kv-store server group(s) left
	gids := []int64{1, 2}
	fireLeaveRequest(t, smc, &pb.LeaveArgs{Gids: &pb.GroupIDs{Ids: gids}})
	time.Sleep(15 * time.Second)

	config = fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	severGroupTearDown(t, 0, 2)
}

/*
	Test information pertained after change of shard config
	After shard move, my previous key set should return the same value (no loss of information)
*/
func TestKVCorrectnessShardMove(t *testing.T) {
	config, smc := prepareShardMasterAndServerGroups(t, 3, NUM_RAFT_SERVERS)
	time.Sleep(15 * time.Second)
	//make some set requests before the Join and Leave of shard master
	keys := []string{"hello", "Victor", "NYU", "Distributed Systems", "GoLang", "shard move work?"}
	values := []string{"world", "is a name", "New York University", "is fun?", "???", "yes"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	//the old keys' value should preserved after a shard move
	_, smc = getConnectionToShardMasterRaftLeader(t)
	for _, key := range keys {
		shard := key2shard(key)
		gid := config.ShardsGroupMap.Gids[shard]
		fireMoveRequest(t, smc, &pb.MoveArgs{Shard: shard, Gid: (gid + 1) % 3})
	}
	time.Sleep(20 * time.Second)

	config = fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	severGroupTearDown(t, 0, 2)
}

/*
	Test if sharded kv-store is fault tolerant to
*/
func TestKVAfterShardNodeOrKVNodeFailure(t *testing.T) {
	numGroups := 3
	numRaftServers := 5
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, numRaftServers)

	//make some set requests before the failure of the shard / kv-store nodes
	keys := []string{"hello", "Victor", "NYU", "Distributed Systems", "GoLang"}
	values := []string{"world", "is a name", "New York University", "is fun?", "???"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	time.Sleep(15 * time.Second)

	//we have 5 shard master nodes, should be able to tolerate 2 failures
	failGivenShardMasterRaftServer(t, "0")
	failGivenShardMasterRaftServer(t, "1")

	//each raft groups has 5 nodes, should be able to tolerate 2 failures
	for i := 0; i < numGroups; i++ {
		failGivenRaftServer(t, strconv.Itoa(i), "0")
		failGivenRaftServer(t, strconv.Itoa(i), "1")
	}

	time.Sleep(60 * time.Second)
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)
	keysAdditional := []string{"das", "wa", "ow", "pq"}
	valuesAdditional := []string{"world", "is a name", "New York University", "is fun?"}
	fireSetRequestsToShardedKVStore(t, keysAdditional, valuesAdditional, config, true, nil)

	relaunchGivenShardMasterRaftServer(t, "0")
	relaunchGivenShardMasterRaftServer(t, "1")
	for i := 0; i < numGroups; i++ {
		relaunchGivenRaftServer(t, strconv.Itoa(i), "0", numRaftServers, numRaftServers)
		relaunchGivenRaftServer(t, strconv.Itoa(i), "1", numRaftServers, numRaftServers)
	}

	time.Sleep(40 * time.Second)
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)
	fireGetRequestsToShardedKVStore(t, keysAdditional, valuesAdditional, config, true, nil)

	severGroupTearDown(t, 0, numGroups-1)
}

/*
	Test if our changes can survive kv-group leader failure
	& shard master leader failure.

*/
func TestSurviveLeaderFailure(t *testing.T) {
	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)
	time.Sleep(10 * time.Second)

	keys := []string{"x", "y", "z", "k"}
	values := []string{"2", "21", "2", "11"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	//fail the leader
	failedLeaders := make(map[int64]string)
	for _, key := range keys {
		shard := key2shard(key)
		gid := config.ShardsGroupMap.Gids[shard]
		if _, ok := failedLeaders[gid]; !ok {
			leaderId := getCurrentLeaderIDByGetRequest(t, strconv.Itoa(int(gid)))
			t.Logf("Failing leader gid: %v, leaderId: %v", gid, leaderId)
			failGivenRaftServer(t, strconv.Itoa(int(gid)), leaderId)
			failedLeaders[gid] = leaderId
		}
	}

	shardLeaderId := getCurrentShardMasterLeaderIDByQueryRequest(t)
	failGivenShardMasterRaftServer(t, shardLeaderId)

	//give time for electing new leader
	time.Sleep(40 * time.Second)
	_, smc := getConnectionToShardMasterRaftLeader(t)
	config = fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	keysAdditional := []string{"efe", "vewvew", "dqwq", "wwfff"}
	valuesAdditional := []string{"world", "is a name", "New York University", "is fun?"}
	fireSetRequestsToShardedKVStore(t, keysAdditional, valuesAdditional, config, true, nil)

	for gid, leaderId := range failedLeaders {
		relaunchGivenRaftServer(t, strconv.Itoa(int(gid)), leaderId, NUM_RAFT_SERVERS, NUM_SHARD_MASTER_SERVERS)
	}
	relaunchGivenShardMasterRaftServer(t, shardLeaderId)

	time.Sleep(60 * time.Second)
	_, smc = getConnectionToShardMasterRaftLeader(t)
	config = fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
	t.Logf("Relaunched failed leaders, and ensuring the data values are intact.")
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)
	fireGetRequestsToShardedKVStore(t, keysAdditional, valuesAdditional, config, true, nil)

	severGroupTearDown(t, 0, numGroups-1)
}

//******** Regression: Raft Test Cases ********//

/*
	Test if clear requests can really clear all existing records
*/
func TestClear(t *testing.T) {
	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)

	keys := []string{"x", "y", "z"}
	values := []string{"99", "88", "18"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	fireClearRequestsToShardedKVStore(t, keys, config)

	//the values should be empty after clear
	values = []string{"", "", ""}
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	severGroupTearDown(t, 0, numGroups-1)
}

/*
	Test if cas requests are behaving correctly
*/
func TestCas(t *testing.T) {
	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)
	time.Sleep(10 * time.Second)

	keys := []string{"x", "y", "z"}
	values := []string{"99", "88", "18"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, nil)

	keys = []string{"x", "y", "k", "z'"}
	values = []string{"199", "000", "18", "sda"}
	expectedVal := []string{"199", "88", "18", ""}
	oldVal := []string{"99", "87", "", "ewq"}
	fireCasRequestsToShardedKVStore(t, keys, values, expectedVal, oldVal, config, true, nil)

	severGroupTearDown(t, 0, numGroups-1)
}

/*
	Test if a Raft server can redirect us to the leader if it is not the leader.
*/
func TestRedirectionHandling(t *testing.T) {
	numGroups := 2
	prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)

	leaderId := getCurrentLeaderIDByGetRequest(t, "0") //group 0

	//fail the leader
	failGivenRaftServer(t, "0", leaderId)
	time.Sleep(20 * time.Second)

	getCurrentLeaderIDByGetRequest(t, "0")

	severGroupTearDown(t, 0, numGroups-1)
}

/*
	Test a serial of requests and
	the requests should be producing results as expected in the requests firing ordering, as no concurrency is here.

	The corresponding linerizability test case by porcupine is
	- TestRaftKv1ClientSequential
*/
func TestSerialRequestsCorrectness(t *testing.T) {
	f, err := os.Create("raft_test_data/c1-sequential.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)
	time.Sleep(10 * time.Second)

	keys := []string{"x", "y"}
	values := []string{"1", "2"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, w)

	keys = []string{"x"}
	values = []string{"1"}
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, w)

	keys = []string{"x", "z", "x", "y"}
	values = []string{"2", "3", "3", "4"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, w)

	keys = []string{"y", "x"}
	values = []string{"4", "3"}
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, w)

	keys = []string{"z"}
	values = []string{"3"}
	fireSetRequestsToShardedKVStore(t, keys, values, config, true, w)

	keys = []string{"z", "x"}
	values = []string{"4", "5"}
	expectedVal := []string{"4", "3"}
	oldVal := []string{"3", "4"}
	fireCasRequestsToShardedKVStore(t, keys, values, expectedVal, oldVal, config, true, w)

	keys = []string{"x", "y", "z"}
	values = []string{"3", "4", "4"}
	fireGetRequestsToShardedKVStore(t, keys, values, config, true, w)

	w.writer.Flush()
	severGroupTearDown(t, 0, numGroups-1)
}

/*
	Simple concurrently firing requests, to make sure it at leasts work without errors.
	No lineralizability test yet.
*/
func TestConcurrentRequests(t *testing.T) {
	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)

	time.Sleep(20 * time.Second)

	testConcurrentSet(t, config, true)
	testConcurrentGet(t, config, true)

	//this will trigger early tear down if no proper control like wait groups
	//severGroupTearDown(t, 0, numGroups-1)
}

/*
	Test concurrent set requests firing to the raft leader
*/
func testConcurrentSet(t *testing.T, config *pb.ShardConfig, parallel bool) {
	tc := []struct {
		key string
		val string
	}{
		{"hello", "hi"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
		{"abc", "def"},
		{"abcde", "fgh"},
		{"ywn202", "Yik Wai Ng"},
		{"nyu", "New York University"},
		{"hello", "hi2"},
		{"test_f_nodes_failure", "32"},
		{"test_leader_failure", "22"},
		{"abc", "def2"},
		{"abcde", "fgh2"},
		{"ywn202", "Yik Wai Ng2"},
		{"nyu", "New York University2"},
	}

	for _, tt := range tc {
		tt := tt
		t.Run("ConcurrentSet", func(st *testing.T) {
			if parallel {
				st.Parallel()
			}
			fireSetRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, config, true, nil)
		})
	}
}

/*
	Test concurrent get requests firing to the raft leader
*/
func testConcurrentGet(t *testing.T, config *pb.ShardConfig, parallel bool) {
	tc := []struct {
		key string
		val string
	}{
		{"hello", "hi"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
		{"abc", "def"},
		{"abcde", "fgh"},
		{"ywn202", "Yik Wai Ng"},
		{"nyu", "New York University"},
		{"hello", "hi2"},
		{"test_f_nodes_failure", "32"},
		{"test_leader_failure", "22"},
		{"abc", "def2"},
		{"abcde", "fgh2"},
		{"ywn202", "Yik Wai Ng2"},
		{"nyu", "New York University2"},
	}

	for _, tt := range tc {
		tt := tt
		t.Run("ConcurrentGet", func(st *testing.T) {
			if parallel {
				st.Parallel()
			}
			fireGetRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, config, false, nil)
		})
	}
}

/*
	A mix of concurrent get, set, cas without expected value check/assertion.
	We will do the linerizability check if it is correct.

	The corresponding linerizability test case by porcupine is
	- TestRaftKvManyClientConcurrentGetSetCas
*/
func TestConcurrentGetSetCas(t *testing.T) {
	f, err := os.Create("raft_test_data/c-many-concurrent-get-set-cas.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	r := rand.Intn(13)

	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)

	time.Sleep(30 * time.Second)

	tc := []struct {
		op     int //0: get, 1:set, 2:cas
		key    string
		val    string
		oldVal string
	}{
		{1, "hello", "hi", ""},
		{1, "test_f_nodes_failure", "3", ""},
		{1, "test_leader_failure", "2", ""},
		{1, "abc", "def", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{1, "nyu", "New New York University", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{2, "abc", "hig", "def"},
		{1, "OOP", "Object Oriented Programming", ""},
		{2, "abc", "dwdwdw", "dwdw"},
		{1, "test_f_nodes_failure", "9", ""},
		{1, "abc", "def", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{2, "nyu", "hig", "New New York University"},
		{1, "test_f_nodes_failure", "9", ""},
		{0, "OOP", "", ""},
		{0, "test_f_nodes_failure", "", ""},
		{2, "test_leader_failure", "8", "7"},
		{1, "abcde", "defee", ""},
		{0, "nyu", "", ""},
		{2, "nyu", "what is it?", "New New York University"},
		{0, "nyuabc", "", ""},
		{2, "abc", "higdwdwdw", "defwwww"},
		{0, "nyu", "", ""},
		{1, "test_f_nodes_failure", "9", ""},
	}

	var wg sync.WaitGroup
	wg.Add(len(tc))

	for _, tt := range tc {
		tt := tt
		time.Sleep(time.Duration(r) * time.Millisecond)
		switch tt.op {
		case 0:
			go func() {
				defer wg.Done()
				fireGetRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, config, false, w)
			}()
		case 1:
			go func() {
				defer wg.Done()
				fireSetRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, config, false, w)
			}()
		case 2:
			go func() {
				defer wg.Done()
				//fireCasRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, []string{""}, []string{tt.oldVal}, config, false, w)
			}()
		}
	}

	wg.Wait()
	w.writer.Flush()

}

/*
	Concurrent requests of high contention,
	simulate several clients are making exactly the same set of requests concurrently.
	We will do the linerizability check if it is correct.

	The corresponding linerizability test case by porcupine is
	- TestRaftKvHighConcurrentContention
*/
func TestHighConcurrentContention(t *testing.T) {
	num_concurr_threads := 10
	t.Logf("Number of concurrent threads: %v", num_concurr_threads)

	f, err := os.Create("raft_test_data/c-high-concurrent-contention.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	r := rand.Intn(15)

	numGroups := 3
	config, _ := prepareShardMasterAndServerGroups(t, numGroups, NUM_RAFT_SERVERS)

	time.Sleep(20 * time.Second)

	tc := []struct {
		op     int //0: get, 1:set, 2:cas
		key    string
		val    string
		oldVal string
	}{
		{1, "hello", "hi", ""},
		{1, "test_f_nodes_failure", "3", ""},
		{1, "test_leader_failure", "2", ""},
		{1, "abc", "def", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{1, "nyu", "New New York University", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{0, "OOP", "", ""},
		{0, "nyu", "", ""},
		{1, "OOP", "Object Oriented Programming", ""},
		{1, "test_f_nodes_failure", "9", ""},
		{1, "abc", "defdsds", ""},
		{1, "hello", "hihi???", ""},
		{1, "test_f_nodes_failure", "4", ""},
		{1, "test_leader_failure", "8", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{1, "test_f_nodes_failure", "9d0", ""},
		{0, "OOP", "", ""},
		{0, "test_f_nodes_failure", "", ""},
		{1, "abcde", "defee", ""},
		{0, "nyu", "", ""},
		{1, "nyu", "New New York University? seriously", ""},
		{0, "nyuabc", "", ""},
		{0, "nyu", "", ""},
		{1, "test_f_nodes_failure", "91", ""},
	}

	var wg sync.WaitGroup
	wg.Add(num_concurr_threads) //simulate 5 clients making the same set of requests concurrently

	for i := 1; i <= num_concurr_threads; i++ {
		go func() {
			time.Sleep((time.Duration(r) + 1) * time.Millisecond)
			defer wg.Done()
			//each simulated client fires the same set of requests
			for _, tt := range tc {
				tt := tt
				time.Sleep(time.Duration(r) * time.Millisecond)
				switch tt.op {
				case 0:
					fireGetRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, config, false, w)
				case 1:
					fireSetRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, config, false, w)
				case 2:
					fireCasRequestsToShardedKVStore(t, []string{tt.key}, []string{tt.val}, []string{""}, []string{tt.oldVal}, config, false, w)
				}
			}
		}()
	}

	wg.Wait()
	w.writer.Flush()
}
