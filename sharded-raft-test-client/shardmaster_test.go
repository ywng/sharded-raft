/*
	Author: ywn202@nyu.edu

	The test cases of the fault tolerant (in Raft) Shard Master.
	Below test cases are testing from the persepctives of clients of Shard Master services.

	***** For the testing to run, it assumes we already have started the required kubernetes service pods. *****
	*****                   launch-tool/launch.py boot-sm NUM_RAFT_SERVERS_FOR_SHARD_MASTER                *****

	In case client is making the request to non-leader server, it will get a redirect result with leader server name.
	Client can lookup the leader's server ip and port with a call to the python script dealt with kubernetes.

*/

package main

import (
	"testing"

	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/sharded-raft/pb"
)

func getConnectionToShardMasterRaftLeader(t *testing.T) (string, pb.ShardMasterClient) {
	leaderId := getCurrentShardMasterLeaderIDByQueryRequest(t)
	endpoint := getShardMasterServiceURL(t, leaderId)
	smc := establishShardMasterConnection(t, endpoint)
	return leaderId, smc
}

func establishShardMasterConnection(t *testing.T, endpoint string) pb.ShardMasterClient {
	t.Logf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		t.Fatalf("Failed to dial GRPC server %v", err)
	}
	t.Logf("Connected")

	// Create a KvStore client
	smc := pb.NewShardMasterClient(conn)

	return smc
}

func getShardMasterServiceURL(t *testing.T, peerNum string) string {
	cmd := exec.Command("../launch-tool/launch.py", "sm-client-url", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot get the service URL, peerNum: %v", peerNum)
	}
	endpoint := strings.Trim(string(stdout), "\n")
	return endpoint
}

func failGivenShardMasterRaftServer(t *testing.T, peerNum string) {
	cmd := exec.Command("../launch-tool/launch.py", "kill-sm", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot kill given peer server. err: %v", err)
	}
	t.Logf("Killed raft peer%v.", peerNum)
	t.Logf(string(stdout))
}

func failAllShardMasterRaftServer(t *testing.T) {
	cmd := exec.Command("../launch-tool/launch.py", "kill-sm-all")
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot kill all shard master servers. err: %v", err)
	}
	t.Logf("Killed all shard master servers.")
	t.Logf(string(stdout))
}

func bootShardMasterServerGroup(t *testing.T, numRaftServers int) {
	cmd := exec.Command("../launch-tool/launch.py", "boot-sm", strconv.Itoa(numRaftServers))
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot boot shard master server group. err: %v", err)
	}
	t.Logf("Boot shard master server group, sm-peer*.")
	t.Logf(string(stdout))
}

func relaunchGivenShardMasterRaftServer(t *testing.T, peerNum string) {
	cmd := exec.Command("../launch-tool/launch.py", "launch-sm", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot re-launch given peer server, peerNum: %v", peerNum)
	}
	t.Logf(string(stdout))
}

func listAvailShardMasterRaftServer(t *testing.T) []string {
	cmd := exec.Command("../launch-tool/launch.py", "list-sm")
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot list Raft servers.")
	}
	re := regexp.MustCompile("[0-9]+")
	return re.FindAllString(string(stdout), -1)
}

func getCurrentShardMasterLeaderIDByQueryRequest(t *testing.T) string {
	redirected := true
	endpoint := getShardMasterServiceURL(t, listAvailShardMasterRaftServer(t)[0])
	var leaderId string = listAvailShardMasterRaftServer(t)[0]

	for redirected {
		smc := establishShardMasterConnection(t, endpoint)

		res, err := smc.Query(context.Background(), &pb.QueryArgs{Num: -1})
		if err != nil {
			t.Fatalf("Request error %v", err)
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			t.Logf("Got response: %v", res)
		}

		if redirected && res.GetRedirect().Server == "" {
			time.Sleep(2 * time.Second)
		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
			leaderId = re.FindAllString(serverName, -1)[0]
			endpoint = getShardMasterServiceURL(t, leaderId)
		}
	}

	return leaderId
}

func fireQueryRequest(t *testing.T, smc pb.ShardMasterClient, req *pb.QueryArgs) *pb.ShardConfig {
	res, err := smc.Query(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	t.Logf("Got response: %v", res)
	return res.GetConfig()
}

func fireJoinRequest(t *testing.T, smc pb.ShardMasterClient, req *pb.JoinArgs) {
	t.Logf("Join req: %v", req)
	res, err := smc.Join(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	t.Logf("Got response: %v", res)
}

func fireLeaveRequest(t *testing.T, smc pb.ShardMasterClient, req *pb.LeaveArgs) {
	res, err := smc.Leave(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	t.Logf("Got response: %v", res)
}

func fireMoveRequest(t *testing.T, smc pb.ShardMasterClient, req *pb.MoveArgs) {
	res, err := smc.Move(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	t.Logf("Got response: %v", res)
}

func fireMembershipChangeRequest(t *testing.T, smc pb.ShardMasterClient, req *pb.Servers) {
	res, err := smc.ChangeConfiguration(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	t.Logf("Got response: %v", res)
}

/*
	Test query shard config
*/
func TestQuery(t *testing.T) {
	_, smc := getConnectionToShardMasterRaftLeader(t)

	fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})

}

/*
	Test adding a new kv-store service group to shard master config
*/
func TestJoin(t *testing.T) {
	_, smc := getConnectionToShardMasterRaftLeader(t)

	fireJoinRequest(t, smc, &pb.JoinArgs{Servers: produceServersMapping(0, 2, NUM_RAFT_SERVERS)})
	fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
}

/*
	Test removing a new kv-store service group from shard master config
*/
func TestLeave(t *testing.T) {
	_, smc := getConnectionToShardMasterRaftLeader(t)

	gids := []int64{1}
	fireLeaveRequest(t, smc, &pb.LeaveArgs{Gids: &pb.GroupIDs{Ids: gids}})
	fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
}

/*
	Test moving a shard of key to a kv service group
*/
func TestMove(t *testing.T) {
	_, smc := getConnectionToShardMasterRaftLeader(t)
	arg := &pb.MoveArgs{Shard: 0, Gid: 2}
	t.Logf("Move arg, shard: %v, gid: %v", arg.Shard, arg)
	fireMoveRequest(t, smc, arg)
	arg = &pb.MoveArgs{Shard: 1, Gid: 0}
	t.Logf("Move arg, shard: %v, gid: %v", arg.Shard, arg)
	fireMoveRequest(t, smc, arg)
	fireQueryRequest(t, smc, &pb.QueryArgs{Num: -1})
}
