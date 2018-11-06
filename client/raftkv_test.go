/*
	Author: ywn202@nyu.edu

	The test cases of the fault tolerant kv-store implemented in Raft.
	Below test cases are testing from the persepctives of clients.

	***** For the testing to run, it assumes we already have started the required kubernetes service pods. *****
	*****                          launch-tool/launch.py boot NUM_RAFT_SERVERS                             *****

	In case client is making the request to non-leader server, it will get a redirect result with leader server name.
	Client can lookup the leader's server ip and port with a call to the python script dealt with kubernetes.

*/

package main

import (
	"testing"

	"regexp"
	"strings"
	"os/exec"
	"os"
	"strconv"
	"fmt"
	"runtime"
	"bufio"

	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
)

const (
	//launch-tool/launch.py boot 5
	NUM_RAFT_SERVERS = 5
)

func getKVConnectionToRaftLeader(t *testing.T) (string, pb.KvStoreClient) {
	leaderId := getCurrentLeaderIDByGetRequest(t)
	endpoint := getKVServiceURL(t, leaderId)
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

func getKVServiceURL(t *testing.T, peerNum string) (string) {
	cmd := exec.Command("../launch-tool/launch.py", "client-url", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
        t.Fatalf("Cannot get the service URL.")
    }
    endpoint := strings.Trim(string(stdout),"\n")
    return endpoint
}

func failGivenRaftServer(t *testing.T, peerNum string)  {
	cmd := exec.Command("../launch-tool/launch.py", "kill", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
        t.Fatalf("Cannot kill given peer server. err: %v", err)
    }
    t.Logf("Killed raft peer%v.", peerNum)
    t.Logf(string(stdout))
}

func relaunchGivenRaftServer(t *testing.T, peerNum string)  {
	cmd := exec.Command("../launch-tool/launch.py", "launch", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
        t.Fatalf("Cannot re-launch given peer server.")
    }
    t.Logf(string(stdout))
}

func listAvailRaftServer(t *testing.T) ([]string) {
	cmd := exec.Command("../launch-tool/launch.py", "list")
	stdout, err := cmd.Output()
	if err != nil {
        t.Fatalf("Cannot list Raft servers.")
    }
    re := regexp.MustCompile("[0-9]+")
    return re.FindAllString(string(stdout), -1)
}

func getCurrentLeaderIDByGetRequest(t *testing.T) (string) {
	redirected := true
	endpoint := getKVServiceURL(t, listAvailRaftServer(t)[0])
	var leaderId string = listAvailRaftServer(t)[0]

	for ;redirected; {
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
			t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		}

		if redirected && res.GetRedirect().Server == "" {
			time.Sleep(2 * time.Second)
		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
			leaderId = re.FindAllString(serverName, -1)[0]
    		endpoint = getKVServiceURL(t, leaderId)
		}
	}

	return leaderId
}

func fireGetRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, w *bufio.Writer) {
	req := &pb.Key{Key: key}
	if w != nil {
		w.WriteString(getRequestObjFormatter(t, key))
	}

	res, err := kvc.Get(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}
	
	if res.GetKv().Key != key || res.GetKv().Value != val {
		t.Fatalf("We fail to get back what we expect.")
	}

	if w != nil {
		w.WriteString(getResponseObjFormatter(t, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
}

func fireSetRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, w *bufio.Writer) {
	//set a key 
	putReq := &pb.KeyValue{Key: key, Value: val}
	if w != nil {
		w.WriteString(setRequestObjFormatter(t, key, val))
	}

	res, err := kvc.Set(context.Background(), putReq)
	if err != nil {
		t.Fatalf("Error while setting a key. err: %v", err)
	}
	
	if res.GetKv().Key != key || res.GetKv().Value != val {
		t.Fatalf("Set key returned the wrong response")
	}

	if w != nil {
		w.WriteString(setResponseObjFormatter(t, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
}

func fireCasRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, oldVal string, success bool, w *bufio.Writer) {
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: key, Value: oldVal}, Value: &pb.Value{Value: val}}
	if w != nil {
		w.WriteString(casRequestObjFormatter(t, key, val, oldVal))
	}

	res, err := kvc.CAS(context.Background(), casReq)
	if err != nil {
		t.Fatalf("Request error %v", err)
	}

	if w != nil {
		w.WriteString(casResponseObjFormatter(t, success, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if success {
		if res.GetKv().Key != key || res.GetKv().Value != val {
			t.Fatalf("Get returned the wrong response")
		} 
	} 
}

func fireClearRequest(t *testing.T, kvc pb.KvStoreClient) {
	_, err := kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("Could not clear")
	}
}

func Goid(t *testing.T) int {
    defer func()  {
        if err := recover(); err != nil {
            t.Logf("panic recover:panic info:%v", err)     }
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
	return fmt.Sprintf("{:process %d, :type :invoke, :f :get, :key \"%v\", :value nil}\n", Goid(t), key)
}
func getResponseObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :get, :key \"%v\", :value \"%v\"}\n", Goid(t), key, val)

}
func setRequestObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :set, :key \"%v\", :value \"%v\"}\n", Goid(t), key, val)
}
func setResponseObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :set, :key \"%v\", :value \"%v\"}\n", Goid(t), key, val)
}
func casRequestObjFormatter(t *testing.T, key string, val string, oldVal string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :cas, :key \"%v\", :value \"%v\", :oldValue \"%v\"}\n", Goid(t), key, val, oldVal)
}
func casResponseObjFormatter(t *testing.T, success bool, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :cas, :success \"%t\", :key \"%v\", :value \"%v\"}\n", Goid(t), success, key, val)
}
	
func check(e error) {
    if e != nil {
        panic(e)
    }
}

/*
	Test if a Raft server can redirect us to the leader if it is not the leader.
 */
func TestRedirectionHandling(t *testing.T) {
	leaderId := getCurrentLeaderIDByGetRequest(t)

	//fail the leader
	failGivenRaftServer(t, leaderId)
	time.Sleep(20 * time.Second)

	getCurrentLeaderIDByGetRequest(t)

	relaunchGivenRaftServer(t, leaderId)
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test if our changes can survive leader failure.
	After a new leader is selected, the kv-store should return what we previously set.
	1. Set a key for current leader
	2. Fail the current leader
	3. Make a get to new leader, should get back what we set
 */
func TestSurviveLeaderFailure(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	fireSetRequest(t, kvc, "test_leader_failure", "2", nil)

	//fail the leader
	failGivenRaftServer(t, leaderId)
	//give time for electing new leader
	time.Sleep(20 * time.Second)

	_, kvcNextLeader := getKVConnectionToRaftLeader(t)
	fireGetRequest(t, kvcNextLeader, "test_leader_failure", "2", nil)

	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, leaderId)
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test it can tolerate f nodes failure given 2f+1 nodes.
	1. Set a key 
	2. Fail f nodes 
	3. Make a get, should get back what we set
 */
func TestTolerateFFailures(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	fireSetRequest(t, kvc, "test_f_nodes_failure", "3", nil)

	//fail f Raft servers
	//here we starts 5 servers, f = 2
	var nextServerToTry int
	nextServerToTry, _ = strconv.Atoi(leaderId)
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+1) % NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+2) % NUM_RAFT_SERVERS))
	time.Sleep(20 * time.Second)
	
	fireGetRequest(t, kvc, "test_f_nodes_failure", "3", nil)

	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+1) % NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+2) % NUM_RAFT_SERVERS))
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test the commited logs should survive after failed nodes rejoin
	1. Fail some nodes
	2. Make some set requests
	3. Re-launch those failed nodes
	3. Make get requests, should get back what we set
 */
func TestCommitedLogsShouldSurviveAfterFailedNodesRejoin(t *testing.T) {
	failedNodes := []string {"0","1"}
	testCommitedLogsShouldSurviveAfterRejoin(t, failedNodes, "test_failed_node_rejoin", "4")
}

/*
	Test the commited logs should survived after failed leader rejoin
	1. Fail the leader
	2. Make some set requests
	3. Re-launch the failed leader
	3. Make get requests, should get back what we set
 */
func TestCommitedLogsShouldSurviveAfterFailedLeaderRejoin(t *testing.T) {
	leaderId := getCurrentLeaderIDByGetRequest(t)
	failedNodes := []string {leaderId}
	testCommitedLogsShouldSurviveAfterRejoin(t, failedNodes, "test_failed_leader_rejoin", "5")
}

func testCommitedLogsShouldSurviveAfterRejoin(t *testing.T, failedNodesList []string, key string, val string) {
	for _, node := range failedNodesList {
		failGivenRaftServer(t, node)
	}
	time.Sleep(20 * time.Second)

	_, kvc := getKVConnectionToRaftLeader(t)
	fireSetRequest(t, kvc, key, val, nil)

	for _, node := range failedNodesList {
		relaunchGivenRaftServer(t, node)
	}
	time.Sleep(20 * time.Second)

	_, kvc = getKVConnectionToRaftLeader(t)
	fireGetRequest(t, kvc, key, val, nil)
}

/*
	If the failing node(s) is not the leader, and not more than f nodes failed, 
	the leader should be able to process requests seamlessly and respond to client.
 */
func TestRequestHandlingDuringNonLeaderFailures(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	t.Logf(leaderId)
	testConcurrentSet(t, kvc, false)

	//fail f Raft servers
	//here we starts 5 servers, f = 2
	var nextServerToTry int
	nextServerToTry, _ = strconv.Atoi(leaderId)
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+1) % NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+2) % NUM_RAFT_SERVERS))

	testConcurrentGet(t, kvc, false)

	time.Sleep(20 * time.Second)
	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+1) % NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+2) % NUM_RAFT_SERVERS))
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test a serial of requests and 
	the requests should be producing results as expected in the requests firing ordering, as no concurrency is here.
 */
func TestSerialRequestsCorrectness(t *testing.T) {
	f, err := os.Create("raft_test_data/c1-sequential.txt")
    check(err)
    defer f.Close()
    w := bufio.NewWriter(f)

	_, kvc := getKVConnectionToRaftLeader(t)
	
	fireSetRequest(t, kvc, "x", "1", w)
	fireSetRequest(t, kvc, "y", "2", w)

	fireGetRequest(t, kvc, "x", "1", w)

	fireSetRequest(t, kvc, "x", "2", w)
	fireSetRequest(t, kvc, "z", "3", w)
	fireSetRequest(t, kvc, "x", "3", w)
	fireSetRequest(t, kvc, "y", "4", w)

	fireGetRequest(t, kvc, "y", "4", w)
	fireGetRequest(t, kvc, "x", "3", w)

	fireSetRequest(t, kvc, "z", "3", w)

	fireCasRequest(t, kvc, "z", "4", "3", true, w)
	fireCasRequest(t, kvc, "x", "5", "4", false, w)

	fireGetRequest(t, kvc, "x", "3", w)
	fireGetRequest(t, kvc, "y", "4", w)
	fireGetRequest(t, kvc, "z", "4", w)

	w.Flush()
}

/*
	Simple concurrently firing requests, to make sure it at leasts work without errors.
	No lineralizability test yet.
 */
func TestConcurrentRequests(t *testing.T) {
	_, kvc := getKVConnectionToRaftLeader(t)
	testConcurrentSet(t, kvc, true)
	testConcurrentGet(t, kvc, true)
}

/*
	Test concurrent set requests firing to the raft leader
 */
func testConcurrentSet(t *testing.T, kvc pb.KvStoreClient, parallel bool) {
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
	}

	for _, tt := range tc {
		tt := tt
		t.Run("ConcurrentSet", func(st *testing.T) {
			if parallel {
				st.Parallel()
			}
			fireSetRequest(t, kvc, tt.key, tt.val, nil)
		})
	}
}

/*
	Test concurrent get requests firing to the raft leader
 */
func testConcurrentGet(t *testing.T, kvc pb.KvStoreClient, parallel bool) {
	tc := []struct {
		key string
		val string
	}{
		{"hello", "hi"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
		{"abc", "def"},
		{"hello", "hi"},
		{"abc", "def"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
	}

	for _, tt := range tc {
		tt := tt
		t.Run("ConcurrentGet", func(st *testing.T) {
			if parallel {
				st.Parallel()
			}
			fireGetRequest(t, kvc, tt.key, tt.val, nil)
		})
	}
}











