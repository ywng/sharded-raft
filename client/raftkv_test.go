/*
	The test cases of the fault tolerant kv-store implemented in Raft.
	Below test cases are testing from the persepctives of clients.

	***** For the testing to run, it assumes we already have started the required kubernetes service pods. *****

	In case client is making the request to non-leader server, it will get a redirect result with leader server name.
	Client can lookup the leader's server ip and port with a call to the python script dealt with kubernetes.

*/

package main

import (
	"testing"

	"regexp"
	"strings"
	"os/exec"
	"strconv"

	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
)

const (
	//launch-tool/launch.py boot 5
	NUM_RAFT_SERVERS = 5
)

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

/*
	Test if a Raft server can redirect us to the leader if it is not the leader.
 */
func TestRedirectionHandling(t *testing.T) {
	redirected := true
	endpoint := getKVServiceURL(t, "0")

	for ;redirected; {
		kvc := establishConnection(t, endpoint)

		//clear kv store
		res, err := kvc.Clear(context.Background(), &pb.Empty{})

		// Request value for hello
		req := &pb.Key{Key: "hello"}
		res, err = kvc.Get(context.Background(), req)
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

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
    		endpoint = getKVServiceURL(t, re.FindAllString(serverName, -1)[0])
    		time.Sleep(1 * time.Second)
		}
	}
}

/*
	Test if our changes can survive leader failure.
	After a new leader is selected, the kv-store should return what we previously set.
 */
func TestSurviveLeaderFailure(t *testing.T) {
	redirected := true
	endpoint := getKVServiceURL(t, "0")
	var leaderId string = "0"

	for ;redirected; {
		kvc := establishConnection(t, endpoint)

		//set a key 
		putReq := &pb.KeyValue{Key: "test_leader_failure", Value: "2"}
		res, err := kvc.Set(context.Background(), putReq)
		if err != nil {
			t.Fatalf("Error while setting a key.")
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			if res.GetKv().Key != "test_leader_failure" || res.GetKv().Value != "2" {
				t.Fatalf("Set key returned the wrong response")
			}
			t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
			leaderId = re.FindAllString(serverName, -1)[0]
    		endpoint = getKVServiceURL(t, leaderId)
    		time.Sleep(1 * time.Second)
		}
	}

	//fail the leader
	failGivenRaftServer(t, leaderId)
	//give time for electing new leader
	time.Sleep(10 * time.Second)

	var nextServerToTry int 
	nextServerToTry, _ = strconv.Atoi(leaderId);
	redirected = true
	for ;redirected; {
		kvc := establishConnection(t, endpoint)

		// Request value for hello
		req := &pb.Key{Key: "test_leader_failure"}
		res, err := kvc.Get(context.Background(), req)
		if err != nil {
			//t.Logf("Request error %v", err)
			nextServerToTry = (nextServerToTry+1) % NUM_RAFT_SERVERS
			endpoint = getKVServiceURL(t, strconv.Itoa(nextServerToTry))
			continue
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			if res.GetKv().Key != "test_leader_failure" || res.GetKv().Value != "2" {
				t.Fatalf("We fail to get back what we set after leader failure.")
			}
			t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)

		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
    		endpoint = getKVServiceURL(t, re.FindAllString(serverName, -1)[0])
    		time.Sleep(1 * time.Second)
		}
	}

	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, leaderId)
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test it can tolerate f nodes failure given 2f+1 nodes.
 */
func TestTolerateFFailures(t *testing.T) {
	redirected := true
	endpoint := getKVServiceURL(t, "0")
	var leaderId string = "0"

	for ;redirected; {
		kvc := establishConnection(t, endpoint)

		//set a key 
		putReq := &pb.KeyValue{Key: "test_f_nodes_failure", Value: "3"}
		res, err := kvc.Set(context.Background(), putReq)
		if err != nil {
			t.Fatalf("Error while setting a key.")
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			if res.GetKv().Key != "test_f_nodes_failure" || res.GetKv().Value != "3" {
				t.Fatalf("Set key returned the wrong response")
			}
			t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
			leaderId = re.FindAllString(serverName, -1)[0]
    		endpoint = getKVServiceURL(t, leaderId)
    		time.Sleep(1 * time.Second)
		}
	}

	//fail f Raft servers
	//here we starts 5 servers, f = 2
	var nextServerToTry int
	nextServerToTry, _ = strconv.Atoi(leaderId)
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+1) % NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+2) % NUM_RAFT_SERVERS))

	redirected = true
	for ;redirected; {
		kvc := establishConnection(t, endpoint)

		// Request value for hello
		req := &pb.Key{Key: "test_f_nodes_failure"}
		res, err := kvc.Get(context.Background(), req)
		if err != nil {
			t.Logf("Request error %v", err)
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			if res.GetKv().Key != "test_f_nodes_failure" || res.GetKv().Value != "3" {
				t.Fatalf("We fail to tolerate f failures.")
			}
			t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)

		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
    		endpoint = getKVServiceURL(t, re.FindAllString(serverName, -1)[0])
    		time.Sleep(1 * time.Second)
		}
	}

	time.Sleep(20 * time.Second)
	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+1) % NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+2) % NUM_RAFT_SERVERS))
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}



