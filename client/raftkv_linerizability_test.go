package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"testing"
	//"log"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-ywng/porcupine"
)

type RaftKvInput struct {
	op    uint8 // 0 => get, 1 => set, 2 => cas, //we don't do clear because it is equivalent to set a key to ""
	key   string
	value string // used for set and cas new value
	oldValue string // used for cas from argument
}

type RaftKvOutput struct {
	ok      bool // used for cas
	value string
}

func getRaftKvModel() porcupine.Model {
	return porcupine.Model{
		PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
			m := make(map[string][]porcupine.Event)
			match := make(map[uint]string) // id -> key
			for _, v := range history {
				if v.Kind == porcupine.CallEvent {
					key := v.Value.(RaftKvInput).key
					m[key] = append(m[key], v)
					match[v.Id] = key
				} else {
					key := match[v.Id]
					m[key] = append(m[key], v)
				}
			}
			var ret [][]porcupine.Event
			for _, v := range m {
				ret = append(ret, v)
			}
			return ret
		},
		Init: func() interface{} {
			// note: we are modeling a single key's value here;
			// we're partitioning by key, so this is okay
			return ""
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			inp := input.(RaftKvInput)
			out := output.(RaftKvOutput)
			st := state.(string)

			//log.Printf("input: %v", inp)
			//log.Printf("ouput: %v", out)
			//log.Printf("state: %v", st)
			if inp.op == 0 {
				// get
				return out.value == st, state
			} else if inp.op == 1 {
				// set
				return true, inp.value
			} else {
				// cas
				ok := (inp.oldValue == st && out.ok) || (inp.oldValue != st && !out.ok)
				result := st
				if inp.oldValue == st {
					result = inp.value
				}
				return ok, result
			}
		},
	}
}

func parseRaftKvLog(filename string) []porcupine.Event {
	file, err := os.Open(filename)
	if err != nil {
		panic("can't open file")
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	invokeGet, _ := regexp.Compile(`{:process (\d+), :type :invoke, :f :get, :key "(.*)", :value nil}`)
	invokeSet, _ := regexp.Compile(`{:process (\d+), :type :invoke, :f :set, :key "(.*)", :value "(.*)"}`)
	invokeCas, _ := regexp.Compile(`{:process (\d+), :type :invoke, :f :cas, :key "(.*)", :value "(.*)", :oldValue "(.*)"}`)
	returnGet, _ := regexp.Compile(`{:process (\d+), :type :ok, :f :get, :key ".*", :value "(.*)"}`)
	returnSet, _ := regexp.Compile(`{:process (\d+), :type :ok, :f :set, :key ".*", :value ".*"}`)
	returnCas, _ := regexp.Compile(`{:process (\d+), :type :ok, :f :cas, :success "(.*)", :key ".*", :value "(.*)"}`)

	var events []porcupine.Event = nil

	id := uint(0)
	procIdMap := make(map[int]uint)
	for {
		lineBytes, isPrefix, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			panic("error while reading file: " + err.Error())
		}
		if isPrefix {
			panic("can't handle isPrefix")
		}
		line := string(lineBytes)

		switch {
		case invokeGet.MatchString(line):
			args := invokeGet.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, porcupine.Event{porcupine.CallEvent, RaftKvInput{op: 0, key: args[2]}, id})
			procIdMap[proc] = id
			id++
		case invokeSet.MatchString(line):
			args := invokeSet.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, porcupine.Event{porcupine.CallEvent, RaftKvInput{op: 1, key: args[2], value: args[3]}, id})
			procIdMap[proc] = id
			id++
		case invokeCas.MatchString(line):
			args := invokeCas.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			events = append(events, porcupine.Event{porcupine.CallEvent, 
						RaftKvInput{op: 2, key: args[2], value: args[3], oldValue: args[4]}, id})
			procIdMap[proc] = id
			id++
		case returnGet.MatchString(line):
			args := returnGet.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, porcupine.Event{porcupine.ReturnEvent, RaftKvOutput{ok: true, value: args[2]}, matchId})
		case returnSet.MatchString(line):
			args := returnSet.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			events = append(events, porcupine.Event{porcupine.ReturnEvent, RaftKvOutput{ok: true}, matchId})
		case returnCas.MatchString(line):
			args := returnCas.FindStringSubmatch(line)
			proc, _ := strconv.Atoi(args[1])
			matchId := procIdMap[proc]
			delete(procIdMap, proc)
			if args[2] == "true" {
				events = append(events, porcupine.Event{porcupine.ReturnEvent, RaftKvOutput{ok: true, value: args[3]}, matchId})
			} else {
				events = append(events, porcupine.Event{porcupine.ReturnEvent, RaftKvOutput{ok: false, value: args[3]}, matchId})
			}
			
		}
	}

	for _, matchId := range procIdMap {
		events = append(events, porcupine.Event{porcupine.ReturnEvent, RaftKvOutput{}, matchId})
	}

	//log.Printf("%v", events)

	return events
}

func checkRaftKv(t *testing.T, logName string, correct bool) {
	t.Parallel()
	raftKvModel := getRaftKvModel()
	events := parseRaftKvLog(fmt.Sprintf("raft_test_data/%s.txt", logName))
	res := porcupine.CheckEvents(raftKvModel, events)
	if res != correct {
		t.Fatalf("expected output %t, got output %t", correct, res)
	}
}

func TestRaftKv1ClientSequential(t *testing.T) {
	checkRaftKv(t, "c1-sequential", true)
}

func TestRaftKvManyClientConcurrentGetSetCas(t *testing.T) {
	checkRaftKv(t, "c-many-concurrent-get-set-cas", true)
}
