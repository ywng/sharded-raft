package main

import (
	"strings"
)

// Define a type that can be used by the flag library to collect an array of strings.
type arrayPeers []string

// Convert array to a string
func (a *arrayPeers) String() string {
	return strings.Join(*a, ",")
}

// Add a string
func (a *arrayPeers) Set(v string) error {
	*a = append(*a, v)
	return nil
}

// Add a string array
func (a *arrayPeers) SetArray(other []string) error {
	*a = append(*a, other...)
	return nil
}

func (a *arrayPeers) Contains(s string) bool {
	for _, server := range *a {
		if server == s {
			return true
		}
	}
	return false
}

func (a *arrayPeers) Clone() *arrayPeers {
	var newArr arrayPeers
	for _, server := range *a {
		newArr = append(newArr, server)
	}
	return &newArr
}

// Merge two arrays
func (a *arrayPeers) Merge(other *arrayPeers) *arrayPeers {
	serversSet := map[string]bool{}

	for _, server := range *a {
		serversSet[server] = true
	}

	for _, server := range *other {
		serversSet[server] = true
	}

	var result arrayPeers
	for key, _ := range serversSet {
		result = append(result, key)
	}
	return &result
}

// Configuration tracks which servers are in the cluster, and whether they have
// votes. This should include the local server, if it's a member of the cluster.
// The servers are listed no particular order, but each should only appear once.
// These entries are appended to the log during membership changes.
type Configuration struct {
	servers *arrayPeers
}

// Clone makes a deep copy of a Configuration.
func (c *Configuration) Clone() Configuration {
	var newConfig Configuration
	newConfig.servers = c.servers.Clone()
	return newConfig
}

// configurations is state tracked on every server about its Configurations.
// Note that, per Diego's dissertation, there can be at most one uncommitted
// configuration at a time (the next configuration may not be created until the
// prior one has been committed).
//
// One downside to storing just two configurations is that if you try to take a
// snapshot when your state machine hasn't yet applied the committedIndex, we
// have no record of the configuration that would logically fit into that
// snapshot. We disallow snapshots in that case now. An alternative approach,
// which LogCabin uses, is to track every configuration change in the
// log.
type Configurations struct {
	// committed is the latest configuration in the log/snapshot that has been
	// committed (the one with the largest index).
	config             Configuration
	lastConfigLogIndex int64
	stable             bool
}
