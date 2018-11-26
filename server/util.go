package main

import (
	rand "math/rand"
	"time"
)

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func ServerListEquals(a *arrayPeers, b *arrayPeers) bool {
	if len(*a) != len(*b) {
		return false
	}

	for _, v1 := range *a {
		var found bool = false
		for _, v2 := range *b {
			if v1 == v2 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	const DurationMax = ELECTION_TIMEOUT_UPPER_BOUND
	const DurationMin = ELECTION_TIMEOUT_LOWER_BOUND
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, d time.Duration) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(d)
}

func stopTimer(timer *time.Timer) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}
	}
}
