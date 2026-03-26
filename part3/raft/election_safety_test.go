package raft

import (
	"testing"
	"time"
)

// TestBecomeFollowerSameTermPreservesVotedFor verifies that becomeFollower
// does not reset votedFor when the term stays the same. Resetting votedFor
// on a same-term transition allows a node to vote twice in the same term,
// which violates election safety.
func TestBecomeFollowerSameTermPreservesVotedFor(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()

	// Pick a follower and inspect its internal state.
	for i := 0; i < 3; i++ {
		cm := h.cluster[i].cm
		cm.mu.Lock()
		if cm.state == Follower && cm.votedFor >= 0 {
			savedVotedFor := cm.votedFor
			savedTerm := cm.currentTerm

			// Call becomeFollower with the same term. votedFor must not change.
			cm.becomeFollower(savedTerm)

			if cm.votedFor != savedVotedFor {
				t.Errorf("becomeFollower(%d) reset votedFor from %d to %d on same-term transition",
					savedTerm, savedVotedFor, cm.votedFor)
			}
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
	t.Fatal("no follower with votedFor >= 0 found")
}

// TestBecomeFollowerHigherTermResetsVotedFor verifies that becomeFollower
// does reset votedFor when the term increases.
func TestBecomeFollowerHigherTermResetsVotedFor(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()

	for i := 0; i < 3; i++ {
		cm := h.cluster[i].cm
		cm.mu.Lock()
		if cm.state == Follower && cm.votedFor >= 0 {
			savedTerm := cm.currentTerm

			// Call becomeFollower with a higher term. votedFor must be reset.
			cm.becomeFollower(savedTerm + 1)

			if cm.votedFor != -1 {
				t.Errorf("becomeFollower(%d) did not reset votedFor (got %d, want -1)",
					savedTerm+1, cm.votedFor)
			}
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
	t.Fatal("no follower with votedFor >= 0 found")
}

// TestStaleVoteReplyIgnored verifies that a vote reply from a previous term
// is not counted. This tests the fix for the stale savedCurrentTerm bug:
// if a node starts an election in term T, then moves to term T+1 before
// receiving the reply, the reply (with Term=T) should be ignored because
// T < currentTerm (T+1).
func TestStaleVoteReplyIgnored(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	// Disconnect the leader so a new election happens.
	h.DisconnectPeer(origLeaderId)
	sleepMs(450)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Fatalf("expected newTerm > origTerm, got %d <= %d", newTerm, origTerm)
	}

	// Disconnect the new leader to force yet another election.
	h.DisconnectPeer(newLeaderId)
	sleepMs(450)

	// Reconnect both old leaders. They'll receive replies from terms that
	// have long since passed. With the fix, these stale replies won't cause
	// any node to count votes from old terms.
	h.ReconnectPeer(origLeaderId)
	h.ReconnectPeer(newLeaderId)
	sleepMs(450)

	// The key assertion: at most one leader in any given term.
	h.CheckSingleLeader()
}

// TestSameTermDoubleVotePrevented creates a scenario where a candidate
// becomes a follower in the same term (via AppendEntries from the leader)
// and then receives a RequestVote from another candidate in the same term.
// The node must not grant the vote because it already voted in this term.
func TestSameTermDoubleVotePrevented(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()

	// Find a follower that voted for the leader.
	followerId := -1
	for i := 0; i < 3; i++ {
		if i == leaderId {
			continue
		}
		cm := h.cluster[i].cm
		cm.mu.Lock()
		if cm.votedFor == leaderId && cm.currentTerm == leaderTerm {
			followerId = i
		}
		cm.mu.Unlock()
		if followerId >= 0 {
			break
		}
	}
	if followerId < 0 {
		t.Fatal("could not find a follower that voted for the leader")
	}

	// Now simulate: another candidate sends RequestVote for the same term.
	// The follower must reject it because it already voted.
	otherCandidate := -1
	for i := 0; i < 3; i++ {
		if i != leaderId && i != followerId {
			otherCandidate = i
			break
		}
	}

	cm := h.cluster[followerId].cm
	args := RequestVoteArgs{
		Term:         leaderTerm,
		CandidateId:  otherCandidate,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
	var reply RequestVoteReply
	if err := cm.RequestVote(args, &reply); err != nil {
		t.Fatal(err)
	}

	if reply.VoteGranted {
		t.Errorf("follower %d granted vote to %d in term %d, but already voted for %d",
			followerId, otherCandidate, leaderTerm, leaderId)
	}
}

// TestElectionSafetyStress runs repeated leader disconnections to stress-test
// that election safety holds: no two leaders in the same term.
func TestElectionSafetyStress(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	for cycle := 0; cycle < 8; cycle++ {
		leaderId, _ := h.CheckSingleLeader()
		h.DisconnectPeer(leaderId)
		sleepMs(350)

		// Check that there's exactly one leader.
		h.CheckSingleLeader()

		h.ReconnectPeer(leaderId)
		sleepMs(150)
	}

	// Final check after all the churn.
	time.Sleep(300 * time.Millisecond)
	h.CheckSingleLeader()
}
