package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"
)

// getPersistedTerm reads currentTerm from the given storage (same encoding as
// persistToStorage).
func getPersistedTerm(storage *MapStorage) int {
	data, found := storage.Get("currentTerm")
	if !found {
		return 0
	}
	var term int
	d := gob.NewDecoder(bytes.NewBuffer(data))
	if err := d.Decode(&term); err != nil {
		return 0
	}
	return term
}

// TestBug_StartElectionMissingPersist demonstrates that startElection() did not
// persist currentTerm and votedFor before the fix. A node that starts several
// elections while disconnected increments its in-memory term, but the persisted
// term stays behind. After a crash-restart the node reverts to the old term and
// can vote again in a term it already voted in — a violation of Raft's
// vote-once-per-term invariant.
func TestBug_StartElectionMissingPersist(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Wait for a leader to be elected and pick a non-leader to disconnect.
	// Disconnecting the leader itself would not trigger repeated elections,
	// making the test produce false negatives.
	leaderId, _ := h.CheckSingleLeader()
	victim := (leaderId + 1) % 3

	h.DisconnectPeer(victim)

	// Give it enough time to run a few elections (each bumps currentTerm).
	time.Sleep(1200 * time.Millisecond)

	// Read the victim's in-memory term and persisted term.
	cm := h.cluster[victim].cm
	cm.mu.Lock()
	inMemoryTerm := cm.currentTerm
	cm.mu.Unlock()

	persistedTerm := getPersistedTerm(h.storage[victim])

	t.Logf("server %d: in-memory term = %d, persisted term = %d", victim, inMemoryTerm, persistedTerm)

	// After the fix, persisted term should keep up with the in-memory term.
	// Before the fix, persisted term would lag behind (stayed at the term from
	// the last RequestVote/AppendEntries RPC, not from startElection).
	if persistedTerm < inMemoryTerm {
		t.Errorf("persisted term (%d) is behind in-memory term (%d); "+
			"startElection is not persisting state", persistedTerm, inMemoryTerm)
	}
}
