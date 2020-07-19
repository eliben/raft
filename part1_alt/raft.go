// Core Raft implementation - Consensus Module.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// and contributors.
// This code is in the public domain.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type ConsensusProxy struct {
	// Chan to receive tasks to run on the main loop.
	taskQueue chan func()

	// id is the server ID of the corresponding CM.
	id int

	// Chan to signal exit
	exit chan bool

	// A pointer to the CM,
	// used to sending tasks to run on the main loop
	cm *ConsensusModule
}

func (proxy *ConsensusProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	done := make(chan interface{})
	cm := proxy.cm
	handler := func() {
		cm.dlog("got voteRequest %+v", args)
		done <- cm.RequestVote(args, reply)
	}
	select {
	case <-proxy.exit:
		return nil
	default:
		proxy.taskQueue <- handler
		<-done
		return nil
	}
}

func (proxy *ConsensusProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	done := make(chan interface{})
	cm := proxy.cm
	handler := func() {
		cm.dlog("got appendEntriesRequest %+v", args)
		done <- cm.AppendEntries(args, reply)
	}
	select {
	case <-proxy.exit:
		return nil
	default:
		proxy.taskQueue <- handler
		<-done
		return nil
	}
}

func (proxy *ConsensusProxy) Stop() {
	close(proxy.exit)
}

type Report struct {
	term     int
	isLeader bool
}

// Report reports the state of this CM.
func (proxy *ConsensusProxy) Report() (id int, term int, isLeader bool) {
	responseChan := make(chan Report)
	cm := proxy.cm
	handler := func() {
		term, isLeader := cm.Report()
		report := Report{
			term,
			isLeader,
		}
		responseChan <- report
	}
	select {
	case <-proxy.exit:
		return -1, -1, false
	default:
		proxy.taskQueue <- handler
		report := <-responseChan
		return proxy.id, report.term, report.isLeader
	}
}

// dlog logs a debugging message is DebugCM > 0.
func (proxy *ConsensusProxy) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", proxy.id) + format
		log.Printf(format, args...)
	}
}

// ConsensusModule (CM) implements a single node of Raft consensus.
type ConsensusModule struct {

	// Chan to receive tasks to run on the main loop.
	taskQueue chan func()

	// Chan to signal exit
	exit chan bool

	// id is the server ID of this CM.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// server is the server containing this CM. It's used to issue RPC calls
	// to peers.
	server *Server

	// The timeout for the current election round
	timeoutDuration time.Duration

	// A ticker used for elections and leadership.
	ticker *time.Ticker

	// The term during which the current election round started
	termStarted int

	// Persistent Raft state on all servers
	currentTerm   int
	votesReceived int32
	votedFor      int
	log           []LogEntry

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine.
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusProxy {
	taskQueue := make(chan func())
	exit := make(chan bool)

	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1
	cm.taskQueue = taskQueue
	cm.exit = exit

	// The run loop of the consensus module,
	// all consensus state is encapsulated within this goroutine.
	go func() {
		// The CM is quiescent until ready is signaled; then, it starts a countdown
		// for leader election.
		<-ready

		cm.electionResetEvent = time.Now()
		cm.resetElectionTimer()

		var leaderCountdown = 0
		cm.ticker = time.NewTicker(10 * time.Millisecond)
		defer cm.ticker.Stop()

		for {
			select {
			case <-cm.exit:
				cm.Stop()
				return

			case task := <-cm.taskQueue:
				task()

			case <-cm.ticker.C:
				if cm.state == Leader {
					leaderCountdown++
					if leaderCountdown > 4 {
						leaderCountdown = 0
						cm.leaderSendHeartbeats()
					}
					continue
				}

				// Reset it if we're not the leader anymore.
				leaderCountdown = 0

				// Start an election if we haven't heard from a leader or haven't voted for
				// someone for the duration of the timeout.
				if elapsed := time.Since(cm.electionResetEvent); elapsed >= cm.timeoutDuration {
					cm.startElection()
				}
			}
		}

	}()

	return &ConsensusProxy{
		taskQueue,
		id,
		exit,
		cm,
	}
}

// Report reports the state of this CM.
func (cm *ConsensusModule) Report() (term int, isLeader bool) {
	return cm.currentTerm, cm.state == Leader
}

// Stop stops this CM, cleaning up its state.
func (cm *ConsensusModule) Stop() {
	cm.state = Dead
	cm.dlog("becomes Dead")
}

// dlog logs a debugging message is DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// See figure 2 in the paper.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// See figure 2 in the paper.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout duration.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// resetElectionTimer resets the election timer.
func (cm *ConsensusModule) resetElectionTimer() {
	cm.timeoutDuration = cm.electionTimeout()
	cm.dlog("election timer started (%v), term=%d", cm.timeoutDuration, cm.currentTerm)
}

// startElection starts a new election with this CM as a candidate.
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.votesReceived = 1

	savedCurrentTerm := cm.currentTerm
	exit := cm.exit
	taskQueue := cm.taskQueue
	id := cm.id
	server := cm.server

	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: id,
			}
			var reply RequestVoteReply

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			if err := server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				handler := func() {
					cm.dlog("received RequestVoteReply %+v", reply)

					if cm.state != Candidate {
						cm.dlog("while waiting for voteReply, state = %v", cm.state)
						return
					}

					if reply.Term > savedCurrentTerm {
						cm.dlog("term out of date in RequestVoteReply")
						cm.becomeFollower(reply.Term)
					} else if reply.Term == savedCurrentTerm {
						if reply.VoteGranted {
							cm.votesReceived++
							votes := int(cm.votesReceived)
							if votes*2 > len(cm.peerIds)+1 {
								// Won the election!
								cm.dlog("wins election with %d votes", votes)
								cm.startLeader()
								cm.votesReceived = 0
							}
						}
					}
				}
				select {
				case <-exit:
					return
				default:
					taskQueue <- handler
				}
			}
		}(peerId)
	}

	// Start another election timer, in case this election is not successful.
	cm.resetElectionTimer()
}

// becomeFollower makes cm a follower and resets its state.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	cm.resetElectionTimer()
}

// startLeader switches cm into a leader state and begins process of heartbeats.
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)
	cm.leaderSendHeartbeats()
}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts cm's state.
func (cm *ConsensusModule) leaderSendHeartbeats() {
	savedCurrentTerm := cm.currentTerm
	id := cm.id
	exit := cm.exit
	server := cm.server
	taskQueue := cm.taskQueue

	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: id,
		}
		go func(peerId int) {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				handler := func() {
					if reply.Term > savedCurrentTerm {
						cm.dlog("term out of date in heartbeat reply")
						cm.becomeFollower(reply.Term)
					}
				}
				select {
				case <-exit:
					return
				default:
					taskQueue <- handler
				}
			}
		}(peerId)
	}
}
