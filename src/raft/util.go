package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

const (
	BaseIntervalTime int = 650
	RandomTimeValue  int = 150
	Null             int = -1
)

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func GetRandomTime() time.Duration {
	return time.Duration(BaseIntervalTime+rand.Intn(RandomTimeValue)) * time.Millisecond
}

func (rf *Raft) UpdateStatus(status Status) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.status = status
}

func (rf *Raft) GetStatus() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.status
}

func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) candidateHasNewerLog(lastLogTerm, lastLogIndex int) bool {
	myLastTerm := rf.log[len(rf.log)-1].Term
	return lastLogTerm > myLastTerm || lastLogTerm == myLastTerm && lastLogIndex >= len(rf.log)-1
}

func (rf *Raft) containLog(term, index int) bool {
	lastLogIndex := len(rf.log) - 1

	return index <= lastLogIndex && term == rf.log[index].Term
}

func (rf *Raft) appendLog(command interface{}) (index int) {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	return len(rf.log) - 1
}

func (rf *Raft) requestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
}

func (rf *Raft) truncateLog(end int) []Entry {
	return rf.log[:end]
}

func (rf *Raft) AppendEntriesArgs(term, id int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	next := rf.nextIndex[id]
	var entries []Entry
	m := 1

	if len(rf.log) > 1 {
		m = min(next, len(rf.log)-1)
		entries = append(entries, rf.log[m:]...)
	}

	return &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: m - 1,
		PrevLogTerm:  rf.log[m-1].Term,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) applyMsg(applyID int) ApplyMsg {
	return ApplyMsg{
		CommandValid: true,
		Command:      rf.log[applyID].Command,
		CommandIndex: applyID,
	}
}

func (rf *Raft) UpdateIndex(id, replicateIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = replicateIdx
	rf.matchIndex[id] = rf.nextIndex[id] - 1
	DPrintf("leader %v update peer %v 's nextxIndex = %v, CurrentTerm= %v\n", rf.me, id, replicateIdx, rf.currentTerm)
}

func (rf *Raft) UpdateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastLogIndex(); i > rf.commitIndex && rf.log[i].Term == rf.currentTerm; i-- {
		count := 0
		for id, idx := range rf.matchIndex {
			if id == rf.me || idx >= i {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				DPrintf("leader %d update commitIndex = %v, CurrentTerm = %d, lastApplyId = %d, lastLog = %v\n", rf.me, rf.commitIndex, rf.currentTerm, rf.lastApplied, rf.log[rf.lastLogIndex()])
				return
			}
		}
	}
}

func (rf *Raft) DecreaseNextIndex(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = rf.nextIndex[id] - (rf.nextIndex[id]-rf.matchIndex[id])/2 - 1
	if rf.nextIndex[id] <= rf.matchIndex[id] {
		rf.nextIndex[id] = rf.matchIndex[id] + 1
	}
}

func (rf *Raft) ConvertRole(argsTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.convertRole(argsTerm)
}

func (rf *Raft) convertRole(argsTerm int) bool {
	ret := false
	if argsTerm > rf.currentTerm {
		rf.status = Follower
		rf.votedFor = Null
		rf.currentTerm = argsTerm
		ret = true
		// DPrintf("			%v change term = %v, votedFor = %v due to higher Term\n", rf.me, rf.currentTerm, rf.votedFor)
	}
	return ret
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) nextInitial() {
	for id := range rf.nextIndex {
		if id == rf.me {
			continue
		}
		rf.nextIndex[id] = rf.lastLogIndex() + 1
	}
}

func (rf *Raft) matchInitial() {
	for id := range rf.matchIndex {
		rf.matchIndex[id] = 0
	}
}
