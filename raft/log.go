// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 从storage中读取已保存的持久化信息
	if storage == nil {
		panic("must have non nil storage")
	}
	raftLog := &RaftLog{
		storage: storage,
	}

	// 从头开始的话storage.FirstIndex()==1, storage.LastIndex()==0,这样storage.Entries啥都取不到，也就是说从零开始的话entries最初是空数组
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex() // 这两个函数不会返回error

	entries, err := storage.Entries(firstIndex, lastIndex+1) // 这样就能获取到[firstIndex, lastIndex]范围内所有entries
	if err != nil {
		panic(err)
	}

	raftLog.entries = entries
	raftLog.stabled = lastIndex
	// applied和committed会在newRaft中重新修正
	raftLog.applied = firstIndex - 1
	raftLog.committed = firstIndex - 1

	return raftLog
}

func (l *RaftLog) entriesFrom(index uint64) ([]pb.Entry, error) {
	offset := l.entries[0].Index
	if index > l.LastIndex() {
		return nil, nil
	}
	var ents []pb.Entry

	for i := index; i <= l.LastIndex(); i++ {
		ents = append(ents, l.entries[i-offset]) // logic_index - offset = slice_index
	}
	return ents, nil
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.matchTerm(maxIndex, term) { // matchTerm保证论文figure8的saftey问题 leader只能提交自己term的entries
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	if t, err := l.Term(i); err == nil {
		return t == term
	}
	return false
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Fatal(fmt.Sprintf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed))
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		log.Fatal(fmt.Sprintf("after(%d) is out of range [committed(%d)]", after, l.committed))
	}
	l.truncateAndAppend(ents)
	//l.maybeCompact()
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	if after == l.LastIndex()+1 {
		l.entries = append(l.entries, ents...)
		return
	}
	// truncate to after and copy to u.entries then append
	//log.Info(fmt.Sprintf("truncate the unstable entries before index %d", after))
	if after-1 < l.stabled {
		l.stabled = after - 1
	}
	offset := uint64(0)
	if len(l.entries) > 0 {
		offset = l.entries[0].Index
	}
	l.entries = append([]pb.Entry{}, l.entries[:after-offset]...) // 从一个空的切片数组开始append
	l.entries = append(l.entries, ents...)
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
func (r *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !r.matchTerm(ne.Index, ne.Term) {
			return ne.Index
		}
	}
	return 0
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries -- 在ready中使用
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	ents := make([]pb.Entry, 0) // 直接var ents []pb.Entry定义的话如果没有append任何元素会返回nil！
	for i := l.stabled + 1; i <= l.LastIndex(); i++ {
		ents = append(ents, l.entries[i-offset])
	}
	return ents
}

// nextEnts returns all the committed but not applied entries -- 在ready中使用
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	var ents []pb.Entry
	for i := l.applied + 1; i <= l.committed; i++ {
		ents = append(ents, l.entries[i-offset])
	}
	return ents
}

// LastIndex return the last index of the log entries
// LastIndex()指的是logic index，如果还没有entries那就返回0，有一个entries就返回1
func (l *RaftLog) LastIndex() uint64 { // 最初的话RaftLog.Entries是空的
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) lastTerm() uint64 { // 但是可能会返回entry[0]这个dummy entry的term（如果没有截断就是0）
	term, _ := l.Term(l.LastIndex()) // Term要保证像lastIndex这样合理位置一定能有一个正确的结果!其他错误位置可能会出问题
	return term
}

// Term return the term of the entry in the given index
// 要让prevLogTerm能通过Term(prevLogIndex)获取
func (l *RaftLog) Term(i uint64) (uint64, error) { // i->logic index   logic_index - offset = slice_index
	// Your Code Here (2A).
	if len(l.entries) == 0 || i == 0 {
		return 0, nil
	}
	offset := l.entries[0].Index // 如果是没有compact情况下的话offset就是1 -- debug看看是不是
	if i < offset {
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) appliedTo(toapply uint64) {
	if toapply == 0 {
		return
	}
	if l.committed < toapply || toapply < l.applied { // applied - toapply - committed
		log.Fatal(fmt.Sprintf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", toapply, l.applied, l.committed))
	}
	l.applied = toapply
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit && tocommit <= l.LastIndex() { // committed - tocommit - last
		l.committed = tocommit
	}
}

func (l *RaftLog) isUpToDate(lastLogTerm, lastLogIndex uint64) bool {
	lastTerm, lastIndex := l.lastTerm(), l.LastIndex()
	return lastLogTerm > lastTerm || (lastLogTerm == lastTerm && lastLogIndex >= lastIndex)
}
