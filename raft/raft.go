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
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64 // 我感觉就是赋值peerID

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (pr *Progress) MaybeDecrTo(index uint64) bool {
	// The rejection must be stale if "index" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	if pr.Next-1 != index {
		return false
	}
	// rejection后next每次其实只回退1（相当于每次next-=1）
	pr.Next = max(index, 1)
	return true
}

func (prs *Progress) MaybeUpdate(match uint64) bool {
	var updated bool
	if prs.Match < match {
		prs.Match = match
		updated = true
	}
	if prs.Next < match+1 {
		prs.Next = match + 1
	}
	return updated
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress // prs还作为peers数组来使用，比如在广播一些消息时就使用prs获取各个peerId

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// random election interval between [electionTimeout, 2 * electionTimeout]
	randomizedElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
// Config中参考NewPeer中的初始化 Storage-PeerStorage applied-ps.AppliedIndex(直接kvDB读取RaftApplyState) id就是peerID
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)                        // 可能是测试用的MemoryStorage也可能是实际场合的PeerStorage
	hardState, confState, _ := c.Storage.InitialState() // InitialState不会返回错误  hardState-term/vote/commit confState-peerID数组

	peers := c.peers
	if len(confState.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both Config.peers and ConfState.Nodes")
		}
		peers = confState.Nodes
	}

	raft := &Raft{
		id:               c.ID, // 其实就是peerID，参考NewPeer中raft.Config的初始化
		Lead:             None,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		PendingConfIndex: 0,
	}

	// 初始化Progress {Next/Match}
	for _, peerID := range peers {
		// TODO: 到底要不要LastIndex()+1 ———— 还是先去看懂txy代码吧！ vldbss代码next初值为1(但他有RejectHint)
		raft.Prs[peerID] = &Progress{Next: raft.RaftLog.LastIndex()} // TODO: match & next 的理论知识又忘了
	}

	// 修正hardState-commit/vote/term
	if !IsEmptyHardState(hardState) {
		raft.RaftLog.committed = hardState.Commit
		raft.Term = hardState.Term
		raft.Vote = hardState.Vote
	}
	// 修正applied - 参考NewPeer，c.Applied从PeerStorage.AppliedIndex()得来(直接kvDB读取RaftApplyState)
	if c.Applied > 0 {
		raft.RaftLog.appliedTo(c.Applied)
	}

	raft.becomeFollower(raft.Term, None)

	// TODO: 打印一些信息

	return raft
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// send sends msg to other peers
func (r *Raft) send(msg pb.Message) {
	// 似乎应该进行必要的check，但是如果其他地方保证这个msg一定都弄完整了可能也就不用check了
	r.msgs = append(r.msgs, msg)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.Prs.
func (r *Raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to] // 惊了，之前误弄成r.Prs[r.id]!!!

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.entriesFrom(pr.Next) // 如果index<=ps.truncatedIndex()，会返回ErrCompact，然后就知道要发送snapshot了

	if errt != nil || erre != nil { // need to send snapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable { // 还未生成好
				return false
			}
			return false // 虽然不可能出其他错，但是就不直接panic了
		}
		m := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snapshot,
		}
		r.send(m)

		//r.RaftLog.maybeCompact() // TODO: leader有了snapshot后应该要尝试截断吧？
	} else {
		m := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			Index:   pr.Next - 1, // prevLogIndex
			LogTerm: term,
			Entries: transformToPointers(ents),
			Commit:  r.RaftLog.committed,
		}
		r.send(m)
	}
	return true
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id)
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match), // 可不能忘了这个发送的commit不能超过
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.pastRandomizedElectionTimeout() {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id}) // 通知当前节点该发起选举了 local msg不用设置term
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.pastHeartbeatTimeout() {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, To: r.id, From: r.id}) // 通知当前节点该发起心跳广播了 local msg不用设置term
		}
	}
}

func (r *Raft) pastRandomizedElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.electionTimeout
}

func (r *Raft) pastHeartbeatTimeout() bool {
	return r.heartbeatElapsed >= r.heartbeatTimeout
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
	//log.Infof("[raft %d](term %d) became follower", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() { // 只做一些基本的状态改变，发起投票这些在caller中继续做
	// Your Code Here (2A).
	r.reset(r.Term + 1) //term+=1
	r.State = StateCandidate
	r.Vote = r.id        // vote for itself
	r.votes[r.id] = true // vote for itself
	// already reset election timeout in r.reset
	//log.Info("[raft %d](term %d) became candidate", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id // 细节还是要注意，之前这个leader的Lead没有设置一直没问题，但是3a测试中需要用这个Lead来进行测试，没加上就会出问题了！

	r.PendingConfIndex = r.RaftLog.LastIndex() // txy文档关于confchange2：新当选leader需要保证之前任期的所有log都apply后才能进行新的confchagne（涉及单步配置变更的safety）

	// reset Progress
	r.forEachProgress(func(id uint64, pr *Progress) { // 我觉得只需要在becomeLeader中清空就行，becomeCandidate和becomeFollower中不需要
		*pr = Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	})

	// TODO: append noop entry
	noopEntry := pb.Entry{Data: nil}
	r.appendEntry(noopEntry) // 追加entries，并且相应修改Progress&commit

	log.Infof("[raft %d](term %d) became leader", r.id, r.Term)
}

func (r *Raft) appendEntry(es ...pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range es { // 修正entries的index/term信息
		es[i].Term = r.Term
		es[i].Index = lastIndex + uint64(i) + 1
	}
	lastIndex = r.RaftLog.append(es...) // 返回append后的lastIndex

	// 每次append entry后都要修正下当前节点(leader)的Progress的match，以及如果是单节点的话commit也可以直接增长

	pr := r.Prs[r.id] // 把对pr==nil的判断提前到了Step入口处，直接ErrStepPeerNotFound
	pr.MaybeUpdate(lastIndex)
	r.maybeCommit() // 我觉得是在如果只有一个节点的情况下可能可以直接增长commit
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybeCommit() bool {
	if len(r.Prs) == 0 {
		return false
	}
	matchIndex := make(uint64Slice, len(r.Prs))
	idx := 0
	for _, p := range r.Prs {
		matchIndex[idx] = p.Match
		idx++
	}
	sort.Sort(matchIndex)
	mci := matchIndex[len(matchIndex)-r.quorum()] // 这么实现非常巧妙，这个位置的matchIndex是大多数节点都达到的
	return r.RaftLog.maybeCommit(mci, r.Term)
}

// reset used in becomeXXX 统一初始化某些状态
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None // 之前term的vote过期
	}

	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.resetRandomizedElectionTimeout()

	r.leadTransferee = None

	r.PendingConfIndex = 0

	r.votes = make(map[uint64]bool) // 发起选举的投票记录清空
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// 本节点已经不再集群中了！
	if r.Prs[r.id] == nil { // 在TestTransferNonMember3A发现这个问题，出现不属于当前cluster的节点时，这里Prs会为nil（我觉得应该在其他某处处理这种node不属于集群的问题比较好，目前暂时先这样）
		log.Debugf(fmt.Sprintf("node %d not exist", r.id))
		r.becomeFollower(r.Term, None)
		return ErrStepPeerNotFound
	}

	// deal with m.Term
	switch {
	case m.Term == 0:
	// local message
	case m.Term < r.Term:
		//stale message, ignore
		return nil
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			// 这些都是leader发来的消息
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	// 这之后确保m.Term == r.Term

	// deal with MsgHup & MsgRequestVote
	switch m.MsgType {
	case pb.MessageType_MsgHup: // 这个消息只有candidate/follower会真正发起投票
		if r.State != StateLeader {
			r.campaign()
		}
	case pb.MessageType_MsgRequestVote: // 各个角色都应该对requestVote请求进行某种响应
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.isUpToDate(m.LogTerm, m.Index) { // 赞成
			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: false})
			r.electionElapsed = 0 // 投票的节点不应该在近期又发起投票
			r.Vote = m.From
		} else { // 反对
			r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true})
		}
	}

	// follower/candidate/leader分别各自处理
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)

	case pb.MessageType_MsgTransferLeader: // 来自TestLeaderTransferToUpToDateNodeFromFollower3A, follower收到leader transfer直接发起选举就行
		r.campaign()
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // 里面会清零electionElapsed、设置r.Lead
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)

	case pb.MessageType_MsgRequestVoteResponse: // 处理投票结果
		r.votes[m.From] = !m.Reject //
		voteFor, voteAgainst := r.countVotes()
		switch r.quorum() {
		case voteFor: // 选举成功
			r.becomeLeader() // 会追加一个noop entry，目的是为了让之前term的entries也都提交
			r.bcastAppend()  // becomeLeader后应该立刻广播Append
		case voteAgainst: // 选举失败
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow: // ignore
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	pr := r.Prs[m.From]
	if pr == nil && m.MsgType != pb.MessageType_MsgBeat && m.MsgType != pb.MessageType_MsgPropose { // 只有这两类消息不需要用到progress
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgBeat: // local msg 让leader发出一轮广播，在tick中会Step这个消息
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose: // 追加新entries
		if r.leadTransferee != None {
			log.Debugf(fmt.Sprintf("%d in TransferLeader, shouldn't accept new proposal", r.id))
			return ErrProposalDropped
		}

		if len(m.Entries) == 0 {
			log.Fatal(fmt.Sprintf("%d stepped empty MessageType_MsgPropose", r.id))
		}

		// txy文档关于confchange1：只有当前共识组的最新ConfChange日志apply后才可以接收新的ConfChange
		for i, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				var cc pb.ConfChange
				if err := cc.Unmarshal(e.Data); err != nil {
					return err
				}
				if r.PendingConfIndex > r.RaftLog.applied {
					log.Debugf(fmt.Sprintf("%d ignore new cc due to previous cc not applied", r.id))
					m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal} // 偷梁换柱直接替换成Normal entry! -- 这样在apply的过程里注意看看要不要改下，这种空entry在apply过程中不知会不会出问题
				} else {
					r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
				}
			}
		}

		// []*Entry -> []Entry
		es := transformFromPointers(m.Entries)
		r.appendEntry(es...)
		r.bcastAppend()

	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			if pr.MaybeDecrTo(m.Index) { // need to update next
				r.sendAppend(m.From) // resend append
			}
		} else {
			if r.leadTransferee != None && m.From == r.leadTransferee { // 说明leadTransferee日志跟上了，可以立刻发出MsgTimeoutNow
				log.Debugf(fmt.Sprintf("%d prepare TransferLeader to %d, log catched up, send MsgTimeoutNow immediately", r.id, r.leadTransferee))
				r.send(pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: m.From, From: r.id, Term: r.Term})
			}
			if pr.MaybeUpdate(m.Index) {
				if r.maybeCommit() {
					r.bcastAppend() // 让follower知道commit的推进
				}
			}
		}

	// 所以其实tinykv这里也是通过leader-follower心跳来检测是否需要再发Append消息
	case pb.MessageType_MsgHeartbeatResponse: // 对这个回复不做任何处理，就只是检查下是否需要再发下AppendEntries，之后要做读写分离readindex时候，可能需要一次心跳来保证leader的合法性
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader: // msg.From->transferee
		// 检查transferee是否log已经up-to-date
		// 如果已经up-to-date就直接发送个MsgTimeoutNow
		// 如果暂时未up-to-date，就帮助follower，发送Append msg，并停止接收新的proposal
		if m.From == r.id {
			log.Debugf(fmt.Sprintf("%d already is leader, no need to transfer", r.id))
			return nil
		}
		r.leadTransferee = m.From             // 通过这个标记就可以停止接收新的proposals
		if pr.Match < r.RaftLog.LastIndex() { // 发送之后就等着response了？要是response没到呢？
			log.Debugf(fmt.Sprintf("%d prepare TransferLeader to %d, sendAppend to help it catch up", r.id, r.leadTransferee))
			r.sendAppend(m.From)
		} else { // 直接发送MsgTimeoutNow 让leadTransferee立刻发起选举
			log.Debugf(fmt.Sprintf("%d prepare TransferLeader to %d, send MsgTimeoutNow immediately", r.id, r.leadTransferee))
			r.send(pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: m.From, From: r.id, Term: r.Term})
		}
	}
	return nil
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) { // 只需要看看用不用修正commit
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit) // 里面会检查m.Commit是否超过了RaftLog.LastIndex
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed { // leader发来的index太小了，应该要修正其progress.next
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: r.RaftLog.committed, Reject: false})
		return
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, transformFromPointers(m.Entries)...); ok {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: mlastIndex, Reject: false})
	} else {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: m.Index, Reject: true})
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snap := m.Snapshot
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: false,
	}
	if r.restoreSnapshot(*snap) { // 成功的话就直接
		msg.Index = r.RaftLog.LastIndex()
	} else {
		msg.Index = r.RaftLog.committed
	}
	r.send(msg)

}

// restore states in snapshot.Metadata
func (r *Raft) restoreSnapshot(snap pb.Snapshot) bool { // 最好用值传递？怕遇到go语法问题，现在go底层机制还不是很熟

	l := r.RaftLog
	if snap.Metadata.Index <= l.committed { // stale snapshot，just return committed
		return false
	}
	if l.matchTerm(snap.Metadata.Index, snap.Metadata.Term) { // snapshot最后一个位置也已经在当前日志中存在了，stale snapshot
		l.commitTo(snap.Metadata.Index) // 至少说明到index位置一定是committed，否则不可能在leader里被截断
		return false
	}

	// 以下情况可以apply这个snapshot，要把信息全部修改成只有这个snapshot，所有entries都情况（其他参数一并修改）

	// 应用Metadata.Index/Term —— 相当于把当前节点改成只有
	metaIndex, metaTerm := snap.Metadata.Index, snap.Metadata.Term
	l.entries = nil
	l.pendingSnapshot = &snap // 等待reay中进行apply
	l.stabled = metaIndex
	l.applied = metaIndex
	l.committed = metaIndex
	l.truncatedIndex = metaIndex
	l.truncatedTerm = metaTerm
	l.offset = metaIndex + 1

	// 应用Metadata.ConfState —— raft中保存peers信息在Progress这个map中，所以这个map要按照confState重塑
	r.Prs = make(map[uint64]*Progress)
	for _, id := range snap.Metadata.ConfState.Nodes { // TODO: 其实我觉得Progress中具体值不需要设置，毕竟becomeLeader后会重新设置，而现在只是follower
		match, next := uint64(0), l.LastIndex()+1
		if id == r.id {
			match = next - 1
		}
		r.Prs[id] = &Progress{Match: match, Next: next}
	}

	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{Match: uint64(0), Next: r.RaftLog.LastIndex() + 1}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	r.maybeCommit() // 删去一个节点，可能有些原先不能commit的日志现在可以commit了
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
	//log.Info(fmt.Sprintf("%d term %d randomizedElectionTimeout=%d", r.id, r.Term, r.randomizedElectionTimeout))
}

func (r *Raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.Prs {
		f(id, pr)
	}
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 { // 如果只有一个节点，直接变成leader
		r.becomeLeader()
		return
	}
	// 广播RequestVote请求
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVote, To: to, From: r.id, Term: r.Term,
			LogTerm: r.RaftLog.lastTerm(), Index: r.RaftLog.LastIndex()}) // 我的天那，Index居然赋值成了r.RaftLog.lastTerm()，居然到project2ab的最后才发现
	}
}

func (r *Raft) countVotes() (voteFor, voteAgainst int) {
	voteFor, voteAgainst = 0, 0
	for _, v := range r.votes {
		if v {
			voteFor++
		} else {
			voteAgainst++
		}
	}
	return
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

/****************** for randomized timeout ****************/
// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}
