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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct { // 为什么SoftState用指针，HardState不用指针？
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevSoftState *SoftState
	prevHardState pb.HardState // isHardStateEqual
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	prevSoftState := raft.softState()
	prevHardState := raft.hardState()
	return &RawNode{Raft: raft, prevSoftState: prevSoftState, prevHardState: prevHardState}, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
// rn.Step是给其他peers传来的msgs，所以会过滤MsgHup和MsgBeat这样的local msg（不过不会过滤MsgPropose，可能是因为MsgPropose可能会从follower转发到leader）
// 因此Campaign()和Propose()函数中要直接调用rn.Raft.Step
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	r := rn.Raft
	l := rn.Raft.RaftLog
	ready := Ready{}

	// 测试样例指出 SoftState和HardState如果未有变化就赋值为nil，而Entries CommittedEntries空的话赋值为[]pb.Entry{}
	// SoftState
	if !isSoftStateEqual(r.softState(), rn.prevSoftState) { // 测试样例指出，如果softState未改变，ready.SoftState应为nil
		ready.SoftState = r.softState()
	}
	// HardState
	if hardstate := r.hardState(); !IsEmptyHardState(hardstate) && !isHardStateEqual(hardstate, rn.prevHardState) {
		ready.HardState = r.hardState()
	}
	// unstabled Entries
	ready.Entries = l.unstableEntries()
	// committed entries
	ready.CommittedEntries = l.nextEnts()
	// messages
	ready.Messages = r.msgs
	// Snapshot
	if l.hasPendingSnapshot() {
		ready.Snapshot = *l.pendingSnapshot
	}

	return ready
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	if !isSoftStateEqual(rn.prevSoftState, rn.Raft.softState()) {
		return true
	}
	if hardstate := rn.Raft.hardState(); !IsEmptyHardState(hardstate) && !isHardStateEqual(rn.prevHardState, hardstate) {
		return true
	}
	if rn.Raft.msgs != nil && len(rn.Raft.msgs) > 0 {
		return true
	}
	if rn.Raft.RaftLog.hasPendingSnapshot() {
		return true
	}
	if rn.Raft.RaftLog.hasUnstableEntries() {
		return true
	}
	// 如果有committed entries的话hardState.Commit会变化
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// messages
	rn.Raft.msgs = nil
	// softstate & hardstate
	rn.prevSoftState = &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
	rn.prevHardState = pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
	// stable index
	rn.Raft.RaftLog.stabled = rn.Raft.RaftLog.LastIndex()
	// apply index
	rn.Raft.RaftLog.appliedTo(rn.Raft.RaftLog.committed) // 同步apply
	// TODO: snapshot advance
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
	// 这里把From成员设置成transferee是为了通过这种方式将transferee传递给raft层吗
}
