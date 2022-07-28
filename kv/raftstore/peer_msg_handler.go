package raftstore

import (
	"bytes"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return // 当前节点不需要处理ready
	}
	rd := d.RaftGroup.Ready()

	if d.IsLeader() { // 根据raft博士论文10.2.1，如果是leader可以先发送msgs再持久化
		d.Send(d.ctx.trans, rd.Messages)
	}

	// 调用ps.SaveReadyState持久化log entries和一些元数据
	// 里面可能持久化unstabled entries / RaftLocalState(HardState(Term/Vote/Commit)/LastIndex/LastTerm) / RaftApplyState(AppliedIndex/Snapshot信息)&Snapshot的apply
	applyResult, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}

	// 发送消息
	if !d.IsLeader() {
		d.Send(d.ctx.trans, rd.Messages)
	}

	//d.Send(d.ctx.trans, rd.Messages) -- 按理说消息重复发送应该也要能正确处理

	if applyResult != nil {
		// change storeMeta
		//d.ctx.storeMeta.Lock()
		//defer d.ctx.storeMeta.Unlock()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applyResult.Region})
		d.ctx.storeMeta.regions[applyResult.Region.Id] = applyResult.Region
	}

	// apply committed entries -- 每一个entry apply到kvDB后都要同时原子修改applyState.AppliedIndex以保证apply safety（来自txy博客）
	for _, entry := range rd.CommittedEntries {
		if d.stopped { // TODO: 这里为什么要加？
			return
		}

		kvWB := new(engine_util.WriteBatch)
		d.peerStorage.applyState.AppliedIndex = entry.Index
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		d.process(&entry, kvWB) // process中进行WriteToDB
	}

	// call RaftGroup(RawNode).Advance推进raft状态机状态
	d.RaftGroup.Advance(rd)
}

// 特别注意process里面要把kvWB写入kvDB中！
func (d *peerMsgHandler) process(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryNormal {
		request := new(raft_cmdpb.RaftCmdRequest) // RaftCmdRequest是在RaftStorage.Write/Reader中生成的（可能来自其他peers），直到这里终于开始处理
		err := request.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}

		p := d.getProposal(entry)

		if request.AdminRequest == nil {
			d.applyNormalRequest(request.Requests, kvWB, p) // Normal request
		} else {
			// 对split进行CheckRegionEpoch, 其他normal request和CompactLog不需要检查RegionEpoch!
			if request.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split {
				if err = util.CheckRegionEpoch(request, d.Region(), true); err != nil {
					log.Infof("region %v store %v applySplitRequest index=%d CheckRegionEpoch fail, stop apply this entry", d.regionId, d.storeID(), entry.Index)
					if p != nil {
						p.cb.Done(ErrResp(err))
					}
					return
				}
			}
			d.applyAdminRequest(request.AdminRequest, kvWB, p) // CompactLog & Split (TransferLeader不用propose就直接执行了）
		}
	} else { // eraftpb.EntryType_EntryConfChange
		cc := &eraftpb.ConfChange{} // cc.Context直接方RaftCmdRequest
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		// 在applyConfChange中进行了CheckRegionEpoch
		d.applyConfChange(entry, cc, kvWB) // ChangePeer
	}
}

func (d *peerMsgHandler) applyConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) {
	// 1.修改并保存新的RegionLocalState 里面的Region.RegionEpoch和Region.Peers要修改 -- 读出现有的RegionLocalState然后修正然后再保存？
	// 2.对于add node, 之后peer会通过storeWorker的maybeCreatePeer创建(leader会发送snapshot过来) -- 意思是不用手动调用什么？
	//   对于remove node, 如果当前peer就是被删除的peer，需要调用destroyPeer()来明确删除Peer(如果当前peer不是被删除的peer，那只需要修改下region和raft内一些peer相关的状态就行)
	// 3.update the region state in storeMeta of GlobalContext
	// 4.对peer.PeerCache的修改
	// 5.调用RawNode.ApplyConfChange - 会进行add/remove node，然后返回最新的peers
	// 6.保证即使执行duplicate commands of the same confchange也能正常

	// 假设regionA在各个store上分布：store1-peer1 store2-peer2 store3-peer3
	// 要删除store1上peer1，就要在store1上调用destroyPeer()，而store2 store3上只要相应修改下元数据，知道store1上peer1已经不在raftgroup中了即可

	originRequest := new(raft_cmdpb.RaftCmdRequest)
	err := originRequest.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	// checkRegionEpoch first!
	p := d.getProposal(entry)
	if err = util.CheckRegionEpoch(originRequest, d.Region(), true); err != nil {
		log.Infof("region %v store %d applyConfChange index=%d CheckRegionEpoch fail, stop apply this entry", d.regionId, d.storeID(), entry.Index)
		if p != nil {
			p.cb.Done(ErrResp(err))
		}
		return
	}

	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() { // 如果当前peer就是要删去的peer，调用destroyPeer将该peer从这个store中删去
		d.destroyPeer() // 这里面会把RaftLocalState/RaftApplyState都删除
	} else {
		region := d.Region() // 从peerstorage中读出来
		region.RegionEpoch.ConfVer++

		if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
			// 修改region.Peers，删去其中删去的PeerID
			var newpeers []*metapb.Peer
			for _, p := range region.Peers {
				if p.GetId() != cc.NodeId { // 除了要删去的peerID，其他都保留
					newpeers = append(newpeers, p)
				}
			}
			region.Peers = newpeers

			d.peer.removePeerCache(cc.NodeId)
		} else {
			// 修改region.Peers，加上新增的PeerID
			newpeer := originRequest.AdminRequest.ChangePeer.Peer
			if err != nil {
				panic(err)
			}
			region.Peers = append(region.Peers, newpeer)

			d.peer.insertPeerCache(newpeer)
		}

		d.ctx.storeMeta.regions[d.regionId] = region
		d.SetRegion(region)

		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		kvWB.MustWriteToDB(d.ctx.engine.Kv) // 话说每个peer都要把RegionLocalState重新写一遍
		d.RaftGroup.ApplyConfChange(*cc)
	}

	if p != nil {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()},
			},
		}
		p.cb.Done(resp)
	}
}

func (d *peerMsgHandler) applyAdminRequest(adminRequest *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch, p *proposal) {
	switch adminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog: // 做了哪些改动，持久化相关信息
		d.applyCompactLogRequest(adminRequest.CompactLog, kvWB, p)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		panic("change peer should be dealt in applyConfChange")
	case raft_cmdpb.AdminCmdType_TransferLeader:
		panic("transfer leader shouldn't be dealt here, should already be dealt in propose cmd stage")
		// 其他request都是要propose-commit达成共识，然后在apply阶段处理，但是transfer leader不需要达成共识，直接propose前就应该处理，所以这个时候不应该出现这个类型命令
	case raft_cmdpb.AdminCmdType_Split:
		d.applySplitRequest(adminRequest.Split, kvWB, p)
	}
}

/*
***特别注意***
现在要进行CheckRegionEpoch和CheckKeyInRegion
只要进行了confchange和split这两个会改变regionEpoch的操作，就要检查epoch是否匹配（不匹配这个命令就不该执行） -- preProposeRaftCmd会进行checkRegionEpoch
这样的话duplicate commands的问题也会解决，因为每次split/confchange后epoch都变化，同样命令发几次就只会执行一次
用到key的地方也要进行CheckKeyInRegion，因为可能region进行了分裂,key可能已经不再里面了 - 那些put/get/delete命令apply时候都要检查下是否keyInRegion
*/

func regionToString(region *metapb.Region) string {
	return fmt.Sprintf("Id %v RegionEpoch %v StartKey %v EndKey %v Peers %v",
		region.Id, region.RegionEpoch, string(region.StartKey), string(region.EndKey), region.Peers)
}

func (d *peerMsgHandler) applySplitRequest(splitRequest *raft_cmdpb.SplitRequest, kvWB *engine_util.WriteBatch, p *proposal) {
	originRegion := d.Region()
	originStartKey := originRegion.StartKey
	originEndKey := originRegion.EndKey

	if err := util.CheckKeyInRegion(splitRequest.SplitKey, originRegion); err != nil {
		log.Infof("splitKey %v not in Region %v", splitRequest.SplitKey, d.Region().GetId())
		if p != nil {
			p.cb.Done(ErrResp(err))
		}
	}

	if bytes.Compare(splitRequest.SplitKey, originEndKey) == 0 || bytes.Compare(splitRequest.SplitKey, originStartKey) == 0 {
		return
	}

	originRegion.RegionEpoch.Version++

	var newPeers []*metapb.Peer
	for i, newPeerId := range splitRequest.NewPeerIds {
		newPeers = append(newPeers, &metapb.Peer{Id: newPeerId, StoreId: originRegion.Peers[i].StoreId})
	}

	newPeerEndKey := []byte{}
	newPeerStartKey := []byte{}

	if engine_util.ExceedEndKey(splitRequest.SplitKey, originEndKey) {
		newPeerStartKey = originEndKey
		newPeerEndKey = splitRequest.SplitKey
	} else {
		originRegion.EndKey = splitRequest.SplitKey
		newPeerStartKey = splitRequest.SplitKey
		newPeerEndKey = originEndKey
	}

	newRegion := &metapb.Region{ // newRegion的version可以是从1开始的吧
		Id:       splitRequest.NewRegionId,
		StartKey: newPeerStartKey,
		EndKey:   newPeerEndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: newPeers,
	}

	log.Infof("applySplitRequest() store %v splitkey %v\noriginRegion %v\nnewRegion    %v",
		d.storeID(), string(splitRequest.SplitKey), regionToString(originRegion), regionToString(newRegion))

	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: originRegion})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	d.ctx.storeMeta.regions[splitRequest.NewRegionId] = newRegion
	d.ctx.storeMeta.regions[d.regionId] = originRegion

	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion) // 在当前store上创建相应region
	if err != nil {
		panic(err)
	}
	d.ctx.router.register(newPeer)
	d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})

	meta.WriteRegionState(kvWB, originRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

	kvWB.MustWriteToDB(d.ctx.engine.Kv) // 一开始忘了持久化...导致region修改的信息没有更新

	if p != nil {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
			AdminResponse: &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{originRegion, newRegion}, // TODO: 返回regions?
				}},
		}
		p.cb.Done(resp)
	}

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) applyCompactLogRequest(req *raft_cmdpb.CompactLogRequest, kvWB *engine_util.WriteBatch, p *proposal) {
	// 该截断了
	if d.peerStorage.applyState.TruncatedState.Index >= req.CompactIndex {
		return
	}
	d.peerStorage.applyState.TruncatedState.Index = req.CompactIndex
	d.peerStorage.applyState.TruncatedState.Term = req.CompactTerm
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	// 真正删除不再使用的raftDB中的entries的工作交给raftlog-gc worker异步完成 -- 很好奇到底怎样异步完成的，有时间仔细看看
	d.ScheduleCompactLog(req.CompactIndex)

	kvWB.WriteToDB(d.peerStorage.Engines.Kv) // 最初居然忘了这里进行持久化！！！

	if p != nil {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
				CompactLog: &raft_cmdpb.CompactLogResponse{},
			},
		}
		p.cb.Done(resp)
	}
}

func getKeyInNormalRequest(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	default:
		return nil
	}
}

func (d *peerMsgHandler) applyNormalRequest(requests []*raft_cmdpb.Request, kvWB *engine_util.WriteBatch, p *proposal) {
	// 只有put/delete需要在WriteBatch中处理
	for _, request := range requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(request.Delete.Cf, request.Delete.Key)
		}
	}

	// 如果是follower节点进行apply，那就会直接跳过对proposals的处理（因为根本没有对应的proposal，只有leader节点有对应的proposal等待callback）

	if p != nil {
		raftCmdResponse := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()}, // Write/Reader的checkResponse中会检查resp.Heder.Error != nil，所以我想这个Header必须要不是ni
		}
		var responses []*raft_cmdpb.Response
		for _, request := range requests { // 实际上requests中要么都是读，要么都是写 不会有读写混合
			key := getKeyInNormalRequest(request)
			if key != nil {
				if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
					raftCmdResponse.Header.Error = util.RaftstoreErrToPbError(err)
				}
			}
			switch request.CmdType {
			case raft_cmdpb.CmdType_Put:
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			case raft_cmdpb.CmdType_Delete:
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			case raft_cmdpb.CmdType_Get:
				val, _ := engine_util.GetCF(d.ctx.engine.Kv, request.Get.Cf, request.Get.Key)
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				})
			case raft_cmdpb.CmdType_Snap: // 返回一个txn
				responses = append(responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				})
				p.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			default:
				log.Fatal("unknown CmdType")
			}
		}
		raftCmdResponse.Responses = responses

		p.cb.Done(raftCmdResponse)
	}

	kvWB.WriteToDB(d.ctx.engine.Kv)
}

func (d *peerMsgHandler) getProposal(entry *eraftpb.Entry) *proposal { // 没有proposal就返回nil
	for len(d.proposals) > 0 && d.proposals[0].index < entry.Index { // remove stale proposals
		// stale cmd
		p := d.proposals[0]
		d.proposals = d.proposals[1:]
		NotifyStaleReq(p.index, p.cb)
	}

	if len(d.proposals) > 0 && d.proposals[0].index == entry.Index { // 找到对应proposal
		p := d.proposals[0]
		d.proposals = d.proposals[1:]
		if entry.Term == p.term {
			return p // 只有这种情况有对应proposal
		} else {
			NotifyStaleReq(p.term, p.cb)
		}
	}
	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, string(split.SplitKey))
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	// 如果是Transfer Leader命令，不需要propose，直接处理
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		d.RaftGroup.TransferLeader(msg.AdminRequest.GetTransferLeader().Peer.GetId())
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
		return
	}

	// Your Code Here (2B).
	index := d.RaftGroup.Raft.RaftLog.LastIndex() + 1

	// 如果是confchange，要使用rawnode.ProposeConfChange，其他就用Propose
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		cp := msg.AdminRequest.GetChangePeer()
		ctx, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		cc := eraftpb.ConfChange{
			ChangeType: cp.ChangeType,
			NodeId:     cp.GetPeer().GetId(),
			Context:    ctx, // 直接传递msg参数，之后要反序列化出来用于CheckRegionEpoch还有add peer中的peer参数
		}
		err = d.RaftGroup.ProposeConfChange(cc)
	} else {
		data, err := msg.Marshal() // 序列化msg
		if err != nil {
			if cb != nil {
				cb.Done(ErrResp(err))
			}
			return
		}
		err = d.RaftGroup.Propose(data)
	}

	if err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	if index == d.RaftGroup.Raft.RaftLog.LastIndex()+1 {
		if cb != nil {
			cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId}))
		}
		return
	}
	// propose成功，将callback记录入peer.proposal中
	if cb != nil { // callback == nil 干脆不用加入proposals了
		d.proposals = append(d.proposals, &proposal{
			index: index,
			term:  d.Term(),
			cb:    cb,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		log.Infof("In onRaftMsg checkSnapshot error %v", err)
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		log.Infof("In onRaftMsg  RaftGroup.Step error %v", err)
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale %v, current %v ignore it",
			regionID, msgType, msg.RegionEpoch, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
