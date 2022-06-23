package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)   ---   在4A中怎么使用？
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// 目前可以不管RegionError，测试里好像也不测试这个RegionError，真的要加上也只要每个函数开头加上个检测RegionError的即可
// 都要使用MvccTxn的api，这样会用上CfWrite/CfLock来帮助获得原子性

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	// 哪些error是要设置到GetResponse中去的？哪些是直接返回？
	// 我感觉是跟事务逻辑相关的error设置到GetResponse中去，其他非事务逻辑产生的err直接返回

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close() // 记得关闭reader!!!!
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.GetResponse{}

	txn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts < txn.StartTS { // 有个比较老的事务仍然把持这锁（如果是更新的事务把持锁不会有问题，因为mvcc只会读到当前事务能看到的数据）
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
	} else {
		resp.NotFound = false
		resp.Value = val
	}
	return resp, nil
}

func getKeysToLatch(mutations []*kvrpcpb.Mutation) [][]byte {
	var keys [][]byte
	for _, mut := range mutations {
		keys = append(keys, mut.Key)
	}
	return keys
}

// TODO: 各种命令都可能过时，也可能重复，要考虑完全这些情况的话还有很多要完善的地方
// 比如重复prewrite/commit 比如prewrite落后太久，发现当前txn早就commit成功了

// 先对每个mutationi中的key检查下有无write conflict或者key already locked —— 出错就在resp.Errors上追加错误
// 然后对每个mutations中的key上锁并且CfDefault中写入value
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	keysToLatch := getKeysToLatch(req.Mutations) // 直接上锁避免可能的多clients并发冲突
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.PrewriteResponse{}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 检查是否已经有更新的事务提交，如果有，为了避免write conflict就要返回错误让txn restart
	IsWriteConflict, IsKeyLocked := false, false

	for _, mut := range req.Mutations {
		// 检查是否有更新的txn commit了这个key —— write conflict
		write, commitTs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTs > req.StartVersion {
			// 如果发现commitTs==req.StartTs，说明已经commit成功了，那这个prewrite应该是迟到很久了 —— 不过目前测试没有测这种case，先不管

			IsWriteConflict = true
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: commitTs,
				Key:        mut.Key,
				Primary:    req.PrimaryLock,
			}})
			continue
		}

		// 检查是否已经上锁了
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         mut.Key,
				LockTtl:     lock.Ttl,
			}})
			IsKeyLocked = true
			continue
		}
	}

	if IsWriteConflict || IsKeyLocked {
		return resp, nil
	}

	// 没有出错，那就写入value和lock信息
	for _, mut := range req.Mutations {
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}

		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
			lock.Kind = mvcc.WriteKindDelete // 嘿嘿，这里弄成WriteKindPut也能过，不知道Lock的这个WriteKind到底应该是啥
		}
		txn.PutLock(mut.Key, lock)
		// 这个Lock的Kind是对应CfDefault的value是put还是del吧？lock本身没有put/delete区分吧
		// lock本身应该只有上锁、没上锁两种情况吧，putLock上锁 deleteLock解锁
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	resp := &kvrpcpb.CommitResponse{}

	// 记录那些当前事务已经commit成功的keys，避免repeated commit -- 如果已经commit了就不用重复putWrite了
	keyAlreadyWrite := make(map[string]bool)

	for _, key := range req.Keys {
		// check repeated KvCommit - 可能当前事务已经commit成功一次了
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil { // 这个key在当前事务中已经在CfWrite上有记录
			if write.Kind == mvcc.WriteKindRollback { // 如果是rollback的情形，要返回错误，因为说明之前prewrite后已经发送过KvRollback，但是不知为何又发送了KvCommit，这不应该发生
				resp.Error = &kvrpcpb.KeyError{Abort: "contradict requests"}
				return resp, nil
			}
			keyAlreadyWrite[string(key)] = true
			continue
		}

		// 判断是否上着本事务在prewrite阶段获取的锁 - 可能锁没了，或者被其他事务上了锁
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			resp.Error = &kvrpcpb.KeyError{Retryable: fmt.Sprintf("key %v lock not found", key)}
			return resp, nil
		} else if lock.Ts != req.StartVersion { // 锁已经编程其他事务的了！
			resp.Error = &kvrpcpb.KeyError{Retryable: fmt.Sprintf("key %v lock rollback", key)}
			return resp, nil
		}
	}

	// 到这里说明prewrite阶段的锁还都在
	for _, key := range req.Keys {
		txn.DeleteLock(key)
		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindPut,
		}

		if _, ok := keyAlreadyWrite[string(key)]; !ok {
			txn.PutWrite(key, req.CommitVersion, write)
		}
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).

	return nil, nil
}

// CheckTxnStatus返回约定:
// locked: lock_ttl > 0
// committed: commit_version > 0
// rolled back: lock_ttl == 0 && commit_version == 0 ———— rollback有TTLExpireRollback/LockNotExistRollback，靠Action区分

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	resp := &kvrpcpb.BatchRollbackResponse{}

	// 记录那些当前事务已经成功写入CfWrite的keys，避免repeated rollback
	keyAlreadyWrite := make(map[string]bool)

	for _, key := range req.Keys {
		// check repeated KvBatchRollback
		write, _, err := txn.CurrentWrite(key) // 看看当前事务有没有write记录
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback { // 矛盾，应该得是rollback记录
				resp.Error = &kvrpcpb.KeyError{Abort: "contradict requests"}
				return resp, nil
			}
			keyAlreadyWrite[string(key)] = true
			continue
		}
	}
	// rollback情况下不用管锁是否存在，哪怕存在也要删去(但是KvCommit场景就需要确保锁仍然是当前事务拥有，否则说明prewrite阶段有问题)

	// 到这里说明prewrite阶段的锁还都在
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion { // 如果当前事务这个key仍持有锁，那就删去  没有锁可能是其他事务已经将锁剥夺了
			txn.DeleteLock(key)
		}
		txn.DeleteValue(key)
		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		}

		if _, ok := keyAlreadyWrite[string(key)]; !ok {
			txn.PutWrite(key, req.StartVersion, write) // 在rollback情景下commitTs位置居然是用startTs代替？？？
		}
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

// 单纯地要么rollback要么commit这个txn
// 靠request.CommitVersion是否为0来判断是rollback还是commit
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).

	// 怎么找到这个事务相关的所有locks?遍历CfLock列吗，是不是还要加上CfWrite记录（如果没有的话）

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	resp := &kvrpcpb.ResolveLockResponse{}

	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for iter.Valid() { // 找到所有lock
		item := iter.Item()

		lock, err := txn.GetLock(item.Key())
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion { // 的确被当前事务锁上了
			txn.DeleteLock(item.Key())

			write, _, err := txn.CurrentWrite(item.Key())
			if err != nil {
				return nil, err
			}
			if write == nil { // 没有write记录
				if req.CommitVersion > 0 { // commit
					txn.PutWrite(item.Key(), req.CommitVersion,
						&mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindPut})
				} else { // rollback
					txn.PutWrite(item.Key(), req.StartVersion,
						&mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
					// delete value
					txn.DeleteValue(item.Key())
				}
			}
		}
		iter.Next()
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
