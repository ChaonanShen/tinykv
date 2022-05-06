package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// TODO: 目前还不关注error handling，response中将error分成regionError和其他error，以后再处理
	// 这种写法借鉴自distributed-txn lab中，用上这个setError可太清晰了，原先写法到处都是return语句
	rsp := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()

	if !setError(err, rsp) {
		val, err := reader.GetCF(req.Cf, req.Key)
		if !setError(err, rsp) {
			if val == nil {
				rsp.NotFound = true
			} else {
				rsp.Value = val
			}
		}
	}

	return rsp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	rsp := &kvrpcpb.RawPutResponse{}

	modify := []storage.Modify{
		{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}},
	}
	err := server.storage.Write(req.Context, modify)
	setError(err, rsp)

	return rsp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	rsp := &kvrpcpb.RawDeleteResponse{}

	modify := []storage.Modify{
		{Data: storage.Delete{Key: req.Key, Cf: req.Cf}},
	}
	err := server.storage.Write(req.Context, modify)
	setError(err, rsp)

	return rsp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rsp := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()

	if !setError(err, rsp) {
		iter := reader.IterCF(req.Cf)
		defer iter.Close()

		for iter.Seek(req.StartKey); iter.Valid() && len(rsp.Kvs) < int(req.Limit); iter.Next() {
			key := iter.Item().KeyCopy(nil)
			val, err := iter.Item().ValueCopy(nil)
			if !setError(err, rsp) {
				rsp.Kvs = append(rsp.Kvs, &kvrpcpb.KvPair{Key: key, Value: val})
			} else {
				break
			}
		}
	}

	return rsp, err
}

// return true if there is error, set regionError & other error in response separately
func setError(err error, rsp interface{}) bool {
	if err == nil {
		return false
	}
	// TODO: regionError要专门设置，有了这个函数加上去非常方便
	switch rsp.(type) {
	case kvrpcpb.RawGetResponse:
		rsp.(*kvrpcpb.RawGetResponse).Error = err.Error()
	case kvrpcpb.RawScanResponse:
		rsp.(*kvrpcpb.RawScanResponse).Error = err.Error()
	case kvrpcpb.RawDeleteResponse:
		rsp.(*kvrpcpb.RawDeleteResponse).Error = err.Error()
	case kvrpcpb.RawPutResponse:
		rsp.(*kvrpcpb.RawPutResponse).Error = err.Error()
	}
	return true
}
