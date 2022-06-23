package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

func (txn *MvccTxn) appendModifyData(data interface{}) { // data - storage.Put/Delete
	txn.writes = append(txn.writes, storage.Modify{Data: data})
}

// 各种PutWrite/PutLock/PutValue就是往相应Cf中添加put记录，不过修改是加入到writes这个Modify数组中的
// 各种DeleteLock/DeleteValue就是往相应Cf中添加delete记录，修改仍是加入到writes这个Modify数组中的
//     PutWrite中write参数的WriteKind已经能够指定是put/delete，所以不需要再加上个DeleteWrite这个函数  而Default/Lock两个列族增加删除记录都需要明确调用DeleteLock/DeleteValue -- 删除记录究竟是用什么表示的，某种标记？
// 各种GetLock/GetValue/GetWrite(CurrentWrite/MostRecentWrite)就是从相应Cf中找到对应写入记录了

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, ts), // ts是commit_ts
		Value: write.ToBytes(),    // write中包含start_ts & WriteKind
		Cf:    engine_util.CfWrite,
	}
	txn.appendModifyData(put)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	val, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	// 可能这个key根本没有对应的锁
	if val == nil {
		return nil, nil
	}
	lock, err := ParseLock(val)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   key, // Lock列族直接用userKey查询即可，不需要timestamp
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	txn.appendModifyData(put)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	txn.appendModifyData(del)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) { // 是读取"这个事务能够看到"的value，要先从write列找到对应记录，然后根据这个记录(找到的start_ts形成encodedKey)中去找对应Default中value
	// Your Code Here (4A).
	// 现在遍历CfWrite列族 要找到时间戳<start_ts并且writeKindPut的那个值或者找到writeKindDelete就知道已经被删除了  CfWrite是先按照key排序，再按照ts降序排序
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key, txn.StartTS))

	var startTs uint64
	for iter.Valid() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if bytes.Compare(userKey, key) != 0 { // 找到的根本已经不是想要的userKey了
			return nil, nil
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, err
		}
		// 除了rollback要忽略（因为rollback后相当于这个txn没有存在过）其他put/delete都要处理
		if write.Kind == WriteKindPut { // 是put记录，find it!
			// userKey/ts符合要求的WriteKindPut记录
			startTs = write.StartTS // 拿到start_ts就可以去CfDefault真正找数据了   write的commit_ts在encodedKey中，start_ts在write.StartTS中
			break
		} else if write.Kind == WriteKindDelete { // 这个key已经被删除了
			return nil, nil
		}
		iter.Next()
	}
	// 根据key和start_ts在CfDefault中找
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, startTs))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	txn.appendModifyData(put)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{
		Key: EncodeKey(key, txn.StartTS), // CfDefault上需要encodedKey
		Cf:  engine_util.CfDefault,
	}
	txn.appendModifyData(del)
}

// 这两个关键还是得知道使用场景到底是什么，不然感觉有点不清不楚
// 应该是只需要WriteKindPut/WriteKindDelete这种的记录吧，rollback也需要吗？？——感觉还是要看使用场景
// 一个理论上看似简单的算法的实现其实真的有很多细节啊！！！！

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 好像是要找当前txn的写入CfWrite记录，但是因为CfWrite中start_ts是在value中的，encodedKey是userKey+start_ts  因为不知道commit_ts，所以先使用最大的ts--TsMax开始遍历
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) { // 应该是要当前txn写入的，不论put/delete/rollback（可以直接遍历所有的，找到userKey/startTs对应的即可）
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	iter.Seek(EncodeKey(key, TsMax))
	for iter.Valid() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if bytes.Compare(userKey, key) != 0 { // 已经不是超过key的范围了
			return nil, 0, nil
		}
		val, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}
		if write.StartTS != txn.StartTS { // 并非当前txn写入的Write记录
			iter.Next()
			continue
		} else {
			commitTs := decodeTimestamp(item.Key())
			return write, commitTs, nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 我感觉应该是找出来看看有没有write conflict的  在当前事务要提交的时候，看看有没有更新的事务已经提交
// 我这里的语义其实是MostRecentCommitWrite，不管Rollback的
// 但是有地方可能即使是rollback也需要，这点注意下
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) { // 应该是要最近写入的，不要rollback的这种 是需要当前txn之前的写入呢？还是不用管？（这牵涉到是从TsMax还是StartTs开始找起）
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	iter.Seek(EncodeKey(key, TsMax))
	for iter.Valid() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if bytes.Compare(userKey, key) != 0 {
			return nil, 0, nil
		}
		val, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}
		if write.Kind == WriteKindRollback {
			iter.Next()
			continue
		} else { // put/delete
			commitTs := decodeTimestamp(item.Key())
			return write, commitTs, nil
		}
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts) // 将ts取反后以大端形式拼接在key之后，这样就能先按key升序，再按ts降序
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp. -- timestamp中有一部分直接采用physical time
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
