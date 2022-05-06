package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

/*
storage = standalone_storage.NewStandAloneStorage(conf)
err := storage.Start()
server := server.NewServer(storage)
*/

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Code Here (1).
	Kv     *badger.DB
	KvPath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{Kv: nil, KvPath: conf.DBPath}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.Kv = CreateDB(s.KvPath)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// 关闭DB，将目录也移除
	if err := s.Kv.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.KvPath); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewBadgerStorageReader(s.Kv), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 生成WriteBatch然后写入, wb.WriteToDB确保原子写入
	wb := new(engine_util.WriteBatch)
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := modify.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}
	return wb.WriteToDB(s.Kv)
}

// CreateDB creates a new Badger DB on disk at path.
func CreateDB(path string) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// BadgerStorageReader

type BadgerStorageReader struct {
	txn *badger.Txn
}

func NewBadgerStorageReader(db *badger.DB) *BadgerStorageReader {
	return &BadgerStorageReader{db.NewTransaction(false)} // no need to update
}

func (reader *BadgerStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound { // key not found不算作error，就用val==nil表示
		val, err = nil, nil
	}
	return val, err
}

func (reader *BadgerStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *BadgerStorageReader) Close() {
	reader.txn.Discard()
}
