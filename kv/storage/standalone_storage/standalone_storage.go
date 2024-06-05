package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	/* 存储数据引擎 */
	engines engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	/* 创建一个数据库实例 */
	kvPath := conf.DBPath
	badgerDb := engine_util.CreateDB(kvPath, false)

	/* 返回单机存储引擎实例 */
	return &StandAloneStorage{
		engines: *engine_util.NewEngines(badgerDb, nil, kvPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	/* 关闭数据库 */
	if err := s.engines.Close(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	/* 开启事务，并且创建一个Reader */
	txn := s.engines.Kv.NewTransaction(false)
	return &diskReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	/* 开启一个写事务 */
	wb := new(engine_util.WriteBatch)

	/* 设置批量写入的指令 */
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}

	/* 写入数据 */
	if err := s.engines.WriteKV(wb); err != nil {
		return err
	}
	return nil
}

/* Badger Storage Reader */
type diskReader struct {
	txn *badger.Txn
}

// Close implements storage.StorageReader.
func (d *diskReader) Close() {
	// 提交事务
	d.txn.Commit()
}

// GetCF implements storage.StorageReader.
func (d *diskReader) GetCF(cf string, key []byte) ([]byte, error) {
	// 直接读取Badger存储引擎的值
	value, _ := engine_util.GetCFFromTxn(d.txn, cf, key)
	return value, nil
}

// IterCF implements storage.StorageReader.
func (d *diskReader) IterCF(cf string) engine_util.DBIterator {
	// 直接通过工具获取 Badger 存储引擎的 Iterator
	return engine_util.NewCFIterator(cf, d.txn)
}
