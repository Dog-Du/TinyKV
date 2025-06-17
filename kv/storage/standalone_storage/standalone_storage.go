package standalone_storage

import (
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
	db *badger.DB // 实现 badger 键值 API 的一个包装器。from https://github.com/talent-plan/tinykv/blob/course/doc/project1-StandaloneKV.md
	// 所以只需要用 badger 来实现这个接口即可。
}

// 新建一个 StandAloneStorage 实例，并打开一个 badger 数据库。
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opt := badger.DefaultOptions // 默认 option 配置
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath // 两个路径需要单独配置，和conf保持一致

	db, err := badger.Open(opt) // 打开
	if err != nil {
		panic(err)
	}

	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// 启动一个 badger 数据库，什么都不需要做
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// close即可
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	// 应该使用 badger.Txn 来实现 Reader 功能，因为 badger 提供的事务处理器可以提供键和值的一致性快照。 from https://github.com/talent-plan/tinykv/blob/course/doc/project1-StandaloneKV.md
	txn := s.db.NewTransaction(false)

	// 返回实现的 StandAloneStorageReader
	return &StandAloneStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	// batch 是多个写操作的集合。
	// 使用 WriteBatch 来批量写入，然后写入到数据库中
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			wb.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			wb.DeleteCF(m.Cf(), m.Key())
		}
	}

	return wb.WriteToDB(s.db)
}
