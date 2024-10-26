package standalone_storage

import (
	"errors"
	"fmt"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	dbOpts badger.Options
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	return &StandAloneStorage{dbOpts: opts}
}

func (s *StandAloneStorage) Start() error {
	var err error
	s.db, err = badger.Open(s.dbOpts)
	if err != nil {
		return fmt.Errorf("badger open: %w", err)
	}

	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db != nil {
		return fmt.Errorf("db close: %w", s.db.Close())
	}

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db closed")
	}

	return &dbReader{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(data.Cf, data.Key), data.Value)
				if err != nil {
					return fmt.Errorf("txn set: %w", err)
				}
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key))
				if err != nil {
					return fmt.Errorf("txn delete: %w", err)
				}
			}

		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("db update: %w", err)
	}

	return nil
}

type dbReader struct {
	txn *badger.Txn
}

var _ storage.StorageReader = &dbReader{}

func (r *dbReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("get cf: %w", err)
	}

	return val, nil
}

func (r *dbReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *dbReader) Close() {
	r.txn.Discard()
}
