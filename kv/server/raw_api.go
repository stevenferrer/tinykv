package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, fmt.Errorf("storage reader: %w", err)
	}

	val, err := r.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, fmt.Errorf("reader get cf: %w", err)
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{{
		Data: storage.Put{
			Cf:    req.GetCf(),
			Key:   req.GetKey(),
			Value: req.GetValue(),
		},
	}})
	if err != nil {

		return nil, fmt.Errorf("storage write: %w", err)
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(nil, []storage.Modify{{
		Data: storage.Delete{
			Cf:  req.GetCf(),
			Key: req.GetKey(),
		},
	}})
	if err != nil {
		return nil, fmt.Errorf("storage write: %w", err)
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, fmt.Errorf("storage reader: %w", err)
	}

	iter := r.IterCF(req.GetCf())
	defer iter.Close()

	iter.Seek(req.GetStartKey())

	limit := req.GetLimit()
	kvs := make([]*kvrpcpb.KvPair, 0, limit)
	var itemCount = uint32(0)
	for iter.Valid() && itemCount < limit {
		val, err := iter.Item().Value()
		if err != nil {
			return nil, fmt.Errorf("item value: %w", err)
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: val,
		})

		iter.Next()
		itemCount += 1
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
