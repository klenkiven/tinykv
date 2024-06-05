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
	reader, err := server.storage.Reader(req.Context)
	value, err := reader.GetCF(req.Cf, req.Key)

	return &kvrpcpb.RawGetResponse{
		Error:    err.Error(),
		Value:    value,
		NotFound: value == nil,
	}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := []storage.Modify{
		storage.Modify{
			Data: &storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	if err := server.storage.Write(req.Context, batch); err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}

	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := []storage.Modify{
		storage.Modify{
			Data: &storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	if err := server.storage.Write(req.Context, batch); err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}

	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	iterCf := reader.IterCF(req.Cf)

	entries := []*kvrpcpb.KvPair{}
	for iterCf.Valid() {
		dbItem := iterCf.Item()
		itemKey := dbItem.Key()
		itemValue, _ := dbItem.Value()
		entries = append(entries, &kvrpcpb.KvPair{
			Key:   itemKey,
			Value: itemValue,
		})
	}

	return &kvrpcpb.RawScanResponse{
		Error: err.Error(),
		Kvs:   entries,
	}, err
}
