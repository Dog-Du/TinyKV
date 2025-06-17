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
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 从 reader 中获取 key 对应的 value
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil,
			Error:       err.Error(),
			Value:       nil,
			NotFound:    true,
		}, nil // 错误存储在 Raw**Response 的 Error 字段中， 而不是 error 中。
	}

	return &kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       value,
		NotFound:    value == nil,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	// 使用 Storage.Modify 来存储数据， Modify是一个范型接口，可以存储 Put 和 Delete 操作。
	// 使用 Write 方法来将数据写入到 storage 中。
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	})

	if err != nil {
		return &kvrpcpb.RawPutResponse{
			RegionError: nil,
			Error:       err.Error(),
		}, nil
	}

	return &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	// 与RawPut差不多一样，如法炮制
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	})

	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			RegionError: nil,
			Error:       err.Error(),
		}, nil
	}

	return &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	// 使用 Storage.Reader 来获取一个 reader 实例，然后使用 IterCF 来遍历数据。
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}

	defer reader.Close()

	iter := reader.IterCF(req.GetCf()) // 获取一个迭代器，用于遍历数据
	defer iter.Close()                 // iter.close 一定要在 reader.close 之前，所以 defer 的顺序是相反的，不能调换

	// 从 start key 开始扫描，如果 start key 为空，则从第一个 key 开始扫描
	iter.Seek(req.GetStartKey())

	raw_scan_response := &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         nil,
	} // 返回的结果，初始化为空

	limit := req.GetLimit() // limit

	for iter.Valid() && limit > 0 {
		limit--

		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}

		raw_scan_response.Kvs = append(raw_scan_response.Kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})

		iter.Next()
	}
	return raw_scan_response, nil
}
