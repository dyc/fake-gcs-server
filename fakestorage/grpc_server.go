package fakestorage

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/fsouza/fake-gcs-server/internal/backend"
	pb "google.golang.org/genproto/googleapis/storage/v2"
	"google.golang.org/grpc"
)

type StorageServer struct {
	pb.UnimplementedStorageServer
	backend     backend.Storage
	url         string
	externalURL string
}

func (s *StorageServer) URL() string {
	if s.externalURL != "" {
		return s.externalURL
	}
	if s.url != "" {
		return s.url
	}
	return ""
}

func (s *StorageServer) CreateBucketWithOpts(opts CreateBucketOpts) {
	err := s.backend.CreateBucket(opts.Name, opts.VersioningEnabled)
	if err != nil {
		panic(err)
	}
}

func (s *StorageServer) ListBuckets(context.Context, *pb.ListBucketsRequest) (*pb.ListBucketsResponse, error) {
	buckets, err := s.backend.ListBuckets()
	if err != nil {
		return nil, err
	}

	resp := &pb.ListBucketsResponse{
		Buckets: make([]*pb.Bucket, len(buckets)),
	}
	for i, bucket := range buckets {
		resp.Buckets[i] = &pb.Bucket{
			Name: bucket.Name,
		}
	}

	return resp, nil
}

func (s *StorageServer) ListObjects(_ctx context.Context, req *pb.ListObjectsRequest) (*pb.ListObjectsResponse, error) {
	objects, err := s.backend.ListObjects(req.Parent, req.Prefix, req.Versions)
	if err != nil {
		return nil, err
	}

	resp := &pb.ListObjectsResponse{
		Objects: make([]*pb.Object, len(objects)),
	}
	for i, object := range objects {
		resp.Objects[i] = &pb.Object{
			Name: object.Name,
		}
	}

	return resp, nil
}

func NewStorageServer(options Options) (*StorageServer, error) {
	backendObjects := toBackendObjects(options.InitialObjects)
	var backendStorage backend.Storage
	var err error
	if options.StorageRoot != "" {
		backendStorage, err = backend.NewStorageFS(backendObjects, options.StorageRoot)
	} else {
		backendStorage = backend.NewStorageMemory(backendObjects)
	}
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", options.Host, options.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(make([]grpc.ServerOption, 0)...)
	s := &StorageServer{
		backend:     backendStorage,
		url:         addr,
		externalURL: options.ExternalURL,
	}
	pb.RegisterStorageServer(grpcServer, s)
	grpcServer.Serve(lis)
	return s, nil
}
