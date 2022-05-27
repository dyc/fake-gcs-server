package fakestorage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/fsouza/fake-gcs-server/internal/backend"
	"github.com/fsouza/fake-gcs-server/internal/checksum"
	pb "google.golang.org/genproto/googleapis/storage/v2"
	"google.golang.org/grpc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var errUnsupportedWriteOperation = errors.New("unsupported write operation")

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

func (s *StorageServer) WriteObject(writeServer pb.Storage_WriteObjectServer) error {
	req, err := writeServer.Recv()
	data := req.GetChecksummedData()
	if data == nil {
		return errUnsupportedWriteOperation
	}
	spec := req.GetWriteObjectSpec()
	if spec == nil {
		return errUnsupportedWriteOperation
	}
	md5Hash := checksum.EncodedMd5Hash(data.Content)
	reqObj := backend.Object{
		ObjectAttrs: backend.ObjectAttrs{
			BucketName:      spec.Resource.Bucket,
			Name:            spec.Resource.Name,
			ContentType:     spec.Resource.ContentType,
			ContentEncoding: spec.Resource.ContentEncoding,
			Md5Hash:         md5Hash,
			Etag:            fmt.Sprintf("%q", md5Hash),
			ACL:             getObjectACL(spec.PredefinedAcl),
		},
		Content: data.Content,
	}
	if data.Crc32C != nil {
		reqObj.Crc32c = fmt.Sprint(*data.Crc32C)
	}
	backendObj, err := s.backend.CreateObject(reqObj)
	if err != nil {
		return err
	}

	created := fromBackendObjects([]backend.Object{backendObj})[0]
	maybeCrc32c, err := strconv.ParseUint(created.Crc32c, 10, 32)
	if err != nil {
		return err
	}
	crc32c := uint32(maybeCrc32c)
	respAcls := make([]*pb.ObjectAccessControl, len(created.ACL))
	for i, acl := range created.ACL {
		respAcls[i] = &pb.ObjectAccessControl{
			Role:   string(acl.Role),
			Id:     acl.EntityID,
			Entity: string(acl.Entity),
			Email:  acl.Email,
			Domain: acl.Domain,
		}
		if acl.ProjectTeam != nil {
			respAcls[i].ProjectTeam = &pb.ProjectTeam{
				ProjectNumber: acl.ProjectTeam.ProjectNumber,
				Team:          acl.ProjectTeam.Team,
			}
		}
	}
	return writeServer.SendAndClose(&pb.WriteObjectResponse{
		WriteStatus: &pb.WriteObjectResponse_Resource{
			Resource: &pb.Object{
				Name:            created.Name,
				Bucket:          created.BucketName,
				Size:            int64(len(created.Content)),
				Generation:      created.Generation,
				ContentType:     created.ContentType,
				ContentEncoding: created.ContentEncoding,
				CreateTime:      timestamppb.New(created.Created),
				DeleteTime:      timestamppb.New(created.Deleted),
				UpdateTime:      timestamppb.New(created.Updated),
				Metadata:        created.Metadata,
				Checksums: &pb.ObjectChecksums{
					Crc32C:  &crc32c,
					Md5Hash: []byte(created.Md5Hash),
				},
				Acl: respAcls,
			},
		},
	})
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
