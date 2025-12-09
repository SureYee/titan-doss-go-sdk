package doss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/titan/doss-go-sdk/internal"
)

var (
	defaultBlockSize int64 = 1 << 20
)

const (
	CodeSuccess = 0
)

type Client struct {
	region    string
	scheduler *scheduler
}

type Config struct {
}

func NewClient() *Client {
	scheduler := newScheduler("http://192.168.0.30:8888")
	return &Client{
		region:    "us-east-1",
		scheduler: scheduler,
	}
}

func (c *Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	conf, err := c.scheduler.getErasureConfig(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := c.scheduler.getDownloadNodes(c.region, *params.Bucket, *params.Key)

	if err != nil {
		return nil, err
	}
	errs := make([]error, len(nodes))
	readers := make([]io.Reader, len(nodes))
	for i, node := range nodes {
		go func(ctx context.Context, index int) {
			cli := c.getS3Client(node.NodeAddress)
			resp, err := cli.GetObject(ctx, params)
			if err != nil {
				errs[i] = err
				return
			}
			readers[i] = resp.Body
		}(ctx, i)
	}
	errCount := 0
	for _, e := range errs {
		if e != nil {
			errCount++
		}
	}
	if errCount > conf.DataShard {
		return nil, errors.New("download file error")
	}

	erasure, err := internal.NewErasure(ctx, conf.DataShard, conf.ParityShard, defaultBlockSize)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	err = erasure.Decode(ctx, w, readers)
	if err != nil {
		return nil, err
	}

	return &s3.GetObjectOutput{
		Body: r,
	}, nil
}

func (c *Client) getS3Client(endpoint string, optFns ...func(*s3.Options)) *s3.Client {
	//fixme use pool
	cli := s3.NewFromConfig(aws.Config{
		Region:       c.region,
		BaseEndpoint: &endpoint,
	}, optFns...)
	return cli
}

func (c *Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body := params.Body

	conf, err := c.scheduler.getErasureConfig(ctx)
	if err != nil {
		return nil, err
	}
	erasure, err := internal.NewErasure(ctx, conf.DataShard, conf.ParityShard, defaultBlockSize)
	if err != nil {
		return nil, err
	}

	size, ok := getBodySize(body)
	if !ok {
		if params.ContentLength == nil {
			return nil, errors.New("contentLength must be set when body can't get size")
		} else {
			size = *params.ContentLength
		}
	}

	nodes, err := c.scheduler.getUploadNodes(c.region, *params.Bucket, *params.Key, size)
	if err != nil {
		return nil, fmt.Errorf("get upload node:%w", err)
	}
	log.Printf("get nodes:%v", nodes)
	if len(nodes) == 0 || len(nodes) != conf.DataShard+conf.ParityShard {
		return nil, fmt.Errorf("node count not match")
	}

	errs := make([]error, len(nodes))
	writers := make([]io.Writer, len(nodes))
	shards := make([]shard, len(nodes))
	wg := sync.WaitGroup{}
	for i, n := range nodes {
		wg.Add(1)
		r, w := io.Pipe()
		writers[i] = w
		go func(ctx context.Context, index int, n node, r io.ReadCloser) {
			endpoint := fmt.Sprintf("http://%s:%d", "192.168.0.30", n.Port)
			defer func() {
				wg.Done()
				r.Close()
				shards[index] = shard{
					Index:       index + 1,
					Size:        0,
					Hash:        "",
					HashType:    "",
					NodeID:      n.ID,
					NodeAddress: endpoint,
				}
			}()
			newParams := clonePutObjectInputWithNewBody(params, r)
			out, err := manager.NewUploader(c.getS3Client(endpoint, optFns...)).Upload(ctx, &newParams)
			log.Println(out, err)
			if err != nil {
				errs[index] = err
				return
			}
		}(ctx, i, n, r)
	}

	_, err = erasure.Encode(ctx, body, writers)
	if err != nil {
		return nil, err
	}

	for _, w := range writers {
		_ = w.(io.Closer).Close()
	}
	wg.Wait()
	if err := c.scheduler.commitObject(ctx, commitObjectReq{
		Bucket:    *params.Bucket,
		Key:       *params.Key,
		Size:      uint64(size),
		Hash:      "",
		HashType:  "",
		ShardList: shards,
	}); err != nil {
		return nil, err
	}

	return &s3.PutObjectOutput{
		BucketKeyEnabled:        new(bool),
		ChecksumCRC32:           new(string),
		ChecksumCRC32C:          new(string),
		ChecksumCRC64NVME:       new(string),
		ChecksumSHA1:            new(string),
		ChecksumSHA256:          new(string),
		ChecksumType:            "",
		ETag:                    new(string),
		Expiration:              new(string),
		RequestCharged:          "",
		SSECustomerAlgorithm:    new(string),
		SSECustomerKeyMD5:       new(string),
		SSEKMSEncryptionContext: new(string),
		SSEKMSKeyId:             new(string),
		ServerSideEncryption:    "",
		Size:                    new(int64),
		VersionId:               new(string),
		ResultMetadata:          middleware.Metadata{},
	}, nil
}

func clonePutObjectInputWithNewBody(i *s3.PutObjectInput, body io.Reader) s3.PutObjectInput {
	return s3.PutObjectInput{
		Bucket:                    i.Bucket,
		Key:                       i.Key,
		ACL:                       i.ACL,
		Body:                      body,
		BucketKeyEnabled:          i.BucketKeyEnabled,
		CacheControl:              i.CacheControl,
		ChecksumAlgorithm:         i.ChecksumAlgorithm,
		ChecksumCRC32:             i.ChecksumCRC32,
		ChecksumCRC32C:            i.ChecksumCRC32C,
		ChecksumCRC64NVME:         i.ChecksumCRC64NVME,
		ChecksumSHA1:              i.ChecksumSHA1,
		ChecksumSHA256:            i.ChecksumSHA256,
		ContentDisposition:        i.ContentDisposition,
		ContentEncoding:           i.ContentEncoding,
		ContentLanguage:           i.ContentLanguage,
		ContentLength:             nil,
		ContentMD5:                nil,
		ContentType:               nil,
		ExpectedBucketOwner:       i.ExpectedBucketOwner,
		Expires:                   i.Expires,
		GrantFullControl:          i.GrantFullControl,
		GrantRead:                 i.GrantRead,
		GrantReadACP:              i.GrantReadACP,
		GrantWriteACP:             i.GrantWriteACP,
		IfMatch:                   i.IfMatch,
		IfNoneMatch:               i.IfNoneMatch,
		Metadata:                  i.Metadata,
		ObjectLockLegalHoldStatus: i.ObjectLockLegalHoldStatus,
		ObjectLockMode:            i.ObjectLockMode,
		ObjectLockRetainUntilDate: i.ObjectLockRetainUntilDate,
		RequestPayer:              i.RequestPayer,
		SSECustomerAlgorithm:      i.SSECustomerAlgorithm,
		SSECustomerKey:            i.SSECustomerKey,
		SSECustomerKeyMD5:         i.SSECustomerKeyMD5,
		SSEKMSEncryptionContext:   i.SSEKMSEncryptionContext,
		SSEKMSKeyId:               i.SSEKMSKeyId,
		ServerSideEncryption:      i.ServerSideEncryption,
		StorageClass:              i.StorageClass,
		Tagging:                   i.Tagging,
		WebsiteRedirectLocation:   i.WebsiteRedirectLocation,
		WriteOffsetBytes:          i.WriteOffsetBytes,
	}
}

func getBodySize(body io.Reader) (int64, bool) {
	switch v := body.(type) {
	case *bytes.Buffer:
		return int64(v.Len()), true
	case *bytes.Reader:
		return int64(v.Len()), true
	case *strings.Reader:
		return int64(v.Len()), true
	case *os.File:
		stat, err := v.Stat()
		if err != nil {
			return 0, false
		}
		return stat.Size(), true
	}

	return 0, false
}
