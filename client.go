package doss

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"
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
	cfg       Config
}

type Config struct {
	SchedulerURL        string
	Region              string
	MaxRetryElapsedTime time.Duration
}

func NewClient(cfg Config) *Client {
	if cfg.SchedulerURL == "" {
		// Or return an error, for now, let's panic
		panic("SchedulerURL must be set")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.MaxRetryElapsedTime == 0 {
		cfg.MaxRetryElapsedTime = time.Second
	}
	scheduler := newScheduler(cfg.SchedulerURL)
	return &Client{
		region:    cfg.Region,
		scheduler: scheduler,
		cfg:       cfg,
	}
}

func (c *Client) getS3Client(endpoint string, optFns ...func(*s3.Options)) *s3.Client {
	cli := s3.NewFromConfig(aws.Config{
		Region:       c.region,
		BaseEndpoint: &endpoint,
	}, optFns...)
	return cli
}

func (c *Client) downloadWithoutEncode() {

}

func (c *Client) downloadWithEncode() {

}

func (c *Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	resp, err := c.scheduler.getDownloadNodes(c.region, *params.Bucket, *params.Key)
	if err != nil {
		return nil, err
	}

	nodes := resp.Shards
	conf := resp.Config

	if !conf.Encode {
		var err error
		var resp *s3.GetObjectOutput
		for _, node := range nodes {
			cli := c.getS3Client(node.NodeAddress)
			resp, err = cli.GetObject(ctx, params)
			if err != nil {
				continue
			}
		}
		return resp, err
	}
	readers := make([]io.Reader, len(nodes))
	eg, egCtx := errgroup.WithContext(ctx)

	// We still need to count successful downloads to see if we have enough shards.
	var successfulDownloads int
	var mu sync.Mutex

	for i, node := range nodes {
		index := i
		node := node
		eg.Go(func() error {
			cli := c.getS3Client(node.NodeAddress)
			resp, err := cli.GetObject(egCtx, params)
			if err != nil {
				// Don't return error here, just log it or mark this reader as nil.
				// errgroup would cancel all other downloads.
				log.Printf("failed to download shard %d from %s: %v", index, node.NodeAddress, err)
				readers[index] = nil // Mark as failed
				return nil
			}

			mu.Lock()
			readers[index] = resp.Body
			successfulDownloads++
			mu.Unlock()
			return nil
		})
	}

	// Wait for all downloads to complete or one to fail.
	// Since we return nil in goroutines, this will wait for all to finish.
	if err := eg.Wait(); err != nil {
		// This part might be unreachable if goroutines always return nil.
		// But it's good practice.
		for _, r := range readers {
			if r, ok := r.(io.Closer); ok && r != nil {
				r.Close()
			}
		}
		return nil, err
	}

	// If too many shards failed, return error.
	if successfulDownloads < conf.DataShard {
		// Close any readers that were successfully opened.
		for _, r := range readers {
			if r, ok := r.(io.Closer); ok && r != nil {
				r.Close()
			}
		}
		return nil, fmt.Errorf("too many shards failed to download, required: %d, available: %d", conf.DataShard, successfulDownloads)
	}

	erasure, err := internal.NewErasure(ctx, conf.DataShard, conf.ParityShard, defaultBlockSize)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()

	go func() {
		// Close all readers and the writer when the goroutine finishes.
		defer func() {
			for _, r := range readers {
				if r != nil {
					r.(io.Closer).Close()
				}
			}
			w.Close()
		}()

		err := erasure.Decode(ctx, w, readers)
		if err != nil {
			// Propagate the error to the reader side.
			w.CloseWithError(err)
		}
	}()

	return &s3.GetObjectOutput{
		Body: r,
	}, nil
}

func (c *Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body := params.Body

	size, ok := getBodySize(body)
	if !ok {
		if params.ContentLength == nil {
			return nil, errors.New("contentLength must be set when body can't get size")
		} else {
			size = *params.ContentLength
		}
	}

	resp, err := c.scheduler.getUploadNodes(c.region, *params.Bucket, *params.Key, size)
	if err != nil {
		return nil, fmt.Errorf("get upload node:%w", err)
	}

	nodes := resp.Shards
	conf := resp.Config

	shardNumber := conf.DataShard + conf.ParityShard

	if len(nodes) == 0 {
		return nil, errors.New("insufficient number of nodes")
	}

	uploadNodes := nodes[:shardNumber]
	backupNodes := nodes[shardNumber:]
	fmt.Println(uploadNodes, backupNodes)

	writers := make([]io.Writer, shardNumber)
	shards := make([]shard, shardNumber)

	var failedUploads int
	var mu sync.Mutex

	eg, egCtx := errgroup.WithContext(ctx)

	for i, n := range uploadNodes {
		r, w := io.Pipe()
		writers[i] = w

		index := i
		node := n

		eg.Go(func() error {
			reader := r
			hasher := md5.New()
			teeReader := io.TeeReader(reader, hasher)
			defer reader.Close()
			endpoint := fmt.Sprintf("http://%s:%d", node.Host, node.Port)
			newParams := clonePutObjectInputWithNewBody(params, teeReader)
			uploader := manager.NewUploader(c.getS3Client(endpoint, optFns...))

			var out *manager.UploadOutput

			// The operation to perform, wrapped in a function.
			operation := func() error {
				// If context is canceled, stop trying.
				if egCtx.Err() != nil {
					return backoff.Permanent(egCtx.Err())
				}
				var err error
				out, err = uploader.Upload(egCtx, &newParams)
				return err
			}

			// Define the backoff strategy.
			bo := backoff.NewExponentialBackOff()
			bo.MaxElapsedTime = c.cfg.MaxRetryElapsedTime
			shards[index] = shard{
				Index:       index + 1,
				Size:        0,
				Hash:        "",
				HashType:    "",
				NodeID:      node.ID,
				NodeAddress: endpoint,
			}
			err := backoff.Retry(operation, backoff.WithContext(bo, egCtx))

			if err != nil {
				log.Printf("shard %d upload failed after retries: %v", index+1, err)
				mu.Lock()
				failedUploads++
				// Check if we have exceeded the fault tolerance
				if failedUploads > conf.ParityShard {
					mu.Unlock()
					return fmt.Errorf("too many shards failed to upload, failed: %d, parity: %d", failedUploads, conf.ParityShard)
				}
				mu.Unlock()
				return nil // Don't return the upload error itself, but allow the group to continue.
			}

			log.Println(out)
			hash := hex.EncodeToString(hasher.Sum(nil))
			shards[index].Size = 0
			shards[index].Hash = hash
			shards[index].HashType = "md5"
			return nil
		})
	}

	hasher := md5.New()
	teeReader := io.TeeReader(body, hasher)

	eg.Go(func() error {
		defer func() {
			for _, w := range writers {
				if c, ok := w.(io.Closer); ok {
					c.Close()
				}
			}
		}()
		if conf.Encode {
			erasure, err := internal.NewErasure(ctx, conf.DataShard, conf.ParityShard, defaultBlockSize)
			if err != nil {
				return err
			}
			_, err = erasure.Encode(egCtx, teeReader, writers)
			return err
		} else {
			_, err := io.Copy(io.MultiWriter(writers...), teeReader)
			return err
		}
	})

	// After all operations, check the final state.
	// Note: eg.Wait() will return the "too many shards failed" error if it was triggered.
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if failedUploads > conf.ParityShard {
		return nil, fmt.Errorf("not enough shards uploaded successfully, required: %d, got: %d", conf.DataShard, shardNumber-failedUploads)
	}
	hash := hex.EncodeToString(hasher.Sum(nil))

	if err := c.scheduler.commitObject(ctx, commitObjectReq{
		Config: erasureConfig{
			Encode:      conf.Encode,
			DataShard:   conf.DataShard,
			ParityShard: conf.ParityShard,
		},
		Bucket:    *params.Bucket,
		Key:       *params.Key,
		Size:      uint64(size),
		Hash:      hash,
		HashType:  "md5",
		ShardList: shards,
	}); err != nil {
		return nil, err
	}

	return &s3.PutObjectOutput{
		ETag: &hash,
		// VersionId is not provided by the current scheduler commit response
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
