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
	"github.com/dustin/go-humanize"
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
	BaseEndpoint        string
	Region              string
	MaxRetryElapsedTime time.Duration
}

func NewClient(cfg Config) *Client {
	if cfg.BaseEndpoint == "" {
		// Or return an error, for now, let's panic
		panic("SchedulerURL must be set")
	}
	if cfg.MaxRetryElapsedTime == 0 {
		cfg.MaxRetryElapsedTime = time.Second
	}
	scheduler := newScheduler(cfg.BaseEndpoint)
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

type result struct {
	idx    int
	reader io.ReadCloser
	err    error
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
			params.Bucket = &node.Bucket
			params.Key = &node.Key
			resp, err = cli.GetObject(ctx, params)
			if err == nil {
				return resp, err
			}
		}
		return resp, err
	}
	readers := make([]io.Reader, len(nodes))
	// Use a WaitGroup to wait for all goroutines to finish.
	resultCh := make(chan result, len(nodes))
	for i, node := range nodes {
		go func(index int, node shard) {
			log.Printf("开始从%s下载文件", node.NodeAddress)
			cli := c.getS3Client(node.NodeAddress)
			newParams := *params
			newParams.Bucket = aws.String(node.Bucket)
			newParams.Key = aws.String(node.Key)
			resp, err := cli.GetObject(ctx, &newParams)
			log.Printf("从%s下载文件完成", node.NodeAddress)
			if err != nil {
				resultCh <- result{idx: index, err: err}
				return
			}
			resultCh <- result{idx: index, reader: ProgressReader(resp.Body, time.Now())}
		}(i, node)
	}

	errs := make([]error, len(nodes))
	for range nodes {
		res := <-resultCh
		if res.err != nil {
			errs[res.idx] = res.err
			// Also close any readers that might have been opened before the error
			if res.reader != nil {
				res.reader.Close()
			}
			continue
		}
		readers[res.idx] = res.reader
	}

	errCount := 0
	for _, e := range errs {
		if e != nil {
			errCount++
		}
	}

	// If too many shards failed, return error.
	if len(nodes)-errCount < conf.DataShard {
		return nil, fmt.Errorf("too many shards failed to download, required: %d, available: %d", conf.DataShard, len(nodes)-errCount)
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
		Body:          r,
		ContentLength: &resp.Object.Size,
	}, nil
}

func (c *Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	log.Println("开始上传文件")
	body := params.Body

	size, ok := getBodySize(body)
	if !ok {
		if params.ContentLength == nil {
			return nil, errors.New("contentLength must be set when body can't get size")
		} else {
			size = *params.ContentLength
		}
	}
	log.Printf("获取到文件大小：%s", humanize.Bytes(uint64(size)))
	resp, err := c.scheduler.getUploadNodes(c.region, *params.Bucket, *params.Key, size)
	if err != nil {
		return nil, fmt.Errorf("get upload node:%w", err)
	}

	nodes := resp.Shards
	conf := resp.Config
	log.Printf("获取到上传节点数据：%#v", nodes)
	log.Printf("获取到纠删码配置：%#v", conf)
	conf.DataShard = 1
	conf.ParityShard = 0
	conf.Encode = false
	shardNumber := conf.DataShard + conf.ParityShard

	if len(nodes) == 0 {
		return nil, errors.New("insufficient number of nodes")
	}

	uploadNodes := nodes[:shardNumber]
	// backupNodes := nodes[shardNumber:] // todo use backup nodes to increase reliability

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
			log.Printf("开始向节点%s上传分片", endpoint)
			newParams := *params
			newParams.Body = teeReader
			newParams.Bucket = aws.String(node.Bucket)
			newParams.Key = aws.String(node.Key)
			uploader := manager.NewUploader(c.getS3Client(endpoint, optFns...))

			// var out *manager.UploadOutput

			// The operation to perform, wrapped in a function.
			operation := func() error {
				// If context is canceled, stop trying.
				if egCtx.Err() != nil {
					return backoff.Permanent(egCtx.Err())
				}
				var err error
				_, err = uploader.Upload(egCtx, &newParams)
				return err
			}

			// Define the backoff strategy.
			bo := backoff.NewExponentialBackOff()
			bo.MaxElapsedTime = c.cfg.MaxRetryElapsedTime
			shards[index] = shard{
				Index:       index + 1,
				Status:      StatusFailed,
				NodeID:      node.ID,
				NodeAddress: endpoint,
				Bucket:      node.Bucket,
				Key:         node.Key,
			}
			err := backoff.Retry(operation, backoff.WithContext(bo, egCtx))

			if err != nil {
				shards[index].Message = err.Error()
				// log.Printf("shard %d upload failed after retries: %v", index+1, err)
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

			// log.Println(out)
			hash := hex.EncodeToString(hasher.Sum(nil))
			shards[index].Size = 0
			shards[index].Hash = hash
			shards[index].HashType = "md5"
			shards[index].Status = StatusSuccess
			log.Printf("向节点%s上传分片完成", endpoint)
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
