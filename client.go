package doss

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cbergoon/merkletree"
	"github.com/cenkalti/backoff/v4"
	"github.com/klauspost/reedsolomon"
	"github.com/titan/doss-go-sdk/internal/erasure"
	"github.com/titan/doss-go-sdk/internal/manager"
	"github.com/titan/doss-go-sdk/internal/scheduler"
	"golang.org/x/sync/errgroup"
)

var (
	defaultBlockSize int64 = 1 << 20
)

type Client struct {
	encoder   reedsolomon.Encoder
	region    string
	scheduler *scheduler.Scheduler
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
	scheduler := scheduler.NewScheduler(cfg.BaseEndpoint)
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

func (c *Client) UploadFile(ctx context.Context, bucket, key string, file *os.File) (any, error) {
	prehash, matched, err := c.preCheck(ctx, file, 256*1024)
	if err != nil {
		log.Printf("预检错误：%s", err)
		return nil, err
	}
	tree, err := c.buildMerkleTree(file, 5*1024*1024)
	if err != nil {
		return nil, err
	}
	log.Printf("pre check:%v", matched)
	if matched {
		log.Printf("预检匹配成功")
		obj, matched, err := c.hashCheck(ctx, bucket, key, tree)
		if err != nil {
			log.Printf("Hash检测错误：%s", err)
			return nil, err
		}
		if matched {
			log.Printf("Hash检测匹配成功")
			return obj, nil
		}
	}
	// 开始上传文件
	log.Println("开始上传文件...")
	_, err = c.upload(ctx, bucket, key, file, tree, prehash)

	return nil, err
}

// hashContent is a wrapper for a byte slice that satisfies the merkletree.Content interface.
type hashContent struct {
	hash []byte
}

// CalculateHash hashes the data using sha256.
func (c hashContent) CalculateHash() ([]byte, error) {
	return c.hash, nil
}

// Equals tests for equality of two Contents.
func (c hashContent) Equals(other merkletree.Content) (bool, error) {
	otherC, ok := other.(hashContent)
	if !ok {
		return false, errors.New("invalid content type")
	}
	return bytes.Equal(c.hash, otherC.hash), nil
}

func (c *Client) upload(ctx context.Context, bucket string, key string, file *os.File, merkleTree *merkletree.MerkleTree, prehash string) (any, error) {
	// 获取节点列表
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := stat.Size()

	filehash, err := hashFile(file)
	if err != nil {
		return nil, err
	}
	resp, err := c.scheduler.GetUploadNodes("", "", filehash, filesize)
	if err != nil {
		log.Printf("获取上传节点列表失败:%s", err)
		return nil, err
	}

	nodes := resp.List
	conf := resp.Config
	log.Printf("获取到纠删码配置：%#v", conf)
	uploader := manager.NewUploader()
	if !conf.EnableMultipart {
		var shardNumber int64 = 1
		if conf.EnableErasure {
			shardNumber = conf.ErasureDataShard + conf.ErasureParityShard
		}
		uploadNodes := nodes[:shardNumber]
		// backupNodes := nodes[shardNumber:] // todo use backup nodes to increase reliability

		writers := make([]io.Writer, shardNumber)
		shards := make([]scheduler.Shard, shardNumber)

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
				endpoint := node.Presigned.Url
				log.Printf("开始向节点%s上传分片", endpoint)
				uploader := manager.NewUploader()

				// The operation to perform, wrapped in a function.
				operation := func() error {
					// If context is canceled, stop trying.
					if egCtx.Err() != nil {
						return backoff.Permanent(egCtx.Err())
					}
					_, err := uploader.UploadFile(egCtx, &v4.PresignedHTTPRequest{
						URL:          node.Presigned.Url,
						Method:       node.Presigned.Method,
						SignedHeader: node.Presigned.Headers,
					}, teeReader, filesize)
					return err
				}

				// Define the backoff strategy.
				bo := backoff.NewExponentialBackOff()
				bo.MaxElapsedTime = c.cfg.MaxRetryElapsedTime
				shards[index] = scheduler.Shard{
					Index:       index + 1,
					Status:      scheduler.StatusFailed,
					NodeID:      node.ID,
					NodeAddress: endpoint,
					// Bucket:      node.Bucket,
					// Key:         node.Key,
				}
				err := backoff.Retry(operation, backoff.WithContext(bo, egCtx))

				if err != nil {
					log.Printf("upload error:%s", err.Error())
					shards[index].Message = err.Error()
					// log.Printf("shard %d upload failed after retries: %v", index+1, err)
					mu.Lock()
					failedUploads++
					// Check if we have exceeded the fault tolerance
					if failedUploads > int(conf.ErasureParityShard) {
						mu.Unlock()
						return fmt.Errorf("too many shards failed to upload, failed: %d, parity: %d", failedUploads, conf.ErasureParityShard)
					}
					mu.Unlock()
					return nil // Don't return the upload error itself, but allow the group to continue.
				}

				// log.Println(out)
				hash := hex.EncodeToString(hasher.Sum(nil))
				shards[index].Size = 0
				shards[index].Hash = hash
				shards[index].HashType = "md5"
				shards[index].Status = scheduler.StatusSuccess
				log.Printf("向节点%s上传分片完成", endpoint)
				return nil
			})
		}

		eg.Go(func() error {
			defer func() {
				for _, w := range writers {
					if c, ok := w.(io.Closer); ok {
						c.Close()
					}
				}
			}()
			if conf.EnableErasure {
				erasure, err := erasure.NewErasure(ctx, int(conf.ErasureDataShard), int(conf.ErasureParityShard), defaultBlockSize)
				if err != nil {
					return err
				}
				_, err = erasure.Encode(egCtx, file, writers)
				return err
			} else {
				_, err := io.Copy(io.MultiWriter(writers...), file)
				return err
			}
		})

		// After all operations, check the final state.
		// Note: eg.Wait() will return the "too many shards failed" error if it was triggered.
		if err := eg.Wait(); err != nil {
			log.Printf("eg wait error:%s", err)
			return nil, err
		}
		log.Println("文件上传成功")

		if failedUploads > int(conf.ErasureParityShard) {
			return nil, fmt.Errorf("not enough shards uploaded successfully, required: %d, got: %d", conf.ErasureDataShard, shardNumber-int64(failedUploads))
		}

		if err := c.scheduler.CommitObject(ctx, scheduler.CommitObjectReq{
			MerkleHash: hex.EncodeToString(merkleTree.MerkleRoot()),
			Config: scheduler.CommitConfig{
				EnableMultipart:     conf.EnableMultipart,
				MultipartChunkCount: conf.MultipartChunkCount,
			},
			Bucket:    bucket,
			Key:       key,
			Hash:      filehash,
			HashType:  "sha256",
			ShardList: shards,
			Size:      uint64(filesize),
			PreHash:   prehash,
			PreSize:   256 * 1024,
			UploadId:  "",
			ObjectId:  "",
		}); err != nil {
			log.Printf("commit object error:%s", err)
			return nil, err
		}
	} else {
		presignParts := make([]*v4.PresignedHTTPRequest, 0, len(nodes))
		var nodeId string
		for _, node := range nodes {
			if nodeId == "" {
				nodeId = node.ID
			}
			presignParts = append(presignParts, &v4.PresignedHTTPRequest{
				URL:          node.Presigned.Url,
				Method:       node.Presigned.Method,
				SignedHeader: node.Presigned.Headers,
			})
		}
		resps, err := uploader.UploadPart(ctx, presignParts, file, filesize, conf.MultipartChunkSize)
		if err != nil {
			return nil, err
		}

		shards := make([]scheduler.Shard, 0, len(resps))
		var leafs []merkletree.Content

		for _, resp := range resps {
			h, err := hex.DecodeString(resp.Hash)
			if err != nil {
				return nil, err
			}
			leafs = append(leafs, hashContent{
				hash: h,
			})
			log.Printf("Part %d ====> %s", resp.PartNumber, resp.Hash)
			shards = append(shards, scheduler.Shard{
				Index:       resp.PartNumber,
				Status:      scheduler.StatusSuccess,
				Size:        uint64(resp.Size),
				Hash:        resp.Hash,
				HashType:    resp.HashType,
				NodeID:      nodeId,
				Message:     "",
				Bucket:      bucket,
				Key:         key,
				NodeAddress: "",
				Etag:        resp.Etag,
			})
		}
		// 创建 Merkle Tree
		tree, err := merkletree.NewTree(leafs)
		if err != nil {
			return nil, err
		}

		// 计算根哈希
		rootHash := hex.EncodeToString(tree.MerkleRoot())
		log.Printf("MerkleRoot ====> %s", rootHash)
		if err := c.scheduler.CommitObject(ctx, scheduler.CommitObjectReq{
			MerkleHash: rootHash,
			Config: scheduler.CommitConfig{
				EnableMultipart:     conf.EnableMultipart,
				MultipartChunkCount: conf.MultipartChunkCount,
			},
			Bucket: bucket,
			Key:    key,
			// Size:      uint64(filesize),
			Hash:      filehash,
			HashType:  "sha256",
			ShardList: shards,
			Size:      uint64(filesize),
			PreHash:   prehash,
			PreSize:   256 * 1024,
			UploadId:  resp.UploadId,
			ObjectId:  resp.ObjectId,
		}); err != nil {
			log.Printf("commit object error:%s", err)
			return nil, err
		}
	}

	return &s3.PutObjectOutput{
		ETag: aws.String(""),
		// VersionId is not provided by the current scheduler commit response
	}, nil
}

func (c *Client) buildMerkleTree(file *os.File, size int64) (*merkletree.MerkleTree, error) {
	defer file.Seek(0, io.SeekStart)
	chunk := make([]byte, size)
	var (
		leafs []merkletree.Content
	)

	// 分片读取文件，计算每个分片的哈希
	for i := int64(0); ; i++ {
		// 每次读取一个分片
		// 最后一个分片不足defaultBlockSize
		n, err := file.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// 计算分片哈希
		// chunk[:n] 是为了防止最后一个分片不足defaultBlockSize
		hash := sha256.Sum256(chunk[:n])
		h := make([]byte, sha256.Size)
		copy(h, hash[:])
		leafs = append(leafs, hashContent{hash: h})
	}

	// 创建 Merkle Tree
	return merkletree.NewTree(leafs)
}

func (c *Client) hashCheck(ctx context.Context, bucket, key string, merkletree *merkletree.MerkleTree) (obj any, match bool, err error) {
	var (
		leafHashs []scheduler.LeafHash
	)

	// 分片读取文件，计算每个分片的哈希
	for i, leaf := range merkletree.Leafs {
		leafHashs = append(leafHashs, scheduler.LeafHash{
			Hash:  hex.EncodeToString(leaf.Hash),
			Index: int64(i) + 1,
		})
	}

	// 计算根哈希
	rootHash := hex.EncodeToString(merkletree.MerkleRoot())
	log.Printf("开始验证merkle hash:%s", rootHash)
	obj, match, err = c.scheduler.HashCheck(ctx, scheduler.HashCheckReq{
		Hash:     rootHash,
		Bucket:   bucket,
		Key:      key,
		LeafHash: leafHashs,
	})
	// 调用调度器进行深度验证
	return obj, match, err
}

// preCheck
// hash预检
func (c *Client) preCheck(ctx context.Context, file *os.File, size int64) (stubHash string, match bool, err error) {
	// 重置文件指针, 读取文件结束后，需要将文件指针归零，否则会影响后续文件操作
	defer file.Seek(0, io.SeekStart)

	// 计算文件头部256KB的哈希
	hasher := sha256.New()
	// 读取文件的前256KB
	if _, err := io.CopyN(hasher, file, size); err != nil && err != io.EOF {
		return "", false, err
	}
	stubHash = hex.EncodeToString(hasher.Sum(nil))
	log.Printf("开始进行预检，hash:%s", stubHash)
	// 调用调度器进行预检
	match, err = c.scheduler.PreCheck(ctx, scheduler.PreCheckReq{
		Size:     size,
		Hash:     stubHash,
		HashType: "sha256",
	})
	return
}

func (c *Client) processAndUploadChunk(ctx context.Context, data []byte, chunkHash string, nodeAddrs []string) error {
	// 1. 纠删码切片 (注意：此处数据已在内存中)
	// Split 会将 data 划分为 K 个分片，并预留 M 个空位
	shards, err := c.encoder.Split(data)
	if err != nil {
		return err
	}

	// 2. 计算校验位 (填充 shards 中的后 M 个分片)
	if err := c.encoder.Encode(shards); err != nil {
		return err
	}

	// 3. 并发上传 K+M 个分片
	var wg sync.WaitGroup
	errChan := make(chan error, len(shards))

	for i := 0; i < len(shards); i++ {
		wg.Add(1)
		go func(idx int, shardData []byte) {
			defer wg.Done()

			// 计算分片特定的 Hash (用于节点校验)
			shardHash := calculateHash(shardData)

			// 模拟发送到具体的存储节点
			// nodeAddrs[idx] 是调度器分配给该分片的节点
			if err := c.uploadToNode(ctx, nodeAddrs[idx], shardHash, shardData); err != nil {
				errChan <- err
			}
		}(i, shards[i])
	}

	wg.Wait()
	close(errChan)

	// 检查是否有分片上传失败
	if len(errChan) > 0 {
		return <-errChan
	}
	return nil
}

func (c *Client) uploadToNode(ctx context.Context, nodeAddr, shardHash string, data []byte) error {

	return nil
}

func calculateHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hashFile(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	hash := hex.EncodeToString(h.Sum(nil))
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hash, nil
}
