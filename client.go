package doss

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/cbergoon/merkletree"
	"github.com/cenkalti/backoff/v4"
	"github.com/sureyee/titan-doss-go-sdk/api"
	"github.com/sureyee/titan-doss-go-sdk/internal/erasure"
	"github.com/sureyee/titan-doss-go-sdk/internal/manager"
	"github.com/sureyee/titan-doss-go-sdk/log"
	"golang.org/x/sync/errgroup"
)

var (
	defaultBlockSize int64 = 1 << 20
)

type Client struct {
	api *api.ApiClient
	cfg *Config
}

type Config struct {
	BaseEndpoint        string
	AccessKey           string
	SecretKey           string
	MaxRetryElapsedTime time.Duration
}

type Option func(*Client)

func WithDebug(logger ...log.Logger) Option {
	return func(c *Client) {
		if len(logger) == 0 {
			log.SetLogger(log.NewLogger())
		} else {
			log.SetLogger(logger[0])
		}
	}
}

func NewClient(cfg *Config) (*Client, error) {
	if cfg.BaseEndpoint == "" {
		return nil, errors.New("apiURL must be set")
	}
	if cfg.MaxRetryElapsedTime == 0 {
		cfg.MaxRetryElapsedTime = time.Second
	}
	api := api.NewApi(cfg.BaseEndpoint, cfg.AccessKey, cfg.SecretKey)
	return &Client{
		api: api,
		cfg: cfg,
	}, nil
}

type OptionFunc api.OptionFunc

type progressWriter struct {
	w        io.Writer
	progress api.ProgressFunc
	loaded   *atomic.Int64
	total    int64
}

func (pw *progressWriter) Write(p []byte) (n int, err error) {
	n, err = pw.w.Write(p)
	if n > 0 && pw.progress != nil {
		newLoaded := min(pw.loaded.Add(int64(n)),
			pw.total)
		pw.progress(newLoaded, pw.total)
	}
	return n, err
}

type progressReader struct {
	r        io.Reader
	progress api.ProgressFunc
	loaded   *atomic.Int64
	total    int64
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if n > 0 && pr.progress != nil {
		newLoaded := min(pr.loaded.Add(int64(n)), pr.total)
		pr.progress(newLoaded, pr.total)
	}
	return n, err
}

func (c *Client) createUpload(ctx context.Context, folderId int64, filename, hash string, size uint64, opts ...api.OptionFunc) (string, error) {
	resp, err := c.api.CreateUpload(ctx, &api.CreateUploadReq{
		Folder:      folderId,
		Filename:    filename,
		ContentType: "",
		Hash:        hash,
		Size:        size,
	}, opts...)
	if err != nil {
		return "", err
	}
	return resp.SessionID, nil
}

func (c *Client) UploadFile(ctx context.Context, folderId int64, file multipart.File, filename string, filesize int64, opts ...api.OptionFunc) (any, error) {
	log.Debug("preparing to upload")
	// 优化：同时计算 Merkle Tree 和文件 Hash，避免重复读取
	tree, fileHash, err := c.buildMerkleTreeWithHash(file, 5*1024*1024)
	if err != nil {
		return nil, err
	}
	log.Debugf("build merkle tree and hash file complete")
	sessionId, err := c.createUpload(ctx, folderId, filepath.Base(filename), fileHash, uint64(filesize), opts...)
	if err != nil {
		return nil, err
	}
	log.Debugf("create upload session success:%s", sessionId)
	obj, matched, err := c.hashCheck(ctx, sessionId, tree, opts...)
	if err != nil {
		return nil, err
	}
	if matched {
		log.Debug("hash checked match")
		return obj, nil
	}
	// 开始上传文件
	log.Debug("upload start")
	_, err = c.upload(ctx, sessionId, file, fileHash, filesize, opts...)
	return nil, err
}

func (c *Client) Upload(ctx context.Context, folderId int64, filename string, opts ...api.OptionFunc) (any, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := stat.Size()
	return c.UploadFile(ctx, folderId, file, filename, filesize, opts...)
}

func (c *Client) DownloadFile(ctx context.Context, objectId int64, w io.Writer, opts ...api.OptionFunc) error {
	resp, err := c.api.GetDownloadNodes(ctx, objectId, opts...)
	if err != nil {
		return err
	}
	if len(resp.Shards) == 0 {
		return errors.New("没有可用的下载节点")
	}

	opt := &api.Option{}
	for _, f := range opts {
		f(opt)
	}

	if opt.GetProgress() != nil {
		w = &progressWriter{
			w:        w,
			progress: opt.GetProgress(),
			loaded:   &atomic.Int64{},
			total:    resp.Fileinfo.Size,
		}
	}

	config := resp.Config
	if config.EnableMultiNode {
		if config.EnableErasure {
			log.Debug("Erasure coding download enabled...")
			return c.downloadErasure(ctx, w, resp.Shards, config)
		} else {
			log.Debug("Multi-node download enabled...")
			return c.downloadMultiNode(ctx, w, resp.Shards, config)
		}
	}
	r, err := c.downloadSimple(ctx, resp.Shards[0])
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	return err
}

func (c *Client) downloadMultiNode(ctx context.Context, w io.Writer, nodes []api.PresignedItem, conf api.DownloadConfig) error {
	// 多节点下载逻辑
	log.Debug("Executing multi-node download...")
	for _, node := range nodes {
		log.Debugf("Downloading from node: %s, URL: %s", node.ID, node.Presigned.Url)
		r, err := c.downloadSimple(ctx, node)
		if err != nil {
			log.Debugf("Failed to download from node %s: %v", node.ID, err)
			return err
		}
		_, err = io.Copy(w, r)
		if err != nil {
			log.Debugf("Failed to copy data from node %s: %v", node.ID, err)
			return err
		}
		log.Debugf("Node %s download completed", node.ID)
	}
	return nil
}

func (c *Client) downloadErasure(ctx context.Context, w io.Writer, nodes []api.PresignedItem, conf api.DownloadConfig) error {
	// 纠删码下载逻辑
	log.Debug("Executing erasure coding download...")
	er, err := erasure.NewErasure(ctx, int(conf.DataShard), int(conf.ParityShard), defaultBlockSize)
	if err != nil {
		log.Debugf("Failed to create Erasure instance: %v", err)
		return err
	}
	readers := make([]io.Reader, conf.DataShard+conf.ParityShard)
	wg := sync.WaitGroup{}

	for i, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.downloadSimple(ctx, node)
			if err != nil {
				log.Debugf("Failed to download from node %s: %v", node.ID, err)
				return
			}
			// u, _ := url.Parse(node.Presigned.Url)
			log.Debugf("Node %d address: %s", i, node.Presigned.Url)
			readers[i] = r
		}()
	}
	wg.Wait()
	return er.Decode(ctx, w, readers)
}

func (c *Client) downloadSimple(ctx context.Context, node api.PresignedItem) (io.Reader, error) {
	downloader := manager.NewDonwloader()
	req := &v4.PresignedHTTPRequest{
		URL:          node.Presigned.Url,
		Method:       node.Presigned.Method,
		SignedHeader: node.Presigned.Headers,
	}
	return downloader.Download(ctx, req)
}

// hashContent is a wrapper for a byte slice that satisfies the merkletree.Content interface.
type hashContent struct {
	content []byte
}

// CalculateHash hashes the data using sha256.
func (c hashContent) CalculateHash() ([]byte, error) {
	h := md5.New()
	if _, err := h.Write(c.content); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// Equals tests for equality of two Contents.
func (c hashContent) Equals(other merkletree.Content) (bool, error) {
	otherC, ok := other.(hashContent)
	if !ok {
		return false, errors.New("invalid content type")
	}
	return bytes.Equal(c.content, otherC.content), nil
}

func (c *Client) upload(ctx context.Context, sessionId string, file multipart.File, filehash string, filesize int64, opts ...api.OptionFunc) (*api.CommitObjectResponse, error) {
	// 获取节点列表
	log.Debug("get nodes for upload")
	resp, err := c.api.GetUploadNodes(ctx, sessionId, opts...)
	if err != nil {
		return nil, err
	}
	log.Debug("got upload node count:%d", len(resp.List))
	log.Debugf("got upload config:%v", resp.Config)

	nodes := resp.List
	conf := resp.Config

	// 初始化变量
	var (
		shards []api.Shard
	)

	opt := &api.Option{}
	for _, f := range opts {
		f(opt)
	}
	progress := opt.GetProgress()
	loaded := &atomic.Int64{}

	// 1. 判断是否开启纠删码 (Erasure Coding) - 优先级最高
	if conf.EnableMultiNode {
		if conf.EnableErasure {
			shards, err = c.uploadErasure(ctx, file, filesize, nodes, conf, progress, loaded)
		} else {
			shards, err = c.uploadMultiNode(ctx, file, filesize, nodes, conf, progress, loaded)
		}
		if err != nil {
			return nil, err
		}

		// 2. 判断是否开启分片上传 (Multipart)
	} else if conf.EnableMultipart {
		shards, err = c.uploadMultipart(ctx, file, filesize, nodes, conf, resp, progress, loaded)
		if err != nil {
			return nil, err
		}

		// 3. 默认简单上传 (Simple Upload)
	} else {
		shards, err = c.uploadSimple(ctx, file, filesize, filehash, nodes, progress, loaded)
		if err != nil {
			return nil, err
		}
	}

	// 4. 统一提交 (Commit Object)
	log.Debug("Start committing object info (CommitObject)...")

	commitResp, err := c.api.CommitObject(ctx, api.CommitObjectReq{
		SessionID: sessionId,
		ShardList: shards,
	}, opts...)

	if err != nil {
		log.Debugf("CommitObject error: %v", err)
		return nil, err
	}

	log.Debug("File upload and commit successful")
	return commitResp, nil
}

func (c *Client) buildMerkleTreeWithHash(file multipart.File, size int64) (*merkletree.MerkleTree, string, error) {
	defer file.Seek(0, io.SeekStart)
	chunk := make([]byte, size)
	var (
		leafs []merkletree.Content
	)

	totalHasher := md5.New()

	// 分片读取文件，计算每个分片的哈希
	for i := int64(0); ; i++ {
		// 每次读取一个分片
		// 最后一个分片不足defaultBlockSize
		n, err := file.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, "", err
		}

		data := chunk[:n]
		// 更新总文件的 Hash
		totalHasher.Write(data)

		// 计算分片哈希
		leafs = append(leafs, hashContent{content: data})
	}

	// 创建 Merkle Tree
	tree, err := merkletree.NewTreeWithHashStrategy(leafs, md5.New)
	if err != nil {
		return nil, "", err
	}

	fileHash := hex.EncodeToString(totalHasher.Sum(nil))
	return tree, fileHash, nil
}

func (c *Client) hashCheck(ctx context.Context, sessionId string, merkletree *merkletree.MerkleTree, opts ...api.OptionFunc) (obj any, match bool, err error) {
	var (
		leafHashs []api.LeafHash
	)

	// 分片读取文件，计算每个分片的哈希
	for i, leaf := range merkletree.Leafs {
		leafHashs = append(leafHashs, api.LeafHash{
			Hash:  hex.EncodeToString(leaf.Hash),
			Index: int64(i) + 1,
		})
	}

	// 计算根哈希
	rootHash := hex.EncodeToString(merkletree.MerkleRoot())
	log.Debugf(" hash:%s", rootHash)
	obj, match, err = c.api.HashCheck(ctx, api.HashCheckReq{
		RootHash:  rootHash,
		LeafHash:  leafHashs,
		SessionID: sessionId,
	}, opts...)
	// 调用调度器进行深度验证
	return obj, match, err
}

func (c *Client) uploadMultiNode(ctx context.Context, file multipart.File, filesize int64, nodes []api.PresignedItem, conf api.UploadConfig, progress api.ProgressFunc, loaded *atomic.Int64) ([]api.Shard, error) {
	chunkSize := conf.MultinodeChunkSize
	if chunkSize == 0 {
		chunkSize = defaultBlockSize
	}

	shardCount := (filesize + chunkSize - 1) / chunkSize
	if len(nodes) < int(shardCount) {
		return nil, fmt.Errorf("节点数量不足: 需要 %d, 实际 %d", shardCount, len(nodes))
	}

	log.Debugf("Start multi-node upload: file size %d, chunk size %d, shard count %d", filesize, chunkSize, shardCount)

	shards := make([]api.Shard, shardCount)
	var mu sync.Mutex
	eg, egCtx := errgroup.WithContext(ctx)

	for i := int64(0); i < shardCount; i++ {
		index := i
		offset := index * chunkSize
		size := chunkSize
		if offset+size > filesize {
			size = filesize - offset
		}

		node := nodes[index]

		eg.Go(func() error {
			uploader := manager.NewUploader()

			// 重试逻辑
			operation := func() error {
				// 每次重试都完全重新开始读取这一段
				r := io.NewSectionReader(file, offset, size)
				h := md5.New()

				var readStream io.Reader = r
				if progress != nil {
					readStream = &progressReader{
						r:        r,
						progress: progress,
						loaded:   loaded,
						total:    filesize,
					}
				}

				tr := io.TeeReader(readStream, h)

				_, err := uploader.UploadFile(egCtx, &v4.PresignedHTTPRequest{
					URL:          node.Presigned.Url,
					Method:       node.Presigned.Method,
					SignedHeader: node.Presigned.Headers,
				}, tr, size)

				if err == nil {
					// 成功后保存 hash
					hash := hex.EncodeToString(h.Sum(nil))
					mu.Lock()
					shards[index] = api.Shard{
						Index:    int(index) + 1,
						Status:   api.StatusSuccess,
						Size:     uint64(size),
						Hash:     hash,
						HashType: "md5",
						NodeID:   node.ID,
					}
					mu.Unlock()
				}
				return err
			}

			bo := backoff.NewExponentialBackOff()
			bo.MaxElapsedTime = c.cfg.MaxRetryElapsedTime

			// 预先设置失败状态
			mu.Lock()
			shards[index] = api.Shard{
				Index:  int(index) + 1,
				Status: api.StatusFailed,
				NodeID: node.ID,
			}
			mu.Unlock()

			if err := backoff.Retry(operation, backoff.WithContext(bo, egCtx)); err != nil {
				log.Debugf("Failed to upload shard %d: %v", index+1, err)
				mu.Lock()
				shards[index].Message = err.Error()
				mu.Unlock()
				return err
			}

			log.Debugf("Node %s shard %d upload completed", node.Presigned.Url, index+1)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	log.Debug("Multi-node upload process completed")
	return shards, nil
}

func (c *Client) uploadErasure(ctx context.Context, file multipart.File, filesize int64, nodes []api.PresignedItem, conf api.UploadConfig, progress api.ProgressFunc, loaded *atomic.Int64) ([]api.Shard, error) {
	log.Debug("Erasure coding upload enabled...")
	var shardNumber int64 = int64(conf.DataShard + conf.ParityShard)
	if len(nodes) < int(shardNumber) {
		err := fmt.Errorf("节点数量不足以支持纠删码: 需要 %d, 实际 %d", shardNumber, len(nodes))
		log.Debugf("Error: %v", err)
		return nil, err
	}

	uploadNodes := nodes[:shardNumber]
	writers := make([]io.Writer, shardNumber)
	shards := make([]api.Shard, shardNumber)
	chunkSize := int64(math.Ceil(float64(filesize) / float64(conf.DataShard)))
	log.Debugf("File size: %d, chunk size: %d, data shards: %d, parity shards: %d", filesize, int64(chunkSize), conf.DataShard, conf.ParityShard)
	var failedUploads int
	var mu sync.Mutex

	eg, egCtx := errgroup.WithContext(ctx)

	for i, n := range uploadNodes {
		r, w := io.Pipe()
		writers[i] = w

		index := i
		node := n

		eg.Go(func() error {
			var reader io.Reader = r
			if progress != nil {
				reader = &progressReader{
					r:        r,
					progress: progress,
					loaded:   loaded,
					total:    filesize * int64(conf.DataShard+conf.ParityShard) / int64(conf.DataShard), // Estimate total erasure volume
				}
			}
			hasher := md5.New()
			teeReader := io.TeeReader(reader, hasher)
			defer r.Close()
			endpoint := node.Presigned.Url
			log.Debugf("Start uploading shard to node %s (index: %d)", endpoint, index)
			uploader := manager.NewUploader()

			operation := func() error {
				if egCtx.Err() != nil {
					return backoff.Permanent(egCtx.Err())
				}

				_, err := uploader.UploadFile(egCtx, &v4.PresignedHTTPRequest{
					URL:          node.Presigned.Url,
					Method:       node.Presigned.Method,
					SignedHeader: node.Presigned.Headers,
				}, teeReader, chunkSize) // 这里的 filesize 传入可能需要调整，但保持原有逻辑
				return err
			}

			bo := backoff.NewExponentialBackOff()
			bo.MaxElapsedTime = c.cfg.MaxRetryElapsedTime
			shards[index] = api.Shard{
				Index:  index + 1,
				Status: api.StatusFailed,
				NodeID: node.ID,
			}
			err := backoff.Retry(operation, backoff.WithContext(bo, egCtx))

			if err != nil {
				errMsg := fmt.Sprintf("分片 %d 上传失败: %v", index+1, err)
				log.Debug(errMsg)
				shards[index].Message = err.Error()
				mu.Lock()
				failedUploads++
				if failedUploads > int(conf.ParityShard) {
					mu.Unlock()
					return fmt.Errorf("失败分片数过多: %d, 允许: %d", failedUploads, conf.ParityShard)
				}
				mu.Unlock()
				return nil // 允许其他协程继续
			}

			hash := hex.EncodeToString(hasher.Sum(nil))
			shards[index].Size = uint64(chunkSize) // 实际应该填入分片大小
			shards[index].Hash = hash
			shards[index].HashType = "md5"
			shards[index].Status = api.StatusSuccess
			log.Debugf("Node %s shard upload completed", endpoint)
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
		erasure, err := erasure.NewErasure(ctx, int(conf.DataShard), int(conf.ParityShard), defaultBlockSize)
		if err != nil {
			log.Debugf("Failed to create Erasure: %v", err)
			return err
		}
		_, err = erasure.Encode(egCtx, file, writers)
		if err != nil {
			log.Debugf("Failed to encode Erasure: %v", err)
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Debugf("Shard upload group error: %v", err)
		return nil, err
	}

	if failedUploads > int(conf.ParityShard) {
		err := fmt.Errorf("上传成功的分片不足: 需要 %d, 实际成功 %d", conf.DataShard, int(shardNumber)-failedUploads)
		log.Debugf("Error: %v", err)
		return nil, err
	}
	log.Debug("Erasure coding upload process completed")
	return shards, nil
}

func (c *Client) uploadMultipart(ctx context.Context, file multipart.File, filesize int64, nodes []api.PresignedItem, conf api.UploadConfig, apiResp *api.UploadNodesResponse, progress api.ProgressFunc, loaded *atomic.Int64) ([]api.Shard, error) {
	log.Debug("Multipart upload enabled...")
	uploader := manager.NewUploader()
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

	// Delegate progress to uploader.UploadPart
	var onProgress func(int64) = nil
	if progress != nil {
		onProgress = func(incr int64) {
			newLoaded := loaded.Add(incr)
			if newLoaded > filesize {
				newLoaded = filesize
			}
			progress(newLoaded, filesize)
		}
	}

	resps, err := uploader.UploadPart(ctx, presignParts, file, filesize, conf.MultipartChunkSize, onProgress)
	if err != nil {
		log.Debugf("Multipart upload shard failed: %v", err)
		return nil, err
	}

	shards := make([]api.Shard, 0, len(resps))
	var leafs []merkletree.Content

	for _, partResp := range resps {
		h, err := hex.DecodeString(partResp.Hash)
		if err != nil {
			log.Debugf("Failed to decode Hash: %v", err)
			return nil, err
		}
		leafs = append(leafs, hashContent{content: h})
		shards = append(shards, api.Shard{
			Index:    partResp.PartNumber,
			Status:   api.StatusSuccess,
			Size:     uint64(partResp.Size),
			Hash:     partResp.Hash,
			HashType: partResp.HashType,
			NodeID:   nodeId,
			Etag:     partResp.Etag,
		})
	}

	log.Debug("Multipart upload process completed")
	return shards, nil
}

func (c *Client) uploadSimple(ctx context.Context, file multipart.File, filesize int64, filehash string, nodes []api.PresignedItem, progress api.ProgressFunc, loaded *atomic.Int64) ([]api.Shard, error) {
	log.Debug("Executing simple single-stream upload...")
	// 简单上传通常只有一个节点
	if len(nodes) == 0 {
		err := errors.New("没有可用的上传节点")
		log.Debugf("Error: %v", err)
		return nil, err
	}
	node := nodes[0]

	// 构造一个 Shard 来代表整个文件
	shard := api.Shard{
		Index:  1,
		NodeID: node.ID,
		Status: api.StatusFailed,
	}

	uploader := manager.NewUploader()
	req := &v4.PresignedHTTPRequest{
		URL:          node.Presigned.Url,
		Method:       node.Presigned.Method,
		SignedHeader: node.Presigned.Headers,
	}
	file.Seek(0, 0) // reset file

	var readStream io.Reader = file
	if progress != nil {
		readStream = &progressReader{
			r:        file,
			progress: progress,
			loaded:   loaded,
			total:    filesize,
		}
	}

	// 使用 uploader
	_, err := uploader.UploadFile(ctx, req, readStream, filesize)
	if err != nil {
		log.Debugf("Simple upload failed: %v", err)
		shard.Message = err.Error()
		return nil, err
	}

	shard.Status = api.StatusSuccess
	shard.Size = uint64(filesize)
	shard.Hash = filehash
	shard.HashType = "md5"

	log.Debug("Simple upload completed")
	return []api.Shard{shard}, nil
}
