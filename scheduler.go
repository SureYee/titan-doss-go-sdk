package doss

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

type response[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type erasureConfig struct {
	Encode      bool `json:"encode"`
	DataShard   int  `json:"dataShard"`
	ParityShard int  `json:"parityShard"`
}

type node struct {
	ID     string `json:"id"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type uploadNodesResponse struct {
	Shards []node        `json:"shards"`
	Config erasureConfig `json:"config"`
}

type commitObjectReq struct {
	Config    erasureConfig `json:"config"`
	Bucket    string        `json:"bucket"`
	Key       string        `json:"key"`
	Size      uint64        `json:"size"`
	Hash      string        `json:"hash"`
	HashType  string        `json:"hashType"`
	ShardList []shard       `json:"shardList"`
}

const (
	StatusSuccess = 1
	StatusFailed  = 2
)

type shard struct {
	Index       int    `json:"index"`
	Status      int    `json:"status"`
	Size        uint64 `json:"size"`
	Hash        string `json:"hash"`
	HashType    string `json:"hashType"`
	NodeID      string `json:"nodeID"`
	NodeAddress string `json:"nodeAddress"`
	Message     string `json:"message"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
}

type downloadNodesResponse struct {
	Shards []shard       `json:"shards"`
	Config erasureConfig `json:"config"`
}

type scheduler struct {
	baseUrl string
	cli     http.Client
}

func newScheduler(baseUrl string) *scheduler {
	return &scheduler{
		baseUrl: baseUrl,
		cli: http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *scheduler) getDownloadNodes(region, bucket, key string) (*downloadNodesResponse, error) {
	query := url.Values{}
	query.Add("region", region)
	query.Add("bucket", bucket)
	query.Add("key", key)
	u := s.baseUrl + "/v1/download-nodes?" + query.Encode()
	resp, err := s.cli.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	d, err := parseBody[downloadNodesResponse](resp.Body)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *scheduler) getUploadNodes(region, bucket, key string, size int64) (*uploadNodesResponse, error) {
	query := url.Values{}
	query.Add("region", region)
	query.Add("bucket", bucket)
	query.Add("key", key)
	query.Add("size", strconv.FormatInt(size, 10))
	u := s.baseUrl + "/v1/upload-nodes?" + query.Encode()
	resp, err := s.cli.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	d, err := parseBody[uploadNodesResponse](resp.Body)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *scheduler) commitObject(ctx context.Context, req commitObjectReq) error {
	u := s.baseUrl + "/v1/commit-object"
	body := bytes.NewBuffer(nil)
	err := json.NewEncoder(body).Encode(req)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return fmt.Errorf("new request error:%w", err)
	}
	r.Header.Set("Content-Type", "application/json")
	d, _ := httputil.DumpRequest(r, true)
	fmt.Println(string(d))

	resp, err := s.cli.Do(r)
	if err != nil {
		return fmt.Errorf("scheduler request error:%s", err)
	}
	defer resp.Body.Close()
	if _, err := parseBody[any](resp.Body); err != nil {
		return err
	}

	return nil

}

func parseBody[T any](body io.Reader) (t T, err error) {
	var resp response[T]
	err = json.NewDecoder(body).Decode(&resp)
	if err != nil {
		return t, err
	}
	if resp.Code != CodeSuccess {
		return t, fmt.Errorf("[%d] %s", resp.Code, resp.Msg)
	}
	return resp.Data, nil
}
