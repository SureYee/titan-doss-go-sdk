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
)

type response[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type erasureConfig struct {
	DataShard   int `json:"dataShard"`
	ParityShard int `json:"parityShard"`
}

type node struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type endpoint struct {
	Shards []node `json:"shards"`
}

type commitObjectReq struct {
	Bucket    string  `json:"bucket"`
	Key       string  `json:"key"`
	Size      uint64  `json:"size"`
	Hash      string  `json:"hash"`
	HashType  string  `json:"hashType"`
	ShardList []shard `json:"shardList"`
}

type shard struct {
	Index       int    `json:"index"`
	Size        uint64 `json:"size"`
	Hash        string `json:"hash"`
	HashType    string `json:"hashType"`
	NodeID      string `json:"nodeID"`
	NodeAddress string `json:"nodeAddress"`
}

type shards struct {
	Shards []shard `json:"shards"`
}

type scheduler struct {
	baseUrl string
	cli     http.Client
}

func newScheduler(baseUrl string) *scheduler {
	return &scheduler{
		baseUrl: baseUrl,
	}
}

func (s *scheduler) getErasureConfig(ctx context.Context) (e erasureConfig, err error) {
	u := s.baseUrl + "/v1/config"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return e, err
	}
	resp, err := s.cli.Do(req)
	if err != nil {
		return e, err
	}
	defer resp.Body.Close()

	return parseBody[erasureConfig](resp.Body)
}

func (s *scheduler) getDownloadNodes(region, bucket, key string) ([]shard, error) {
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

	d, err := parseBody[shards](resp.Body)
	if err != nil {
		return nil, err
	}
	return d.Shards, nil
}

func (s *scheduler) getUploadNodes(region, bucket, key string, size int64) ([]node, error) {
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

	d, err := parseBody[endpoint](resp.Body)
	if err != nil {
		return nil, err
	}
	return d.Shards, nil
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
