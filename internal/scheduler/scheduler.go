package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	CodeSuccess = 0
)

const (
	StatusSuccess = 1
	StatusFailed  = 2
)

const (
	V1_DownloadNodes = "/v1/download-nodes"
)

type Shard struct {
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
	Etag        string `json:"etag"`
}

type object struct {
	Size     int64  `json:"size"`
	Hash     string `json:"hash"`
	HashType string `json:"hashType"`
}

type downloadNodesResponse struct {
	Object object  `json:"fileinfo"`
	Shards []Shard `json:"shards"`
	// Config  `json:"config"`
}

type Scheduler struct {
	baseUrl string
	cli     http.Client
}

func NewScheduler(baseUrl string) *Scheduler {
	return &Scheduler{
		baseUrl: baseUrl,
		cli: http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *Scheduler) GetDownloadNodes(region, bucket, key string) (*downloadNodesResponse, error) {
	query := url.Values{}
	query.Add("region", region)
	query.Add("bucket", bucket)
	query.Add("key", key)
	u := s.baseUrl + V1_DownloadNodes + "?" + query.Encode()
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

func (s *Scheduler) GetUploadNodes(uploadId, objectId, fileHash string, size int64) (*UploadNodesResponse, error) {
	query := url.Values{}
	query.Add("uploadId", uploadId)
	query.Add("objectId", objectId)
	query.Add("fileHash", fileHash)
	query.Add("size", strconv.FormatInt(size, 10))
	u := s.baseUrl + "/v1/upload-nodes?" + query.Encode()
	resp, err := s.cli.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	d, err := parseBody[UploadNodesResponse](resp.Body)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Scheduler) CommitObject(ctx context.Context, req CommitObjectReq) error {
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

// PreCheck
// 预检
func (s *Scheduler) PreCheck(ctx context.Context, req PreCheckReq) (match bool, err error) {
	//
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return false, err
	}
	u := s.baseUrl + "/v1/pre-check-hash"
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u, buf)
	if err != nil {
		return false, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := s.cli.Do(r)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	d, err := parseBody[PreCheckHashResponse](resp.Body)
	if err != nil {
		return false, err
	}
	return d.Match, nil
}

func (s *Scheduler) HashCheck(ctx context.Context, req HashCheckReq) (object any, match bool, err error) {
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return nil, false, err
	}
	u := s.baseUrl + "/v1/check-hash"
	resp, err := s.cli.Post(u, "application/json", buf)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	d, err := parseBody[PreCheckHashResponse](resp.Body)
	if err != nil {
		return nil, false, err
	}
	return nil, d.Match, nil
}

func parseBody[T any](body io.Reader) (t T, err error) {
	var resp Response[T]
	err = json.NewDecoder(body).Decode(&resp)
	if err != nil {
		return t, err
	}
	if resp.Code != CodeSuccess {
		return t, fmt.Errorf("[%d] %s", resp.Code, resp.Msg)
	}
	return resp.Data, nil
}
