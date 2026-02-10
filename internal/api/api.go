package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	V1_DownloadNodes  = "/v1/download-nodes"
	V1_HashCheck      = "/v1/check-hash"
	V1_GetUploadNodes = "/v1/upload-nodes"
	V1_CommitObject   = "/v1/commit-object"
)

type Shard struct {
	Index    int    `json:"index"`
	Status   int    `json:"status"`
	Size     uint64 `json:"size"`
	Hash     string `json:"hash"`
	HashType string `json:"hashType"`
	NodeID   string `json:"nodeID"`
	Message  string `json:"message"`
	Etag     string `json:"etag"`
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

type ApiClient struct {
	baseUrl string
	cli     http.Client
}

func NewApi(baseUrl string) *ApiClient {
	return &ApiClient{
		baseUrl: baseUrl,
		cli: http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *ApiClient) GetDownloadNodes(ctx context.Context, region, bucket, key string) (*downloadNodesResponse, error) {
	req, err := s.buildRequest(ctx, http.MethodGet, V1_DownloadNodes, map[string]string{
		"region": region,
		"bucket": bucket,
		"key":    key,
	}, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.cli.Do(req)
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

func (s *ApiClient) GetUploadNodes(ctx context.Context, sessionId string) (*UploadNodesResponse, error) {
	req, err := s.buildRequest(ctx, http.MethodGet, V1_GetUploadNodes, map[string]string{
		"sessionId": sessionId,
	}, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.cli.Do(req)
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

func (s *ApiClient) CommitObject(ctx context.Context, req CommitObjectReq) error {
	r, err := s.buildRequest(ctx, http.MethodPost, V1_CommitObject, nil, req)
	if err != nil {
		return err
	}
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
func (s *ApiClient) PreCheck(ctx context.Context, req PreCheckReq) (match bool, err error) {
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

func (s *ApiClient) HashCheck(ctx context.Context, req HashCheckReq) (object any, match bool, err error) {
	r, err := s.buildRequest(ctx, http.MethodPost, V1_HashCheck, nil, req)
	if err != nil {
		return nil, false, err
	}
	resp, err := s.cli.Do(r)
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

func (s *ApiClient) buildRequest(ctx context.Context, method string, path string, param map[string]string, data any) (*http.Request, error) {
	var body io.Reader
	if data != nil {
		buf := bytes.NewBuffer(nil)
		if err := json.NewEncoder(buf).Encode(data); err != nil {
			return nil, err
		}
		body = buf
	}

	if len(param) > 0 {
		query := url.Values{}
		for k, v := range param {
			query.Add(k, v)
		}
		path += "?" + query.Encode()
	}

	u := s.baseUrl + path
	r, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+s.genToken())
	return r, nil
}

func (s *ApiClient) genToken() string {
	return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzA3NzY1OTksImlhdCI6MTc3MDYwMzc5OSwidXVpZCI6ImQ1c3U4aXZnNW82bGVjcjhic2cwIn0.EHtLqPn0iimeGHyo-kwMMh9ziPA-DPdzQhWKJSmVLvg"
}

func (s *ApiClient) CreateUpload(ctx context.Context, req *CreateUploadReq) (*CreateUploadResp, error) {
	r, err := s.buildRequest(ctx, http.MethodPost, "/v1/create-upload", nil, req)
	if err != nil {
		return nil, err
	}
	resp, err := s.cli.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return parseBody[*CreateUploadResp](resp.Body)
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
