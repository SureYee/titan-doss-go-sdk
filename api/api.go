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

	"github.com/golang-jwt/jwt/v5"
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

type ApiClient struct {
	baseUrl   string
	accessKey string
	secretKey string
	cli       http.Client
}

func NewApi(baseUrl string, accessKey, secretKey string) *ApiClient {
	return &ApiClient{
		baseUrl:   baseUrl,
		accessKey: accessKey,
		secretKey: secretKey,
		cli: http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type Option struct {
	token    string
	progress ProgressFunc
}

type OptionFunc func(*Option)

type ProgressFunc func(loaded, total int64)

func WithToken(token string) OptionFunc {
	return func(o *Option) {
		o.token = token
	}
}

func WithProgress(f ProgressFunc) OptionFunc {
	return func(o *Option) {
		o.progress = f
	}
}

func (o *Option) GetProgress() ProgressFunc {
	return o.progress
}

func (s *ApiClient) GetDownloadNodes(ctx context.Context, fileId int64, opts ...OptionFunc) (*DownloadNodesResponse, error) {
	req, err := s.buildRequest(ctx, http.MethodGet, V1_DownloadNodes, map[string]string{
		"fileId": fmt.Sprint(fileId),
	}, nil, opts...)
	if err != nil {
		return nil, err
	}
	resp, err := s.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	d, err := parseBody[DownloadNodesResponse](resp.Body)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *ApiClient) GetUploadNodes(ctx context.Context, sessionId string, opts ...OptionFunc) (*UploadNodesResponse, error) {
	req, err := s.buildRequest(ctx, http.MethodGet, V1_GetUploadNodes, map[string]string{
		"sessionId": sessionId,
	}, nil, opts...)
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

func (s *ApiClient) CommitObject(ctx context.Context, req CommitObjectReq, opts ...OptionFunc) error {
	r, err := s.buildRequest(ctx, http.MethodPost, V1_CommitObject, nil, req, opts...)
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

func (s *ApiClient) HashCheck(ctx context.Context, req HashCheckReq, opts ...OptionFunc) (object any, match bool, err error) {
	r, err := s.buildRequest(ctx, http.MethodPost, V1_HashCheck, nil, req, opts...)
	if err != nil {
		return nil, false, err
	}
	resp, err := s.cli.Do(r)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	d, err := parseBody[HashCheckResponse](resp.Body)
	if err != nil {
		return nil, false, err
	}
	return nil, d.Match, nil
}

func (s *ApiClient) buildRequest(ctx context.Context, method string, path string, param map[string]string, data any, opts ...OptionFunc) (*http.Request, error) {
	var opt = new(Option)
	for _, o := range opts {
		o(opt)
	}

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
	var token string
	if opt.token != "" {
		token = opt.token
	} else {
		token = s.genToken()
	}
	r.Header.Set("Authorization", "Bearer "+token)
	return r, nil
}

func (s *ApiClient) genToken() string {
	if s.accessKey == "" || s.secretKey == "" {
		return ""
	}
	token, _ := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"accessKey": s.accessKey,
		"exp":       time.Now().Add(15 * time.Minute).Unix(),
	}).SignedString([]byte(s.secretKey))
	return token
}

func (s *ApiClient) CreateUpload(ctx context.Context, req *CreateUploadReq, opts ...OptionFunc) (*CreateUploadResp, error) {
	r, err := s.buildRequest(ctx, http.MethodPost, "/v1/create-upload", nil, req, opts...)
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
		return t, NewResponseError(resp.Code, resp.Msg)
	}
	return resp.Data, nil
}
