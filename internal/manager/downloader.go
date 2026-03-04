package manager

import (
	"context"
	"fmt"
	"io"
	"net/http"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type Downloader struct {
	http.Client
}

func NewDonwloader() *Downloader {
	return &Downloader{
		Client: http.Client{},
	}
}

func (d *Downloader) Download(ctx context.Context, presignResult *v4.PresignedHTTPRequest) (io.Reader, error) {
	req, err := http.NewRequestWithContext(ctx, presignResult.Method, presignResult.URL, nil)
	if err != nil {
		return nil, err
	}
	// 将预签名结果中的头部信息添加到请求中
	// 这部分头部信息包含了认证和授权所需的重要字段，如 "X-Amz-..." 等
	req.Header = presignResult.SignedHeader
	return download(d, req)
}

func download(d *Downloader, req *http.Request) (io.Reader, error) {
	// 使用 Downloader 内嵌的 http.Client 发送请求
	// d.Client 是一个标准的 Go HTTP 客户端
	resp, err := d.Do(req)
	if err != nil {
		return nil, err
	}
	// 在函数结束时关闭响应体，以防资源泄漏
	// 检查 HTTP 响应状态码
	// 通常，成功的下载会返回 200 OK
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("下载失败，HTTP 状态码: %d", resp.StatusCode)
	}
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		defer resp.Body.Close()
		_, err := io.Copy(pw, resp.Body)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}
