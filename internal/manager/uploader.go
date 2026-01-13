package manager

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"golang.org/x/sync/errgroup"
)

type Uploader struct {
	http.Client
}

func NewUploader() *Uploader {
	return &Uploader{
		Client: http.Client{},
	}
}

func (u *Uploader) Upload(ctx context.Context, presignResult *v4.PresignedHTTPRequest, r io.Reader) error {
	// 根据预签名请求创建一个新的 HTTP PUT 请求
	// presignResult.URL 包含了完整的上传地址和查询参数
	// presignResult.Method 指定了 HTTP 方法 (通常是 "PUT")
	req, err := http.NewRequestWithContext(ctx, presignResult.Method, presignResult.URL, r)
	if err != nil {
		return err
	}

	// 将预签名结果中的头部信息添加到请求中
	// 这部分头部信息包含了认证和授权所需的重要字段，如 "X-Amz-..." 等
	req.Header = presignResult.SignedHeader
	return u.upload(ctx, req)
}

func (u *Uploader) upload(ctx context.Context, req *http.Request) error {
	// 使用 Uploader 内嵌的 http.Client 发送请求
	// u.Client 是一个标准的 Go HTTP 客户端
	log.Println("开始调用文件上传")
	b, _ := httputil.DumpRequest(req, false)
	log.Printf("request:%s", b)
	resp, err := u.Do(req)
	if err != nil {
		return err
	}
	log.Println("文件上传结束")
	// 在函数结束时关闭响应体，以防资源泄漏
	defer resp.Body.Close()
	log.Println("http code:", resp.StatusCode)
	// 检查 HTTP 响应状态码
	// 通常，成功的上传会返回 200 OK
	if resp.StatusCode != http.StatusOK {
		// 如果状态码不是 200 OK，读取响应体中的错误信息并返回一个错误
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("get body:%s", body)
			return err
		}
		return fmt.Errorf("upload failed with status code %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	log.Printf("get body:%s", body)

	// 上传成功，返回 nil
	return nil
}

func (u *Uploader) UploadPart(ctx context.Context, presignParts []*v4.PresignedHTTPRequest, r io.ReaderAt, partSize int64) error {
	g, ctx := errgroup.WithContext(ctx)

	for i, presignPart := range presignParts {
		partNumber := i
		presignPart := presignPart

		g.Go(func() error {
			// Create a section reader for the part.
			offset := int64(partNumber) * partSize
			sectionReader := io.NewSectionReader(r, offset, partSize)

			return u.Upload(ctx, presignPart, sectionReader)
		})
	}

	return g.Wait()
}
