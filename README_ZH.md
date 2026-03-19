# Titan DOSS Go SDK

Titan DOSS Go SDK 是为 Titan DOSS (去中心化对象存储服务) 提供的 Go 语言客户端开发包。它封装了与 Titan DOSS 存储网络的交互接口，提供文件上传、下载等功能，并根据服务端配置自动支持多节点并发、分片重组、纠删码等多种传输模式，以及上传与下载的实时进度监听功能。

## 特性

- **文件上传与下载**: 提供了直观简单的 API，只需少部分代码即可将文件上传至 Titan DOSS 网络或从中下载。
- **自适应高级传输模式**: 内部根据请求的配置自动支持简单单流上传下载、多节点并发执行、文件分片（Multipart）上传以及基于纠删码（Erasure Coding）的网络传输机制。
- **进度监听**: 支持注册回调函数，实时监控并获取上传和下载进度。
- **数据完整性校验**: 上传流程中内置基于 Merkle Tree 和 Hash 的完整性校验，以确保数据的准确与安全。

## 安装

```bash
go get github.com/sureyee/titan-doss-go-sdk
```

## 快速开始

### 1. 初始化客户端

使用 SDK 之前，首先需要初始化 `Client`，通常通过指定 API 的接入端点（Endpoint）进行配置。

```go
package main

import (
	"log"

	doss "github.com/sureyee/titan-doss-go-sdk"
)

func main() {
	cfg := &doss.Config{
		BaseEndpoint: "https://doss-api-storage-dev.titannet.io/api/gateway",
		// AccessKey: "Your_Access_Key", // 根据使用的服务端鉴权形式可选择性填入  
		// SecretKey: "Your_Secret_Key", // 根据使用的服务端鉴权形式可选择性填入
	}

	cli, err := doss.NewClient(cfg)
	if err != nil {
		log.Fatalf("初始化客户端失败: %v", err)
	}
	
	// 在此处使用 cli...
}
```

### 2. 上传文件

通过 `Upload` 或 `UploadFile` 方法将本地文件上传至云端网络。可通过 `api.WithToken` 获取认证授权支持。

```go
package main

import (
	"context"
	"log"

	doss "github.com/sureyee/titan-doss-go-sdk"
	"github.com/sureyee/titan-doss-go-sdk/api"
)

func uploadExample(cli *doss.Client, token string) {
	ctx := context.Background()
	folderId := int64(0)
	filename := "path/to/your/local/file.txt"

	// 传入本地文件路径发起上传调用
	_, err := cli.Upload(ctx, folderId, filename, api.WithToken(token))
	if err != nil {
		log.Fatalf("文件上传失败: %v", err)
	}

	log.Println("文件上传成功")
}
```

### 3. 下载文件

通过服务端系统提供的 `Object ID`，你可以配合 `io.Writer` （如本地文件句柄）对文件进行下载写入。同样能够结合 `api.WithProgress` 监听整体流程中的详细下载进度。

```go
package main

import (
	"context"
	"log"
	"os"

	doss "github.com/sureyee/titan-doss-go-sdk"
	"github.com/sureyee/titan-doss-go-sdk/api"
)

func downloadExample(cli *doss.Client, objectId int64, token string) {
	ctx := context.Background()

	file, err := os.Create("downloaded_file.txt")
	if err != nil {
		log.Fatalf("创建本地文件失败: %v", err)
	}
	defer file.Close()

	// 附加上传/下载进度函数的调用
	err = cli.DownloadFile(ctx, objectId, file, api.WithToken(token), api.WithProgress(func(loaded, total int64) {
		log.Printf("下载进度: %d / %d bytes (%.2f%%)\n", loaded, total, float64(loaded)/float64(total)*100)
	}))

	if err != nil {
		log.Fatalf("文件下载失败: %v", err)
	}

	log.Println("文件下载成功")
}
```

## 高级用法

### 进度跟踪
不论是下载还是上传，都可传入 Option 形式的参数 `api.WithProgress` 到网络请求内，这会被自动识别和执行：

```go
api.WithProgress(func(loaded, total int64) {
    log.Printf("进度更新: 已加载 %d / 总计大小 %d", loaded, total)
})
```
