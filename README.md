# Titan DOSS Go SDK

Titan DOSS Go SDK is a Go client library developed for Titan DOSS (Decentralized Object Storage Service). It provides an easy-to-use interface to interact with the Titan DOSS storage network, supporting features like file uploading, downloading, multi-node transmission, erasure coding, and progress monitoring.

## Features

- **File Upload/Download**: Simple APIs to upload and download files to/from the Titan DOSS network.
- **Advanced Upload/Download Modes**: Supports single stream, multi-node, multipart, and erasure coding file transmission automatically selected based on server configuration.
- **Progress Tracking**: Monitor upload and download progress in real-time.
- **Hash Verification**: Merkle tree and hash checking to ensure data integrity.

## Installation

```bash
go get github.com/sureyee/titan-doss-go-sdk
```

## Quick Start

### 1. Initialize the Client

To use the SDK, you need to configure it with the Titan DOSS API Gateway endpoint.

```go
package main

import (
	"log"

	doss "github.com/sureyee/titan-doss-go-sdk"
)

func main() {
	cfg := &doss.Config{
		BaseEndpoint: "https://doss-api-storage-dev.titannet.io/api/gateway",
		// AccessKey: "your_access_key", // Optional depending on auth strategy  
		// SecretKey: "your_secret_key", // Optional depending on auth strategy
	}

	cli, err := doss.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize client: %v", err)
	}
	
	// Use cli here
}
```

### 2. Upload a File

Use the `Upload` or `UploadFile` method to upload a locally stored file to DOSS. You can pass options like `api.WithToken` for authorization.

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

	_, err := cli.Upload(ctx, folderId, filename, api.WithToken(token))
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}

	log.Println("Upload successful")
}
```

### 3. Download a File

Use the `DownloadFile` method to retrieve a file by its object ID and copy its contents to a local file or any other `io.Writer`. You can optionally monitor the download progress.

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
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Download file with progress callback
	err = cli.DownloadFile(ctx, objectId, file, api.WithToken(token), api.WithProgress(func(loaded, total int64) {
		log.Printf("Progress: %d / %d bytes (%.2f%%)\n", loaded, total, float64(loaded)/float64(total)*100)
	}))

	if err != nil {
		log.Fatalf("Download failed: %v", err)
	}

	log.Println("Download completed successfully")
}
```

## Advanced Usage

### Progress Monitoring
You can monitor the upload and download progress in real-time by passing the `api.WithProgress` option to the request:

```go
api.WithProgress(func(loaded, total int64) {
    log.Printf("Progress Update: loaded %d of %d", loaded, total)
})
```
