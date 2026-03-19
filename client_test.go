package doss_test

import (
	"log"
	"os"
	"testing"
	"time"

	doss "github.com/sureyee/titan-doss-go-sdk"
	"github.com/sureyee/titan-doss-go-sdk/api"
)

const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzI4NzMxNjEsImlhdCI6MTc3MjcwMDM2MSwidXVpZCI6ImQ1c3U4aXZnNW82bGVjcjhic2cwIn0.2U7xIC7M5HX2et131cnyFmhesaAUjTXurmkWJwJrURE"

func TestDownload(t *testing.T) {
	cli, err := doss.NewClient(&doss.Config{
		BaseEndpoint: "https://doss-api-storage-dev.titannet.io/api/gateway",
	})
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("tmp/download")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	st := time.Now()
	err = cli.DownloadFile(t.Context(), 399, f, api.WithToken(token), api.WithProgress(func(loaded, total int64) {
		log.Printf("loaded: %d, total: %d", loaded, total)
	}))
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("use time:%s", time.Since(st).String())
}

func TestUpload(t *testing.T) {
	cli, err := doss.NewClient(&doss.Config{
		BaseEndpoint: "https://doss-api-storage-dev.titannet.io/api/gateway",
	})
	if err != nil {
		log.Fatal(err)
	}

	st := time.Now()
	_, err = cli.Upload(t.Context(), 0, "tmp/node", api.WithToken(token))
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("use time:%s", time.Since(st).String())
}
