package doss_test

import (
	"os"
	"testing"
	"time"

	doss "github.com/sureyee/titan-doss-go-sdk"
	"github.com/sureyee/titan-doss-go-sdk/api"
	"github.com/sureyee/titan-doss-go-sdk/log"
)

const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzQ1ODQ4MDQsImlhdCI6MTc3NDQxMjAwNCwidXVpZCI6ImQ1c3U4aXZnNW82bGVjcjhic2cwIn0.ZE5CnS_0j3yFhHA-xppXcRuJuF_RTCdIH2MrhC7qaAE"

func TestDownload(t *testing.T) {
	cli, err := doss.NewClient(&doss.Config{
		BaseEndpoint: "https://doss-api-storage-dev.titannet.io/api/gateway",
	})
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Create("tmp/download")
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()
	st := time.Now()
	err = cli.DownloadFile(t.Context(), 399, f, api.WithToken(token), api.WithProgress(func(loaded, total int64) {
		t.Logf("loaded: %d, total: %d", loaded, total)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("use time:%s", time.Since(st).String())
}

func TestUpload(t *testing.T) {
	log.SetLogger(log.NewLogger())
	cli, err := doss.NewClient(&doss.Config{
		BaseEndpoint: "https://doss-api-storage-dev.titannet.io/api/gateway",
	})
	if err != nil {
		t.Fatal(err)
	}

	st := time.Now()
	_, err = cli.Upload(t.Context(), 0, "tmp/upload", api.WithToken(token))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("use time:%s", time.Since(st).String())
}
