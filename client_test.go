package doss_test

import (
	"log"
	"os"
	"testing"
	"time"

	doss "github.com/sureyee/titan-doss-go-sdk"
	"github.com/sureyee/titan-doss-go-sdk/api"
)

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
	err = cli.DownloadFile(t.Context(), 389, f, api.WithToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzI3NjMyNjgsImlhdCI6MTc3MjU5MDQ2OCwidXVpZCI6ImQ1b3Y0M3ZuYWg4dTU3ZGhyNDIwIn0.d4wMKJwhgcVucKCn9xc4ukeZxmjqnawD3_Ng4GMvZQ8"))
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("use time:%s", time.Since(st).String())
}
