package main

import (
	"context"
	"log"

	"github.com/titan/doss-go-sdk"
)

func main() {
	cli, err := doss.NewClient(doss.Config{BaseEndpoint: "http://192.168.0.30:8888", Region: "auto"})
	if err != nil {
		log.Fatal(err)
	}
	resp, err := cli.UploadFile(context.TODO(), 0, "./tmp/file")
	if err != nil {
		log.Fatal(err)
	}
	log.Print(resp)
}
