package main

import (
	"context"
	"log"
	"os"

	"github.com/titan/doss-go-sdk"
)

func main() {
	cli := doss.NewClient(doss.Config{BaseEndpoint: "http://127.0.0.1:8888", Region: "auto"})
	file, err := os.Open("tmp/aaa.svg")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bucket := "default"
	key := "file"
	resp, err := cli.UploadFile(context.TODO(), bucket, key, file)
	if err != nil {
		log.Fatal(err)
	}
	log.Print(resp)
}
