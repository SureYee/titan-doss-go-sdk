package doss

import (
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestUpload(t *testing.T) {
	cli := NewClient()
	file, err := os.Open("tmp/file1.txt")
	if err != nil {
		t.Fatal(err)
	}
	bucket := "default"
	key := "file1.txt"
	resp, err := cli.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   file,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(resp)
}

func TestDownload(t *testing.T) {
	cli := NewClient()
	bucket := "default"
	key := "file1.txt"
	resp, err := cli.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create("tmp/download.txt")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(f, resp.Body)
	f.Close()
	resp.Body.Close()

	t.Log(resp)
}
