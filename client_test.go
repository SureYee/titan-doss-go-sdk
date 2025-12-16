package doss

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestUpload(t *testing.T) {
	cli := NewClient(Config{SchedulerURL: "http://192.168.0.30:8888"})
	file, err := os.Open("tmp/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	bucket := "default"
	key := "file.txt"
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

func TestS3Upload(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"minioadmin", // Access Key ID
			"minioadmin", // Secret Access Key
			"",
		)),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatal(err)
	}

	m := manager.NewUploader(s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
	}))
	f, err := os.Open("tmp/file")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := m.Upload(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("mybucket"),
		Key:    aws.String("file"),
		Body:   f,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", resp)
}

func TestDownload(t *testing.T) {
	cli := NewClient(Config{SchedulerURL: "http://192.168.0.30:8888"})
	bucket := "default"
	key := "file"
	resp, err := cli.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create("tmp/download")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(f, resp.Body)
	f.Close()
	resp.Body.Close()

	t.Log(resp)
}
