package erasure

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
)

func TestErasure_EncodeDecode(t *testing.T) {
	ctx := context.Background()
	dataBlocks := 10
	parityBlocks := 4
	// blockSize must be a multiple of dataBlocks to avoid padding in intermediate blocks during streaming encode
	blockSize := int64(1024)

	e, err := NewErasure(ctx, dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Fatalf("NewErasure failed: %v", err)
	}

	fileSize := 5 * 1024 * 1024
	data := make([]byte, fileSize)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Failed to create random data: %v", err)
	}

	writers := make([]io.Writer, dataBlocks+parityBlocks)
	buffers := make([]*bytes.Buffer, dataBlocks+parityBlocks)
	for i := range writers {
		buffers[i] = new(bytes.Buffer)
		writers[i] = buffers[i]
	}

	totalWritten, err := e.Encode(ctx, bytes.NewReader(data), writers)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if totalWritten != int64(fileSize) {
		t.Errorf("Expected total written %d, got %d", fileSize, totalWritten)
	}

	readers := make([]io.Reader, dataBlocks+parityBlocks)
	for i := range readers {
		readers[i] = bytes.NewReader(buffers[i].Bytes())
	}

	var decoded bytes.Buffer
	if err := e.Decode(ctx, &decoded, readers); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Due to Reed-Solomon encoding padding, the decoded data might be larger than original.
	// We verify that it starts with the original data.
	if !bytes.HasPrefix(decoded.Bytes(), data) {
		t.Error("Decoded data does not match original data prefix")
	}
}

func TestErasure_DecodeWithFailures(t *testing.T) {
	ctx := context.Background()
	dataBlocks := 10
	parityBlocks := 4
	blockSize := int64(100)

	e, err := NewErasure(ctx, dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Fatalf("NewErasure failed: %v", err)
	}

	data := []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.")

	writers := make([]io.Writer, dataBlocks+parityBlocks)
	buffers := make([]*bytes.Buffer, dataBlocks+parityBlocks)
	for i := range writers {
		buffers[i] = new(bytes.Buffer)
		writers[i] = buffers[i]
	}

	_, err = e.Encode(ctx, bytes.NewReader(data), writers)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	for i := 1; i <= parityBlocks; i++ {
		readers := make([]io.Reader, dataBlocks+parityBlocks)
		for j := range readers {
			if j < i {
				readers[j] = nil
			} else {
				readers[j] = bytes.NewReader(buffers[j].Bytes())
			}
		}

		var decoded bytes.Buffer
		if err := e.Decode(ctx, &decoded, readers); err != nil {
			t.Fatalf("Decode with %d failures failed: %v", i, err)
		}
		if !bytes.HasPrefix(decoded.Bytes(), data) {
			t.Errorf("Decoded data with %d failures does not start with original data", i)
		}
	}

	readers := make([]io.Reader, dataBlocks+parityBlocks)
	for j := range readers {
		if j <= parityBlocks {
			readers[j] = nil
		} else {
			readers[j] = bytes.NewReader(buffers[j].Bytes())
		}
	}
	var decoded bytes.Buffer
	if err := e.Decode(ctx, &decoded, readers); err == nil {
		t.Error("Expected error when too many shards are missing, got nil")
	}
}
