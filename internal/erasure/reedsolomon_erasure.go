package internal

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// Writes to multiple writers
type multiWriter struct {
	writers []io.Writer
	errs    []error
}

var (
	errWriterNotFound = errors.New("writer not found")
)

func countErrs(errs []error, err error) int {
	i := 0
	for _, err1 := range errs {
		if err1 == err || errors.Is(err1, err) {
			i++
		}
	}
	return i
}

// Write writes data to writers.
func (p *multiWriter) Write(ctx context.Context, blocks [][]byte) error {
	for i := range p.writers {
		if p.errs[i] != nil {
			continue
		}
		if p.writers[i] == nil {
			p.errs[i] = errWriterNotFound
			continue
		}
		var n int
		n, p.errs[i] = p.writers[i].Write(blocks[i])
		if p.errs[i] == nil {
			if n != len(blocks[i]) {
				p.errs[i] = io.ErrShortWrite
				p.writers[i] = nil
			}
		} else {
			p.writers[i] = nil
		}
	}

	for _, err := range p.errs {
		if err != nil {
			return err
		}
	}

	return nil
}

type Erasure struct {
	encoder                  reedsolomon.Encoder
	dataBlocks, parityBlocks int
	blockSize                int64
}

func NewErasure(ctx context.Context, dataBlocks, parityBlocks int, blockSize int64) (e Erasure, err error) {
	if dataBlocks <= 0 || parityBlocks < 0 {
		return e, reedsolomon.ErrInvShardNum
	}

	if dataBlocks+parityBlocks > 256 {
		return e, reedsolomon.ErrMaxShardNum
	}

	e = Erasure{
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		blockSize:    blockSize,
	}

	// Encoder when needed.
	e.encoder, err = reedsolomon.New(dataBlocks, parityBlocks, reedsolomon.WithAutoGoroutines(int(e.ShardSize())))
	if err != nil {
		return e, err
	}
	return e, err
}

func (e *Erasure) ShardSize() int64 {
	return ceilFrac(e.blockSize, int64(e.dataBlocks))
}

func (e *Erasure) Encode(ctx context.Context, src io.Reader, writers []io.Writer) (int64, error) {
	// encoder
	var total int64
	buf := make([]byte, e.blockSize)
	writer := &multiWriter{
		writers: writers,
		errs:    make([]error, len(writers)),
	}
	for {
		var blocks [][]byte
		n, err := io.ReadFull(src, buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, err
			}
		}

		eof := err == io.EOF || err == io.ErrUnexpectedEOF

		if n == 0 && total != 0 {
			break
		}

		blocks, err = e.EncodeData(ctx, buf[:n])
		if err != nil {
			return 0, err
		}
		if err = writer.Write(ctx, blocks); err != nil {
			return 0, err
		}
		total += int64(n)
		if eof {
			break
		}
	}
	return total, nil
}

func (e *Erasure) EncodeData(ctx context.Context, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.dataBlocks+e.parityBlocks), nil
	}
	encoded, err := e.encoder.Split(data)
	if err != nil {
		return nil, err
	}
	if err = e.encoder.Encode(encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}

func (e *Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.Reader) error {
	if len(readers) != e.dataBlocks+e.parityBlocks {
		return reedsolomon.ErrInvShardNum
	}

	// We read ShardSize from each reader, which corresponds to blockSize of original data
	// (plus padding if any).
	shardSize := int(e.ShardSize())
	shards := make([][]byte, e.dataBlocks+e.parityBlocks)
	readErrs := make([]error, len(readers))
	ns := make([]int, len(readers))
	var wg sync.WaitGroup

	for {
		// Reset state for next block
		for i := range shards {
			shards[i] = nil
			readErrs[i] = nil
			ns[i] = 0
		}

		// Parallel read from all readers
		wg.Add(len(readers))
		for i, r := range readers {
			go func(i int, r io.Reader) {
				defer wg.Done()
				if r == nil {
					readErrs[i] = io.EOF // Treat nil reader as EOF/Failed
					return
				}
				buf := make([]byte, shardSize)
				var n int
				var err error
				// Use ReadFull to try to get full shard.
				// If last block is partial, ReadFull returns ErrUnexpectedEOF or EOF with n > 0.
				n, err = io.ReadFull(r, buf)
				ns[i] = n
				readErrs[i] = err
				if n > 0 {
					shards[i] = buf
				}
			}(i, r)
		}
		wg.Wait()

		// Determine the consensus read length
		var targetLen int
		allEOF := true

		// Find the first valid length
		for i := range readers {
			if readers[i] != nil && ns[i] > 0 {
				allEOF = false
				if targetLen == 0 {
					targetLen = ns[i]
				}
			}
		}

		if allEOF {
			// Check if any errors other than EOF occurred for non-nil readers?
			// But we treated nil readers as EOF above.
			// Effectively, if no data was read from any active reader, we are done.
			return nil
		}

		if targetLen == 0 {
			// Not all EOF, but no data read?
			// This means active readers returned (0, err) where err != EOF?
			// Or just empty reads?
			return errors.New("unexpected read error: no data read from active readers")
		}

		// Validate shards and prepare for reconstruction
		validCount := 0
		for i := range readers {
			if readers[i] == nil {
				shards[i] = nil
				continue
			}

			// If this reader read nothing, but others did -> inconsistent
			if ns[i] == 0 {
				shards[i] = nil
				// If it returned EOF, it's out of sync (short stream)
				// Mark as failed
				readers[i] = nil
				continue
			}

			// If length mismatch
			if ns[i] != targetLen {
				shards[i] = nil
				readers[i] = nil
				continue
			}

			// If we had a read error (like UnexpectedEOF which is normal for last block,
			// or other errors), we still have data.
			// But if it's a hard error, maybe we should distrust it?
			// io.ReadFull returns EOF or ErrUnexpectedEOF only if it hit EOF.
			// So (n > 0, err != nil) is usually fine for the last block.

			// Truncate buffer to actual read size
			shards[i] = shards[i][:targetLen]
			validCount++
		}

		if validCount < e.dataBlocks {
			return errors.New("too many failed readers, cannot reconstruct")
		}

		// Reconstruct if necessary
		// We always try to Verify first?
		// Or if we have nil shards, we MUST Reconstruct.
		ok, err := e.encoder.Verify(shards)
		if !ok || err != nil {
			if err = e.encoder.Reconstruct(shards); err != nil {
				return err
			}
			ok, err = e.encoder.Verify(shards)
			if !ok || err != nil {
				return errors.New("reconstruction failed verification")
			}
		}

		// Write data to writer
		// Join writes dataBlocks * len(shards[0])
		if err := e.encoder.Join(writer, shards, targetLen*e.dataBlocks); err != nil {
			return err
		}
	}
}

func ceilFrac(numerator, denominator int64) (ceil int64) {
	if denominator == 0 {
		// do nothing on invalid input
		return ceil
	}
	// Make denominator positive
	if denominator < 0 {
		numerator = -numerator
		denominator = -denominator
	}
	ceil = numerator / denominator
	if numerator > 0 && numerator%denominator != 0 {
		ceil++
	}
	return ceil
}
