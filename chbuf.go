package chinsert

import (
	"bytes"
	"fmt"
	"io"
)

// BufInsert takes care of data integrity which is critical for the clickhouse data insertion.
// We cannot insert half of the clickhouse record, so each Encoder's output must be kept unsplitted
type BufInsert struct {
	limit    int
	buf      *bytes.Buffer
	inserter io.Writer
}

// NewBuf constructor
func NewBuf(inserter io.Writer, limit int) *BufInsert {
	if limit <= 0 {
		panic(fmt.Errorf("Limit must be greater than 0, got %d", limit))
	}
	res := &BufInsert{
		limit:    limit,
		buf:      &bytes.Buffer{},
		inserter: inserter,
	}
	res.buf.Grow(limit)
	return res
}

// Write implementation
// It is guaranteed no old data will be lost on error
func (bw *BufInsert) Write(p []byte) (n int, err error) {
	if len(p)+bw.buf.Len() > bw.limit {
		err = bw.Flush()
		if err != nil {
			return -1, err
		}
	}
	return bw.buf.Write(p)
}

// Flush data
func (bw *BufInsert) Flush() error {
	if bw.buf.Len() == 0 {
		return nil
	}
	_, err := bw.inserter.Write(bw.buf.Bytes())
	if err == nil {
		bw.buf.Reset()
	}
	return err
}

// Close writer
func (bw *BufInsert) Close() error {
	return bw.Flush()
}
