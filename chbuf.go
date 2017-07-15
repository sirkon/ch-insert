package chinsert

import (
	"bytes"
	"fmt"
)

// BufInsert takes care of data integrity which is critical for the clickhouse insert task.
// We cannot insert half of the clickhouse record, so each *Encoder output must be kept
// unsplitted
type BufInsert struct {
	limit    int
	buf      *bytes.Buffer
	inserter *CHInsert
}

// NewBufInsert constructor
func NewBufInsert(inserter *CHInsert, limit int) *BufInsert {
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
		bw.buf.Reset()
	}
	return bw.buf.Write(p)
}

// Flush data
func (bw *BufInsert) Flush() error {
	if bw.buf.Len() == 0 {
		return nil
	}
	_, err := bw.inserter.Write(bw.buf.Bytes())
	return err
}
