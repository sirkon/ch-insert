package chinsert

import (
	"sync"
)

// MultiInsertLock is designed to take insertion from several sources with low concurrency level, so thus
// lock will likely be cheaper than a queue of tasks to write (these will need allocations or excessive copying.
// These are actually avoidable at the cost of increased complexity of writers)
type MultiInsertLock struct {
	buf  *BufInsert
	lock sync.Locker
}

// NewMultiInsertLock is a MultiInsertLock constructor.
// Parameters:
//   buf
func NewMultiInsertLock(buf *BufInsert) *MultiInsertLock {
	return &MultiInsertLock{
		buf:  buf,
		lock: &sync.Mutex{},
	}
}

// Write implementation for io.Writer
func (mi *MultiInsertLock) Write(p []byte) (n int, err error) {
	mi.lock.Lock()
	n, err = mi.buf.Write(p)
	mi.lock.Unlock()
	return
}

// Flush implementation for io.Flusher
func (mi *MultiInsertLock) Flush() error {
	mi.lock.Lock()
	err := mi.buf.Flush()
	mi.lock.Unlock()
	return err
}

// Close implementation for io.Closer
func (mi *MultiInsertLock) Close() error {
	mi.lock.Lock()
	err := mi.buf.Close()
	mi.lock.Unlock()
	return err
}
