package chinsert

import (
	"sync"
)

var _ WriterWithSchemaCheck = &ThreadSafeInsert{}

// ThreadSafeInsert thread safe insertion primitive
type ThreadSafeInsert struct {
	inserter *SmartInsert
	lock     sync.RWMutex
}

func (w *ThreadSafeInsert) Write(p []byte) (n int, err error) {
	w.lock.Lock()
	n, err = w.inserter.Write(p)
	w.lock.Unlock()
	return
}

// Schema ...
func (w *ThreadSafeInsert) Schema() ([]Column, error) {
	return w.inserter.Schema()
}
