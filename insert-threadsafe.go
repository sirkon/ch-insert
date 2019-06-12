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

// Flush ...
func (w *ThreadSafeInsert) Flush() error {
	w.lock.Lock()
	err := w.inserter.Flush()
	w.lock.Unlock()
	return err
}

// Close ...
func (w *ThreadSafeInsert) Close() error {
	w.lock.Lock()
	err := w.inserter.Close()
	w.lock.Unlock()
	return err
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
