package chinsert

import (
	"sync/atomic"
	"time"
)

var _ WriterWithSchemaCheck = &SmartInsert{}

// Epoch returns UNIX epoch time
type Epoch interface {
	Seconds() int64
}

// EpochDirect ...
type EpochDirect int64

// NewEpochDirect sets up new EpochDirect. It is thread safe and should be reused
// It is designed to run forever, although it would be easy to add force stop (via context, for instance)
// capability if needed.
func NewEpochDirect() *EpochDirect {
	var count EpochDirect
	count = EpochDirect(time.Now().Unix())
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			t := <-ticker.C
			atomic.StoreInt64((*int64)(&count), t.Unix())
		}
	}()
	return &count
}

// Seconds implementation
func (e *EpochDirect) Seconds() int64 {
	return atomic.LoadInt64((*int64)(e))
}

// SmartInsert is the insertion method that tries not to insert more than 1 time per second to decrease
// a stress on Clickhouse server.
// It has soft limit capacity which it tries to hold but once the previous flush was less than a second ago
// it can grow more and up to the hard limit which is set in the underlying BufInsert object
type SmartInsert struct {
	BufInsert
	softLimit int
	prevTick  int64
	epoch     Epoch
}

// NewSmartInsert constructor
func NewSmartInsert(insert *BufInsert, softLimit int, epoch Epoch) *SmartInsert {
	return &SmartInsert{
		BufInsert: *insert,
		softLimit: softLimit,
		epoch:     epoch,
		prevTick:  epoch.Seconds(),
	}
}

// Write implementation
func (si *SmartInsert) Write(p []byte) (n int, err error) {
	if len(p)+si.BufInsert.buf.Len() > si.softLimit {
		ct := si.epoch.Seconds()
		if ct > si.prevTick {
			err = si.BufInsert.Flush()
			if err != nil {
				return -1, err
			}
			si.prevTick = ct
		}
	}
	return si.BufInsert.Write(p)
}
