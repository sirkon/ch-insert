package chinsert

import (
	"fmt"
	"net/http"
	"sync"
)

// global EpochDirect
var globalEpoch *EpochDirect
var onceGlobalEcho sync.Once

// Open is a shortcut to create SmartInsert writer over the http
//   insertURL clickhouse access URL including user, password, host, port and database name
//   table table name
//   softLimit sets a buffer size where the writer will try to flush data when the amount of data written surpasses
//             the value but then and only then when the last write happened more than a second ago
//   hardLimit sets absolute buffer size where the flush will perform immediately after the buffer grows higher
// Example:
//
// ...
// epoch := NewEpochDirect()
// inserter, err := chinsert.Open("localhost:8123", "test", 10*1024*1024, 1024*1024*1024)
// if err != nil {
// 	panic(err)
// }
// encoder := test.NewTestingRawEncoder(inserter)
// ...
func Open(url, table string, softLimit, hardLimit int) (*SmartInsert, error) {
	params, err := ParseURL(url)
	if err != nil {
		return nil, err
	}
	if softLimit <= 0 {
		return nil, fmt.Errorf("softlimit must be greater than 0, got %d", softLimit)
	}
	if softLimit > hardLimit {
		return nil, fmt.Errorf(
			"hardLimit must be greater than softLimit, got softLimit=%d, hardLimit=%d",
			softLimit,
			hardLimit,
		)
	}
	onceGlobalEcho.Do(func() {
		globalEpoch = NewEpochDirect()
	})

	rawInsert := New(&http.Client{}, params, table)
	buf := NewBuf(rawInsert, hardLimit)
	res := NewSmartInsert(buf, softLimit, globalEpoch)
	return res, nil
}
