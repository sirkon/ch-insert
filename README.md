# ch-insert
Clickhouse HTTP interface data inserter.

Clickhouse HTTP RowBinary inserter and special bufferizer. Both these objects implement io.Writer and are intended to be used with the [ch-encode](https://github.com/DenisCheremisov/ch-encode)-produced RowBinary data encoder.

Usage example:
### First create table test and generate encoder using [ch-encode](https://github.com/DenisCheremisov/ch-encode)
```bash
clickhouse-client --query "CREATE TABLE test
    date Date,
    uid String,
    hidden UInt8
) ENGINE = MergeTree(date, (date, uid, hidden), 8192);" # Create table test

go get -u github.com/sirkon/ch-encode
go get -u github.com/sirkon/ch-insert
echo 'uid: UID' > dict.yaml   # We want uid to be represented as UID in Go code

bin/ch-encode --yaml-dict dict.yaml test  # Generate encoder package in current directory
mv test src/                              # and move it to src/ in order for go <cmd> to be able to use it
```

### Usage
```go
// file main.go
package main

import (
	"net/http"
	"test"
	"time"
	"test"

	chinsert "github.com/sirkon/ch-insert"
)

func main() {
	rawInserter := chinsert.New(
		&http.Client{},
		chinsert.ConnParams{
			Host: "localhost",
			Port: 8123,
		},
		"test")

	inserter := chinsert.NewBuf(rawInserter, 10*1024*1024) // 10Mb buffer
	defer inserter.Close()
	encoder := test.NewTestRawEncoder(inserter)
	if err := encoder.Encode(test.Date.FromTime(time.Now()), test.UID("123"), test.Hidden(1)); err != nil {
		panic(err)
	}
	if err := encoder.Encode(test.Date.FromTime(time.Now()), test.UID("123"), test.Hidden(0)); err != nil {
		panic(err)
	}
}
```

Run it:
```bash
go run main.go
```

And see data in clickhouse test table:
```
SELECT *
FROM test

┌──────date─┬─uid─┬──hidden─┐
│ 2017-07-15 │ 123 │       0 │
│ 2017-07-15 │ 123 │       1 │
└───────────┴─────┴────────┘

2 rows in set. Elapsed: 0.004 sec.
```
