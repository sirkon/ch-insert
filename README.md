# ch-insert
Clickhouse HTTP interface data inserter.

Two types for Clickhouse data insertion defined. One is inserting collected data via the HTTP interface as RowBinary and the other is bufferizer with guaranties on data integrity. These two methods both implemented io.Writer and are intended to be used with the [ch-encode](https://github.com/DenisCheremisov/ch-encode) produced RowBinary data encoder.

Usage example:
### First create table test and generate encoder using [ch-encode](https://github.com/DenisCheremisov/ch-encode)
```bash
clickhouse-client --query "CREATE TABLE test
    date Date, 
    uid String, 
    hidden UInt8
) ENGINE = MergeTree(date, (date, uid, hidden), 8192);" # Create table test
    
mkdir test
cd test
export GOPATH=`pwd`
go get -u github.com/DenisCheremisov/ch-encode
go get -u github.com/DenisCheremisov/ch-insert
echo 'uid: UID' > dict.yaml   # We want uid to be represented as UID in Go code
    
bin/ch-encode --yaml-dict dict.yaml test  # Generate encoder package in current directory
mv test src/                              # and move it to src/ in order for go <cmd> to be able to use it
go install test                           # install package
```

### Usage
```go
package main

import (
	"net/http"
	"test"
	"time"

	chinsert "github.com/DenisCheremisov/ch-insert"
)

func main() {
	rawInserter := chinsert.NewCHInsert(
		&http.Client{},
		chinsert.ConnParams{
			Host: "localhost",
			Port: 8123,
		},
		"test")

	inserter := chinsert.NewBufInsert(rawInserter, 10*1024*1024)
	defer inserter.Close()
	encoder := test.NewTestRawEncoder(inserter)
	if err := encoder.Encode(test.Date.FromTime(time.Now()), []byte("123"), 1); err != nil {
		panic(err)
	}
	if err := encoder.Encode(test.Date.FromTime(time.Now()), []byte("123"), 0); err != nil {
		panic(err)
	}
}
```

Run it:
```bash
go install main
bin/main
```

And see data in clickhouse test table:
```
SELECT *
FROM test 

┌───────date─┬─uid─┬─hidden─┐
│ 2017-07-15  │ 123 │      0 │
│ 2017-07-15  │ 123 │      1 │
└────────────┴─────┴───────┘

2 rows in set. Elapsed: 0.004 sec. ```
