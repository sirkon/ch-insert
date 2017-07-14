# ch-insert
Clickhouse HTTP interface data inserter.

Two types for Clickhouse data insertion defined. One is inserting collected data via the HTTP interface as RowBinary and the other is bufferizer with guaranties on data integrity. These two methods are intended to be used with the [encoder](https://github.com/DenisCheremisov/ch-encode) (both implement io.Writer interface)

Usage example:
```go
rawInserter := chinsert.NewCHInsert(
	&http.Client{},
	chinsert.ConnParams{
		Host: "localhost",
		Port: 8123,
	},
	"test")
inserter := chinert.NewBufInsert(rawInserter, 10*1024*1024)
```
