package main

import (
	"net/http"
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
		"test", // Table name to insert data in
	)

	epoch := chinsert.NewEpochDirect()
	inserter := chinsert.NewBuf(rawInserter, 1024*1024*1024) // 1Gb buffer is hard limit for insertion
	defer inserter.Close()

	si := chinsert.NewSmartInsert(inserter, 10*1024*1024, epoch)
	encoder := test.NewTestRawEncoder(si)
	for i := 0; i < 100000000; i++ {
		if err := encoder.Encode(test.Date.FromTimestamp(epoch.Seconds()), test.UID("123"), test.Hidden(0)); err != nil {
			panic(err)
		}
	}
}
