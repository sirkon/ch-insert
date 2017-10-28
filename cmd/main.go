package main

import (
	"test"

	chinsert "github.com/sirkon/ch-insert"
)

func main() {
	inserter, err := chinsert.Open("localhost:8123/default", "test", 10*1024*1024, 1024*1024*1024)
	if err != nil {
		panic(err)
	}
	defer inserter.Close()
	epoch := chinsert.NewEpochDirect()
	encoder := test.NewTestRawEncoder(inserter)
	for i := 0; i < 100000000; i++ {
		if err := encoder.Encode(test.Date.FromTimestamp(epoch.Seconds()), test.UID("123"), test.Hidden(0)); err != nil {
			panic(err)
		}
	}
}
