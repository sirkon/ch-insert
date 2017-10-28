package chinsert

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

// Insert inserts data into the given clickhouse table via the HTTP interface
type Insert struct {
	client *http.Client
	url    string
}

// New constructor
func New(client *http.Client, params ConnParams, table string) *Insert {
	r, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/", params.Host, params.Port), nil)
	if err != nil {
		panic(err)
	}
	q := r.URL.Query()
	if len(params.User) > 0 {
		q.Set("user", params.User)
	}
	if len(params.Password) > 0 {
		q.Set("password", string(params.Password))
	}
	if len(params.DBName) > 0 {
		q.Set("database", params.DBName)
	}
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	r.URL.RawQuery = q.Encode()
	return &Insert{
		client: client,
		url:    r.URL.String(),
	}
}

func (c *Insert) Write(p []byte) (n int, err error) {
	request, err := http.NewRequest("POST", c.url, bytes.NewBuffer(p))
	if err != nil {
		return -1, err
	}
	resp, err := c.client.Do(request)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}
	if resp.StatusCode != http.StatusOK {
		return -1, errors.New(string(data))
	}
	return len(p), nil
}
