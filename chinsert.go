package chinsert

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

// ConnParams client connection parameters
type ConnParams struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// CHInsert inserts data into the given clickhouse table via the HTTP interface
type CHInsert struct {
	client *http.Client
	url    string
}

// NewCHInsert constructor
func NewCHInsert(client *http.Client, params ConnParams, table string) *CHInsert {
	r, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/", params.Host, params.Port), nil)
	if err != nil {
		panic(err)
	}
	q := r.URL.Query()
	if len(params.User) > 0 {
		q.Set("user", params.User)
	}
	if len(params.Password) > 0 {
		q.Set("password", params.Password)
	}
	if len(params.DBName) > 0 {
		q.Set("database", params.DBName)
	}
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	r.URL.RawQuery = q.Encode()
	return &CHInsert{
		client: client,
		url:    r.URL.String(),
	}
}

func (c *CHInsert) Write(p []byte) (n int, err error) {
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
