package chinsert

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

var _ WriterWithSchemaCheck = &Insert{}

// Insert inserts data into the given clickhouse table via the HTTP interface
type Insert struct {
	client    *http.Client
	insertURL string
	schemaURL string
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
	insertURL := r.URL.String()
	q.Set("query", fmt.Sprintf("SELECT name, type FROM system.columns WHERE table = '%s' FORMAT JSONEachRow", table))
	r.URL.RawQuery = q.Encode()
	schemaURL := r.URL.String()
	return &Insert{
		client:    client,
		insertURL: insertURL,
		schemaURL: schemaURL,
	}
}

func (c *Insert) Write(p []byte) (n int, err error) {
	request, err := http.NewRequest("POST", c.insertURL, bytes.NewBuffer(p))
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

// Schema returns list of columns of this inserter's clickhouse table
func (c *Insert) Schema() (res []Column, err error) {
	resp, err := http.Get(c.schemaURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %s", err)
	}
	defer func() {
		if cErr := resp.Body.Close(); cErr != nil {
			if err == nil {
				err = cErr
			}
		}
	}()
	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read out error schema response (status %d): %s", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("failed to get a schema: %s (%s)", string(data), err)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var col Column
		if err := json.Unmarshal(scanner.Bytes(), &col); err != nil {
			return nil, fmt.Errorf("failed to read out schema response: %s", err)
		}
		res = append(res, col)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to reao out schema response: %s", err)
	}

	return res, nil
}
