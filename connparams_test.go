package chinsert

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnParams(t *testing.T) {
	var url string
	var p ConnParams
	var r ConnParams
	var err error

	url = "user:password@host:8123/default"
	p = ConnParams{
		User:     "user",
		Password: "password",
		Host:     "host",
		Port:     8123,
		DBName:   "default",
	}
	r, err = ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, p, r)
	require.Equal(t, url, r.String())

	url = ":password@host:8123/default"
	_, err = ParseURL(url)
	require.Error(t, err)

	url = "user:password@host/default"
	_, err = ParseURL(url)
	require.Error(t, err)

	url = "user@host:8123/default"
	p = ConnParams{
		User:   "user",
		Host:   "host",
		Port:   8123,
		DBName: "default",
	}
	r, err = ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, p, r)
	require.Equal(t, url, r.String())

	url = "host:8123"
	p = ConnParams{
		Host: "host",
		Port: 8123,
	}
	r, err = ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, p, r)
	require.Equal(t, url, r.String())
}
