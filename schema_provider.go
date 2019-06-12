package chinsert

import (
	"io"
)

// Column represents a column of clickhouse table
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// WriterWithSchemaCheck writer with schema check mismatch functionality
type WriterWithSchemaCheck interface {
	io.Writer
	Schema() ([]Column, error)
}
