package chinsert

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufInsert(t *testing.T) {
	under := &bytes.Buffer{}
	buf := NewBuf(under, 5)
	const alphabet = "abcdefghijklmnopqrstuvwxyz\n"
	io.WriteString(buf, alphabet)
	io.WriteString(buf, alphabet)
	io.WriteString(buf, alphabet)
	buf.Close()
	require.Equal(t, strings.Repeat(alphabet, 3), under.String())
}
