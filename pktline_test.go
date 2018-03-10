package githttp

import (
	"bytes"
	"io"
	"testing"
)

func TestPktLineWriter(t *testing.T) {
	var buf bytes.Buffer

	writer := NewPktLineWriter(&buf)
	writer.WritePktLine([]byte("hello"))
	writer.Flush()
	writer.WritePktLine([]byte(""))
	writer.Close()

	expected := []byte("0009hello" + // first pkt-line
		"0000" + // flush pkt
		"0004" + // empty pkt
		"0000") // flush pkt sent by Close()
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("pkt-writer expected %q, got %q", expected, buf.Bytes())
	}
}

func TestPktLineReader(t *testing.T) {
	buf := bytes.NewBuffer([]byte("0009hello" + // first pkt-line
		"0000" + // flush pkt
		"0004")) // empty pkt

	expected := []PktLineResponse{
		{"hello", nil},
		{"", ErrFlush},
		{"", nil},
		{"", io.EOF},
	}
	if actual, ok := ComparePktLineResponse(
		buf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}
