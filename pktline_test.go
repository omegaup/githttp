package githttp

import (
	"bytes"
	"io"
	"testing"
)

type expectedPktLine struct {
	err  error
	line string
}

func comparePktLineResponse(t *testing.T, pktLines []expectedPktLine, r io.Reader) bool {
	reader := NewPktLineReader(r)

	success := true
	for idx, expected := range pktLines {
		line, err := reader.ReadPktLine()
		if err != expected.err {
			t.Errorf("line %d: expected err %q, got %q", idx, expected.err, err)
			success = false
		}
		if err == nil && !bytes.Equal(line, []byte(expected.line)) {
			t.Errorf("line %d: expected line %q, got %q", idx, string(expected.line), line)
			success = false
		}
	}
	return success
}

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

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "hello"},
			{ErrFlush, ""},
			{nil, ""},
			{io.EOF, ""},
		},
		buf,
	)
}
