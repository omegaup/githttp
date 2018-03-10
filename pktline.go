package githttp

import (
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	// ErrFlush is returned whtn the client sends an explicit flush packet.
	ErrFlush = errors.New("flush")
)

const (
	pktLineHeaderLength = 4
)

// A PktLineWriter implements git pkt-line protocol on top of an io.Writer. The
// documentation for the protocol can be found in
// https://github.com/git/git/blob/master/Documentation/technical/protocol-common.txt
type PktLineWriter struct {
	w io.Writer
}

// NewPktLineWriter creates a new pkt-line based on the supplied Writer.
func NewPktLineWriter(w io.Writer) *PktLineWriter {
	return &PktLineWriter{
		w: w,
	}
}

// Flush sends a flush-pkt, which is a special value in the pkt-line protocol.
func (w *PktLineWriter) Flush() error {
	_, err := w.w.Write([]byte("0000"))
	return err
}

// Close sends a flush-pkt.
func (w *PktLineWriter) Close() error {
	return w.Flush()
}

// WritePktLine sends one pkt-line.
func (w *PktLineWriter) WritePktLine(data []byte) error {
	if len(data)+pktLineHeaderLength > 0x10000 {
		return errors.New("data too long")
	}
	if _, err := w.w.Write([]byte(fmt.Sprintf("%04x", pktLineHeaderLength+len(data)))); err != nil {
		return err
	}
	_, err := w.w.Write(data)
	return err
}

// A PktLineReader implements git pkt-line protocol on top of an io.Reader. The
// documentation for the protocol can be found in
// https://github.com/git/git/blob/master/Documentation/technical/protocol-common.txt
type PktLineReader struct {
	r io.Reader
}

// NewPktLineReader creates a new pkt-line based on the supplied Reader.
func NewPktLineReader(r io.Reader) *PktLineReader {
	return &PktLineReader{
		r: r,
	}
}

// ReadPktLine returns the next pkt-line. The special value of pkt-flush is
// represented by ErrFlush, to distinguish it from the empty pkt-line.
func (r *PktLineReader) ReadPktLine() ([]byte, error) {
	hexLength := make([]byte, pktLineHeaderLength)
	if _, err := io.ReadFull(r.r, hexLength); err != nil {
		return nil, err
	}
	length, err := strconv.ParseUint(string(hexLength), 16, 16)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, ErrFlush
	}
	if length < pktLineHeaderLength {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, length-pktLineHeaderLength)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// PktLineResponse represents an expected entry from PktLineReader.
type PktLineResponse struct {
	Line string
	Err  error
}

// ComparePktLine compares what is being read from the supplied Reader when
// interpreted by a PktLineReader against an expected list of PktLineResponses.
func ComparePktLineResponse(
	r io.Reader,
	expectedResponse []PktLineResponse,
) ([]PktLineResponse, bool) {
	reader := NewPktLineReader(r)

	actual := make([]PktLineResponse, 0)
	ok := true
	for _, expected := range expectedResponse {
		line, err := reader.ReadPktLine()
		actual = append(actual, PktLineResponse{string(line), err})
		if expected.Err != err {
			ok = false
		}
		if expected.Err == nil && expected.Line != string(line) {
			ok = false
		}
	}
	return actual, ok
}
