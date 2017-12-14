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
	kPktLineHeaderLength = 4
)

// A PktLineWriter implements git pkt-line protocol on top of an io.Writer. The
// documentation for the protocol can be found in
// https://github.com/git/git/blob/master/Documentation/technical/protocol-common.txt
type PktLineWriter struct {
	w io.Writer
}

func NewPktLineWriter(w io.Writer) *PktLineWriter {
	return &PktLineWriter{
		w: w,
	}
}

// Flush sends a flush-pkt, which is a special value in the pkt-line protocol.
func (w *PktLineWriter) Flush() error {
	if _, err := w.w.Write([]byte("0000")); err != nil {
		return err
	}
	return nil
}

func (w *PktLineWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func (w *PktLineWriter) WritePktLine(data []byte) error {
	if len(data)+kPktLineHeaderLength > 0x10000 {
		return errors.New("data too long")
	}
	if _, err := w.w.Write([]byte(fmt.Sprintf("%04x", kPktLineHeaderLength+len(data)))); err != nil {
		return err
	}
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	return nil
}

// A PktLineReader implements git pkt-line protocol on top of an io.Reader. The
// documentation for the protocol can be found in
// https://github.com/git/git/blob/master/Documentation/technical/protocol-common.txt
type PktLineReader struct {
	r io.Reader
}

func NewPktLineReader(r io.Reader) *PktLineReader {
	return &PktLineReader{
		r: r,
	}
}

// ReadPktLine returns the next pkt-line. The special value of pkt-flush is
// represented by ErrFlush, to distinguish it from the empty pkt-line.
func (r *PktLineReader) ReadPktLine() ([]byte, error) {
	hexLength := make([]byte, kPktLineHeaderLength)
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
	if length < kPktLineHeaderLength {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, length-kPktLineHeaderLength)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return nil, err
	}
	return data, nil
}
