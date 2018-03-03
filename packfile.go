package githttp

import (
	"errors"
	"fmt"
	git "github.com/lhchavez/git2go"
	"io"
	"os"
)

const (
	indexFileMagic  = 0xff744f63
	packFileVersion = 2
	msb32           = 0x80000000
)

var (
	// ErrInvalidMagic is returned when the index file does not start with the
	// magic header.
	ErrInvalidMagic = errors.New("bad pack header")

	// ErrInvalidVersion is returned when the index file does not have the
	// expected version (2).
	ErrInvalidVersion = errors.New("bad pack version")

	// ErrLargePackfile is returned when an offset in a packfile would overflow a
	// 32-bit signed integer.
	ErrLargePackfile = errors.New("packfile too large")
)

// A PackfileIndex represents the contents of an .idx file.
type PackfileIndex struct {
	Fanout  [256]uint32
	Entries []PackfileEntry
}

// A PackfileEntry represents one of the entries in an .idx file.
type PackfileEntry struct {
	Oid    git.Oid
	CRC    uint32
	Offset uint64
	Size   uint64
	Type   git.ObjectType
}

// readUInt32 reads 4 bytes from the supplied Reader and interprets them as a
// network-byte-order uint32.
func readUInt32(r io.Reader) (uint32, error) {
	data := make([]byte, 4)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, err
	}
	result := uint32(data[0])<<24 |
		uint32(data[1])<<16 |
		uint32(data[2])<<8 |
		uint32(data[3])
	return result, nil
}

// ParseIndex parses the index located at the supplied filename and returns its
// contents as a PackfileIndex. The format for this file is documented in
// https://github.com/git/git/blob/master/Documentation/technical/pack-format.txt
func ParseIndex(filename string, odb *git.Odb) (*PackfileIndex, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if magic, err := readUInt32(f); err != nil || magic != indexFileMagic {
		return nil, ErrInvalidMagic
	}
	if version, err := readUInt32(f); err != nil || version != packFileVersion {
		return nil, ErrInvalidVersion
	}

	// The index file starts with an array of 256 integers, which represent the
	// number of objects contained in the packfile whose hash start with each of
	// the 256 possible bytes.
	index := &PackfileIndex{}
	for i := 0; i < 256; i++ {
		if index.Fanout[i], err = readUInt32(f); err != nil {
			return nil, err
		}
	}
	index.Entries = make([]PackfileEntry, index.Fanout[255])

	// Next come the sorted OIDs for all the objects in the packfile.
	for i := 0; i < len(index.Entries); i++ {
		if _, err = f.Read(index.Entries[i].Oid[:]); err != nil {
			return nil, err
		}
		// Sizes and types are obtained from the object database.
		index.Entries[i].Size, index.Entries[i].Type, err = odb.ReadHeader(
			&index.Entries[i].Oid,
		)
		if err != nil {
			return nil, err
		}
	}

	// Afterwards, the CRC checksums of all the entries.
	for i := 0; i < len(index.Entries); i++ {
		if index.Entries[i].CRC, err = readUInt32(f); err != nil {
			return nil, err
		}
	}

	// Next, the offsets of all entries.
	for i := 0; i < len(index.Entries); i++ {
		offset, err := readUInt32(f)
		if err != nil {
			return nil, err
		}
		if offset&msb32 != 0 {
			return nil, ErrLargePackfile
		}
		index.Entries[i].Offset = uint64(offset)
	}

	// Large packfiles have an additional table of 8-byte offset entries. We
	// don't support those, so we don't even bother reading that table.
	// Finally, the SHA-1 hash of the whole index comes, but we trust that
	// libgit2 has done the right thing.

	return index, nil
}

// UnpackPackfile parses the packfile, ensures that the it is valid, creates an
// index file in the specified directory, and returns the path of the packfile.
func UnpackPackfile(
	odb *git.Odb,
	r io.Reader,
	dir string,
	progressCallback func(git.TransferProgress) git.ErrorCode,
) (*PackfileIndex, string, error) {
	if progressCallback == nil {
		progressCallback = func(stats git.TransferProgress) git.ErrorCode {
			return git.ErrorCode(0)
		}
	}

	// The indexer will parse the packfile and create an index file.
	indexer, err := git.NewIndexer(
		dir,
		odb,
		progressCallback,
	)
	if err != nil {
		return nil, "", err
	}
	defer indexer.Free()
	_, err = io.Copy(indexer, r)
	if err != nil {
		return nil, "", errors.New("eof")
	}
	hash, err := indexer.Commit()
	if err != nil {
		return nil, "", err
	}

	// With the index file, we can inspect the contents of the packfile.
	indexPath := fmt.Sprintf("%s/pack-%s.idx", dir, hash)
	backend, err := git.NewOdbBackendOnePack(indexPath)
	if err != nil {
		return nil, "", err
	}
	if err := odb.AddAlternate(backend, 1); err != nil {
		return nil, "", err
	}
	index, err := ParseIndex(indexPath, odb)
	if err != nil {
		return nil, "", err
	}
	for _, entry := range index.Entries {
		switch entry.Type {
		case git.ObjectCommit:
		case git.ObjectTree:
		case git.ObjectBlob:
			// This is fine.
		default:
			return nil, "", errors.New("object-type-unallowed")
		}
	}

	return index, fmt.Sprintf("%s/pack-%s.pack", dir, hash), nil
}
