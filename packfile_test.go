package githttp

import (
	git "github.com/lhchavez/git2go"
	"io/ioutil"
	"os"
	"testing"
)

const (
	packFilename  = "testdata/repo.git/objects/pack/pack-3915156951f90b8239a1d1933cbe85ae1bc7457f.pack"
	indexFilename = "testdata/repo.git/objects/pack/pack-3915156951f90b8239a1d1933cbe85ae1bc7457f.idx"
)

func testParsedIndex(t *testing.T, idx *PackfileIndex) {
	if 3 != len(idx.Entries) {
		t.Errorf("Expected 3 entries, got %v", len(idx.Entries))
	}

	// The entries in the index are sorted.
	entries := []struct {
		hash       string
		size       uint64
		objectType git.ObjectType
	}{
		{"417c01c8795a35b8e835113a85a5c0c1c77f67fb", 33, git.ObjectTree},
		{"88aa3454adb27c3c343ab57564d962a0a7f6a3c1", 170, git.ObjectCommit},
		{"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", 0, git.ObjectBlob},
	}
	for i, entry := range entries {
		if entry.hash != idx.Entries[i].Oid.String() {
			t.Errorf("Entry %d hash mismatch: expected %v, got %v", i, entry.hash, idx.Entries[i].Oid)
		}
		if entry.size != idx.Entries[i].Size {
			t.Errorf("Entry %d size mismatch: expected %v, got %v", i, entry.size, idx.Entries[i].Size)
		}
		if entry.objectType != idx.Entries[i].Type {
			t.Errorf("Entry %d type mismatch: expected %v, got %v", i, entry.objectType, idx.Entries[i].Type)
		}
	}
}

func TestParseIndex(t *testing.T) {
	odb, err := git.NewOdb()
	if err != nil {
		t.Fatalf("Failed to create odb: %v", err)
	}
	defer odb.Free()

	backend, err := git.NewOdbBackendOnePack(packFilename)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	if err := odb.AddAlternate(backend, 1); err != nil {
		t.Fatalf("Failed to add backend: %v", err)
	}

	idx, err := ParseIndex(indexFilename, odb)
	if err != nil {
		t.Errorf("Failed to parse the index: %v", err)
	}

	testParsedIndex(t, idx)
}

func TestUnpackPackfile(t *testing.T) {
	dir, err := ioutil.TempDir("", "packfile_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	odb, err := git.NewOdb()
	if err != nil {
		t.Fatalf("Failed to create odb: %v", err)
	}
	defer odb.Free()

	f, err := os.Open(packFilename)
	if err != nil {
		t.Fatalf("Failed to open the index file: %v", err)
	}
	defer f.Close()

	idx, _, err := UnpackPackfile(odb, f, dir, nil)
	if err != nil {
		t.Fatalf("Failed to unpack packfile: %v", err)
	}

	testParsedIndex(t, idx)
}
