package githttp

import (
	"bytes"
	"errors"
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func gitOid(hash string) git.Oid {
	oid, err := git.NewOid(hash)
	if err != nil {
		panic(err)
	}
	return *oid
}

func TestDiscoverReferences(t *testing.T) {
	buf := bytes.NewBuffer([]byte(
		"00a67217a7c7e582c46cec22a130adf4b9d7d950fba0 HEAD\x00symref=HEAD:refs/heads/master multi_ack thin-pack side-band side-band-64k ofs-delta shallow no-progress include-tag\n" +
			"00441d3fcd5ced445d1abc402225c0b8a1299641f497 refs/heads/integration\n" +
			"003f7217a7c7e582c46cec22a130adf4b9d7d950fba0 refs/heads/master\n" +
			"003cb88d2441cac0977faf98efc80305012112238d9d refs/tags/v0.9\n" +
			"003c525128480b96c89e6418b1e40909bf6c5b2d580f refs/tags/v1.0\n" +
			"003fe92df48743b7bc7d26bcaabfddde0a1e20cae47c refs/tags/v1.0^{}\n" +
			"0000"))
	discovery, err := DiscoverReferences(buf)
	if err != nil {
		t.Fatalf("Failed to discover refs: %v %q", err, discovery)
	}
	expectedSymref := "refs/heads/master"
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("expected symref %q, got %q", expectedSymref, discovery.HeadSymref)
	}
	expectedCapabilities := Capabilities{
		"symref=HEAD:refs/heads/master", "multi_ack", "thin-pack", "side-band",
		"side-band-64k", "ofs-delta", "shallow", "no-progress", "include-tag",
	}
	if !expectedCapabilities.Equal(discovery.Capabilities) {
		t.Errorf("expected capabilities %q, got %q", expectedCapabilities, discovery.Capabilities)
	}
	expectedHash := "7217a7c7e582c46cec22a130adf4b9d7d950fba0"
	headReference := discovery.References["HEAD"]
	if expectedHash != headReference.String() {
		t.Errorf("expected hash of HEAD %q, got %q", expectedHash, headReference.String())
	}
}

func TestHandlePrePullRestricted(t *testing.T) {
	var buf bytes.Buffer
	log := log15.New()
	err := handlePrePull("testdata/repo.git", AuthorizationAllowedRestricted, log, &buf)
	if err != nil {
		t.Errorf("Failed to get pre-pull: %v", err)
	}
	discovery, err := DiscoverReferences(&buf)
	if err != nil {
		t.Errorf("Failed to parse the reference discovery: %v", err)
	}
	expectedSymref := "refs/heads/master"
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("Expected %v, got %v", expectedSymref, discovery.HeadSymref)
	}
	expectedReferences := map[string]git.Oid{
		"HEAD":              gitOid("6d2439d2e920ba92d8e485e75d1b740ae51b609a"),
		"refs/heads/master": gitOid("6d2439d2e920ba92d8e485e75d1b740ae51b609a"),
	}
	if !reflect.DeepEqual(expectedReferences, discovery.References) {
		t.Errorf("Expected %v, got %v", expectedReferences, discovery.References)
	}
}

func TestHandlePrePull(t *testing.T) {
	var buf bytes.Buffer
	log := log15.New()
	err := handlePrePull("testdata/repo.git", AuthorizationAllowed, log, &buf)
	if err != nil {
		t.Errorf("Failed to get pre-pull: %v", err)
	}
	discovery, err := DiscoverReferences(&buf)
	if err != nil {
		t.Errorf("Failed to parse the reference discovery: %v", err)
	}
	expectedSymref := "refs/heads/master"
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("Expected %v, got %v", expectedSymref, discovery.HeadSymref)
	}
	expectedReferences := map[string]git.Oid{
		"HEAD":              gitOid("6d2439d2e920ba92d8e485e75d1b740ae51b609a"),
		"refs/heads/master": gitOid("6d2439d2e920ba92d8e485e75d1b740ae51b609a"),
		"refs/meta/config":  gitOid("d0c442210b72c207637a63e4eda991bc27abc0bd"),
	}
	if !reflect.DeepEqual(expectedReferences, discovery.References) {
		t.Errorf("Expected %v, got %v", expectedReferences, discovery.References)
	}
}

func TestHandlePrePush(t *testing.T) {
	var buf bytes.Buffer
	log := log15.New()
	err := handlePrePush("testdata/repo.git", AuthorizationAllowed, log, &buf)
	if err != nil {
		t.Errorf("Failed to get pre-push: %v", err)
	}
	discovery, err := DiscoverReferences(&buf)
	if err != nil {
		t.Errorf("Failed to parse the reference discovery: %v", err)
	}
	expectedSymref := ""
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("Expected %v, got %v", expectedSymref, discovery.HeadSymref)
	}
	expectedReferences := map[string]git.Oid{
		"refs/heads/master": gitOid("6d2439d2e920ba92d8e485e75d1b740ae51b609a"),
		"refs/meta/config":  gitOid("d0c442210b72c207637a63e4eda991bc27abc0bd"),
	}
	if !reflect.DeepEqual(expectedReferences, discovery.References) {
		t.Errorf("Expected %v, got %v", expectedReferences, discovery.References)
	}
}

func TestHandleEmptyPrePull(t *testing.T) {
	var buf bytes.Buffer
	log := log15.New()
	err := handlePrePull("testdata/empty.git", AuthorizationAllowed, log, &buf)
	if err != nil {
		t.Errorf("Failed to get pre-pull: %v", err)
	}
	discovery, err := DiscoverReferences(&buf)
	if err != nil {
		t.Errorf("Failed to parse the reference discovery: %v", err)
	}
	expectedSymref := ""
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("Expected %v, got %v", expectedSymref, discovery.HeadSymref)
	}
	expectedReferences := map[string]git.Oid{}
	if !reflect.DeepEqual(expectedReferences, discovery.References) {
		t.Errorf("Expected %v, got %v", expectedReferences, discovery.References)
	}
}

func TestHandleEmptyPrePush(t *testing.T) {
	var buf bytes.Buffer
	log := log15.New()
	err := handlePrePush("testdata/empty.git", AuthorizationAllowed, log, &buf)
	if err != nil {
		t.Errorf("Failed to get pre-push: %v", err)
	}
	discovery, err := DiscoverReferences(&buf)
	if err != nil {
		t.Errorf("Failed to parse the reference discovery: %v", err)
	}
	expectedSymref := ""
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("Expected %v, got %v", expectedSymref, discovery.HeadSymref)
	}
	expectedReferences := map[string]git.Oid{
		"capabilities^{}": gitOid("0000000000000000000000000000000000000000"),
	}
	if !reflect.DeepEqual(expectedReferences, discovery.References) {
		t.Errorf("Expected %v, got %v", expectedReferences, discovery.References)
	}
}

func TestHandlePullUnknownRef(t *testing.T) {
	var inBuf, outBuf bytes.Buffer

	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("want 0000000000000000000000000000000000000000 thin-pack ofs-delta agent=git/2.14.1\n"))
	pw.Flush()
	pw.WritePktLine([]byte("done"))

	log := log15.New()
	err = handlePull("testdata/repo.git", AuthorizationAllowed, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "ERR upload-pack: not our ref 0000000000000000000000000000000000000000"},
			{io.EOF, ""},
		},
		&outBuf,
	)
}

func TestHandleClone(t *testing.T) {
	var inBuf, outBuf bytes.Buffer

	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
	pw.Flush()
	pw.WritePktLine([]byte("done"))

	log := log15.New()
	err = handlePull("testdata/repo.git", AuthorizationAllowed, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	if !comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "NAK\n"},
		},
		&outBuf,
	) {
		return
	}

	odb, err := git.NewOdb()
	if err != nil {
		t.Fatalf("Failed to create odb: %v", err)
	}
	defer odb.Free()

	idx, _, err := UnpackPackfile(odb, &outBuf, dir, nil)
	if err != nil {
		t.Fatalf("Failed to unpack packfile: %v", err)
	}

	entries := []struct {
		hash       string
		size       uint64
		objectType git.ObjectType
	}{
		{"06f8815b4dc1ba5cabf619d8a8ef392d0f88a2f1", 71, git.ObjectTree},
		{"417c01c8795a35b8e835113a85a5c0c1c77f67fb", 33, git.ObjectTree},
		{"6d2439d2e920ba92d8e485e75d1b740ae51b609a", 217, git.ObjectCommit},
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

func TestHandlePull(t *testing.T) {
	var inBuf, outBuf bytes.Buffer

	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
	pw.Flush()
	pw.WritePktLine([]byte("have 88aa3454adb27c3c343ab57564d962a0a7f6a3c1\n"))
	pw.WritePktLine([]byte("done"))

	log := log15.New()
	err = handlePull("testdata/repo.git", AuthorizationAllowed, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	if !comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "ACK 88aa3454adb27c3c343ab57564d962a0a7f6a3c1\n"},
		},
		&outBuf,
	) {
		return
	}

	odb, err := git.NewOdb()
	if err != nil {
		t.Fatalf("Failed to create odb: %v", err)
	}
	defer odb.Free()

	idx, _, err := UnpackPackfile(odb, &outBuf, dir, nil)
	if err != nil {
		t.Fatalf("Failed to unpack packfile: %v", err)
	}

	entries := []struct {
		hash       string
		size       uint64
		objectType git.ObjectType
	}{
		{"06f8815b4dc1ba5cabf619d8a8ef392d0f88a2f1", 71, git.ObjectTree},
		{"6d2439d2e920ba92d8e485e75d1b740ae51b609a", 217, git.ObjectCommit},
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

func TestHandleCloneShallowNegotiation(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
	pw.WritePktLine([]byte("deepen 1"))
	pw.Flush()

	log := log15.New()
	err = handlePull("testdata/repo.git", AuthorizationAllowed, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	if !comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "shallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"},
			{ErrFlush, ""},
			{io.EOF, ""},
		},
		&outBuf,
	) {
		return
	}
}

func TestHandleCloneShallowClone(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
	pw.WritePktLine([]byte("deepen 1"))
	pw.Flush()
	pw.WritePktLine([]byte("done"))

	log := log15.New()
	err = handlePull("testdata/repo.git", AuthorizationAllowed, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	if !comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "shallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"},
			{ErrFlush, ""},
			{nil, "NAK\n"},
		},
		&outBuf,
	) {
		return
	}

	odb, err := git.NewOdb()
	if err != nil {
		t.Fatalf("Failed to create odb: %v", err)
	}
	defer odb.Free()

	idx, _, err := UnpackPackfile(odb, &outBuf, dir, nil)
	if err != nil {
		t.Fatalf("Failed to unpack packfile: %v", err)
	}

	entries := []struct {
		hash       string
		size       uint64
		objectType git.ObjectType
	}{
		{"06f8815b4dc1ba5cabf619d8a8ef392d0f88a2f1", 71, git.ObjectTree},
		{"6d2439d2e920ba92d8e485e75d1b740ae51b609a", 217, git.ObjectCommit},
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

func TestHandleCloneShallowUnshallow(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
	pw.WritePktLine([]byte("shallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"))
	pw.WritePktLine([]byte("deepen 2147483647"))
	pw.Flush()
	pw.WritePktLine([]byte("have 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"))
	pw.WritePktLine([]byte("done"))

	log := log15.New()
	err = handlePull("testdata/repo.git", AuthorizationAllowed, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	if !comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unshallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"},
			{ErrFlush, ""},
			{nil, "ACK 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"},
		},
		&outBuf,
	) {
		return
	}

	odb, err := git.NewOdb()
	if err != nil {
		t.Fatalf("Failed to create odb: %v", err)
	}
	defer odb.Free()

	idx, _, err := UnpackPackfile(odb, &outBuf, dir, nil)
	if err != nil {
		t.Fatalf("Failed to unpack packfile: %v", err)
	}

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

func TestHandlePushUnborn(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("0000000000000000000000000000000000000000 88aa3454adb27c3c343ab57564d962a0a7f6a3c1 refs/heads/master\x00report-status\n"))
	pw.Flush()

	f, err := os.Open(kPackFilename)
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log := log15.New()
	err = handlePush(dir, AuthorizationAllowed, noopUpdateCallback, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	if !comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unpack ok\n"},
			{nil, "ok refs/heads/master\n"},
			{ErrFlush, ""},
		},
		&outBuf,
	) {
		return
	}

	var buf bytes.Buffer
	err = handlePrePull(dir, AuthorizationAllowed, log, &buf)
	if err != nil {
		t.Errorf("Failed to get pre-pull: %v", err)
	}
	discovery, err := DiscoverReferences(&buf)
	if err != nil {
		t.Errorf("Failed to parse the reference discovery: %v", err)
	}
	expectedSymref := "refs/heads/master"
	if expectedSymref != discovery.HeadSymref {
		t.Errorf("Expected %v, got %v", expectedSymref, discovery.HeadSymref)
	}
	expectedReferences := map[string]git.Oid{
		"HEAD":              gitOid("88aa3454adb27c3c343ab57564d962a0a7f6a3c1"),
		"refs/heads/master": gitOid("88aa3454adb27c3c343ab57564d962a0a7f6a3c1"),
	}
	if !reflect.DeepEqual(expectedReferences, discovery.References) {
		t.Errorf("Expected %v, got %v", expectedReferences, discovery.References)
	}
}

func TestHandlePushCallback(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("0000000000000000000000000000000000000000 88aa3454adb27c3c343ab57564d962a0a7f6a3c1 refs/heads/master\x00report-status\n"))
	pw.Flush()

	f, err := os.Open(kPackFilename)
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log := log15.New()
	err = handlePush(dir, AuthorizationAllowed, func(
		repository *git.Repository,
		command *GitCommand,
		oldCommit, newCommit *git.Commit,
	) error {
		return errors.New("go away")
	}, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unpack ok\n"},
			{nil, "ng refs/heads/master go away\n"},
			{ErrFlush, ""},
		},
		&outBuf,
	)
}

func TestHandlePushUnknownCommit(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("0000000000000000000000000000000000000000 0101010101010101010101010101010101010101 refs/heads/master\x00report-status\n"))
	pw.Flush()

	f, err := os.Open(kPackFilename)
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log := log15.New()
	err = handlePush(dir, AuthorizationAllowed, noopUpdateCallback, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unpack ok\n"},
			{nil, "ng refs/heads/master unknown-commit\n"},
			{ErrFlush, ""},
		},
		&outBuf,
	)
}

func TestHandlePushRestrictedRef(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("0000000000000000000000000000000000000000 88aa3454adb27c3c343ab57564d962a0a7f6a3c1 refs/meta/config\x00report-status\n"))
	pw.Flush()

	f, err := os.Open(kPackFilename)
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log := log15.New()
	err = handlePush(dir, AuthorizationAllowedRestricted, noopUpdateCallback, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unpack ok\n"},
			{nil, "ng refs/meta/config restricted-ref\n"},
			{ErrFlush, ""},
		},
		&outBuf,
	)
}

func TestHandlePushMerge(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("0000000000000000000000000000000000000000 6d4fad66ff6271a19aee1bfab1172b34ee05f43f refs/heads/master\x00report-status\n"))
	pw.Flush()

	f, err := os.Open("testdata/pack-merge-commit.pack")
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log := log15.New()
	err = handlePush(dir, AuthorizationAllowedRestricted, noopUpdateCallback, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unpack ok\n"},
			{nil, "ng refs/heads/master merge-unallowed\n"},
			{ErrFlush, ""},
		},
		&outBuf,
	)
}

func TestHandlePushMultipleCommits(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	// Taken from git 2.14.1
	pw := NewPktLineWriter(&inBuf)
	pw.WritePktLine([]byte("0000000000000000000000000000000000000000 55260393bc770a8488b305a5f8e47ab6540f49e8 refs/heads/master\x00report-status\n"))
	pw.Flush()

	f, err := os.Open("testdata/pack-multiple-updates.pack")
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log := log15.New()
	err = handlePush(dir, AuthorizationAllowedRestricted, noopUpdateCallback, log, &inBuf, &outBuf)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	comparePktLineResponse(
		t,
		[]expectedPktLine{
			{nil, "unpack ok\n"},
			{nil, "ng refs/heads/master multiple-updates-unallowed\n"},
			{ErrFlush, ""},
		},
		&outBuf,
	)
}
