package githttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/omegaup/go-base/logging/log15/v3"

	git "github.com/libgit2/git2go/v33"
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
	log, _ := log15.New("info", false)
	m := NewLockfileManager()
	defer m.Clear()

	err := handlePrePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowedRestricted,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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
	log, _ := log15.New("info", false)
	m := NewLockfileManager()
	defer m.Clear()

	err := handlePrePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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
	log, _ := log15.New("info", false)
	m := NewLockfileManager()
	defer m.Clear()

	err := handlePrePush(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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
	log, _ := log15.New("info", false)
	m := NewLockfileManager()
	defer m.Clear()

	err := handlePrePull(
		context.Background(),
		m,
		"testdata/empty.git",
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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
	log, _ := log15.New("info", false)
	m := NewLockfileManager()
	defer m.Clear()

	err := handlePrePush(
		context.Background(),
		m,
		"testdata/empty.git",
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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
	m := NewLockfileManager()
	defer m.Clear()

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("want 0000000000000000000000000000000000000000 thin-pack ofs-delta agent=git/2.14.1\n"))
		pw.Flush()
		pw.WritePktLine([]byte("done"))
	}

	log, _ := log15.New("info", false)
	err = handlePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	expected := []PktLineResponse{
		{"ERR upload-pack: not our ref 0000000000000000000000000000000000000000", nil},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandleClone(t *testing.T) {
	var inBuf, outBuf bytes.Buffer

	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
		pw.Flush()
		pw.WritePktLine([]byte("done"))
	}

	log, _ := log15.New("info", false)
	err = handlePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	expected := []PktLineResponse{
		{"NAK\n", nil},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Fatalf("pkt-reader expected %q, got %q", expected, actual)
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
	m := NewLockfileManager()
	defer m.Clear()

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
		pw.Flush()
		pw.WritePktLine([]byte("have 88aa3454adb27c3c343ab57564d962a0a7f6a3c1\n"))
		pw.WritePktLine([]byte("done"))
	}

	log, _ := log15.New("info", false)
	err = handlePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	expected := []PktLineResponse{
		{"ACK 88aa3454adb27c3c343ab57564d962a0a7f6a3c1\n", nil},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Fatalf("pkt-reader expected %q, got %q", expected, actual)
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
	m := NewLockfileManager()
	defer m.Clear()

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
		pw.WritePktLine([]byte("deepen 1"))
		pw.Flush()
	}

	log, _ := log15.New("info", false)
	err = handlePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	expected := []PktLineResponse{
		{"shallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandleCloneShallowClone(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
		pw.WritePktLine([]byte("deepen 1"))
		pw.Flush()
		pw.WritePktLine([]byte("done"))
	}

	log, _ := log15.New("info", false)
	err = handlePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	expected := []PktLineResponse{
		{"shallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n", nil},
		{"", ErrFlush},
		{"NAK\n", nil},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Fatalf("pkt-reader expected %q, got %q", expected, actual)
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
	m := NewLockfileManager()
	defer m.Clear()

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("want 6d2439d2e920ba92d8e485e75d1b740ae51b609a thin-pack ofs-delta agent=git/2.14.1\n"))
		pw.WritePktLine([]byte("shallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"))
		pw.WritePktLine([]byte("deepen 2147483647"))
		pw.Flush()
		pw.WritePktLine([]byte("have 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n"))
		pw.WritePktLine([]byte("done"))
	}

	log, _ := log15.New("info", false)
	err = handlePull(
		context.Background(),
		m,
		"testdata/repo.git",
		AuthorizationAllowed,
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to clone: %v", err)
	}

	expected := []PktLineResponse{
		{"unshallow 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n", nil},
		{"", ErrFlush},
		{"ACK 6d2439d2e920ba92d8e485e75d1b740ae51b609a\n", nil},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Fatalf("pkt-reader expected %q, got %q", expected, actual)
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
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("0000000000000000000000000000000000000000 88aa3454adb27c3c343ab57564d962a0a7f6a3c1 refs/heads/master\x00report-status\n"))
		pw.Flush()
	}

	f, err := os.Open(packFilename)
	if err != nil {
		t.Fatalf("Failed to open the packfile: %v", err)
	}
	defer f.Close()
	if _, err = io.Copy(&inBuf, f); err != nil {
		t.Fatalf("Failed to copy the packfile: %v", err)
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ok refs/heads/master\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Fatalf("pkt-reader expected %q, got %q", expected, actual)
	}

	var buf bytes.Buffer
	err = handlePrePull(
		context.Background(),
		m,
		dir,
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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

func TestHandlePushPreprocess(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if os.Getenv("PRESERVE") == "" {
		defer os.RemoveAll(dir)
	}
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("0000000000000000000000000000000000000000 f460ceba1a6ac94a074efe17011866b93fd51d39 refs/heads/master\x00report-status\n"))
		pw.Flush()

		f, err := os.Open("testdata/sumas.pack")
		if err != nil {
			t.Fatalf("Failed to open the packfile: %v", err)
		}
		defer f.Close()
		if _, err = io.Copy(&inBuf, f); err != nil {
			t.Fatalf("Failed to copy the packfile: %v", err)
		}
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			PreprocessCallback: func(
				ctx context.Context,
				originalRepository *git.Repository,
				tmpDir string,
				originalPackPath string,
				originalCommands []*GitCommand,
			) (string, []*GitCommand, error) {
				if len(originalCommands) != 1 {
					t.Fatalf("More than one command unsupported")
				}

				originalCommit, err := originalRepository.LookupCommit(originalCommands[0].New)
				if err != nil {
					log.Error(
						"Error looking up commit",
						map[string]any{
							"err": err,
						},
					)
					return originalPackPath, originalCommands, err
				}
				defer originalCommit.Free()

				newPackPath := path.Join(tmpDir, "new.pack")
				newCommands, err := SpliceCommit(
					originalRepository,
					m,
					originalCommit,
					nil,
					map[string]io.Reader{},
					[]SplitCommitDescription{
						{
							PathRegexps: []*regexp.Regexp{
								regexp.MustCompile("^cases$"),
							},
							ReferenceName: "refs/heads/private",
						},
						{
							PathRegexps: []*regexp.Regexp{
								regexp.MustCompile("^statements$"),
							},
							ReferenceName: "refs/heads/public",
						},
					},
					&git.Signature{
						Name:  "author",
						Email: "author@test.test",
						When:  time.Unix(0, 0).In(time.UTC),
					},
					&git.Signature{
						Name:  "committer",
						Email: "committer@test.test",
						When:  time.Unix(0, 0).In(time.UTC),
					},
					"refs/heads/master",
					nil,
					"Reviewed-In: http://localhost/review/1/",
					newPackPath,
					log,
				)
				if err != nil {
					log.Error(
						"Error splicing commit",
						map[string]any{
							"err": err,
						},
					)
					return originalPackPath, originalCommands, err
				}

				log.Debug(
					"Commands changed",
					map[string]any{
						"old commands": originalCommands,
						"newCommands":  newCommands,
					},
				)

				return newPackPath, newCommands, nil
			},
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ok refs/heads/master\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Fatalf("pkt-reader expected %q, got %q", expected, actual)
	}

	var buf bytes.Buffer
	err = handlePrePull(
		context.Background(),
		m,
		dir,
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&buf,
	)
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
		"HEAD":               gitOid("8f3e429bd47a1a3e2f41739dfd58b946f367a071"),
		"refs/heads/master":  gitOid("8f3e429bd47a1a3e2f41739dfd58b946f367a071"),
		"refs/heads/public":  gitOid("e9b04df7b2fe682b35ae7e33841e480fcaa7ffec"),
		"refs/heads/private": gitOid("5a6e286aa91c51b1624d58651c5b6914d041c759"),
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
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("0000000000000000000000000000000000000000 88aa3454adb27c3c343ab57564d962a0a7f6a3c1 refs/heads/master\x00report-status\n"))
		pw.Flush()

		f, err := os.Open(packFilename)
		if err != nil {
			t.Fatalf("Failed to open the packfile: %v", err)
		}
		defer f.Close()
		if _, err = io.Copy(&inBuf, f); err != nil {
			t.Fatalf("Failed to copy the packfile: %v", err)
		}
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			UpdateCallback: func(
				ctx context.Context,
				repository *git.Repository,
				level AuthorizationLevel,
				command *GitCommand,
				oldCommit, newCommit *git.Commit,
			) error {
				return errors.New("go away")
			},
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ng refs/heads/master go away\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandlePushUnknownCommit(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("0000000000000000000000000000000000000000 0101010101010101010101010101010101010101 refs/heads/master\x00report-status\n"))
		pw.Flush()

		f, err := os.Open(packFilename)
		if err != nil {
			t.Fatalf("Failed to open the packfile: %v", err)
		}
		defer f.Close()
		if _, err = io.Copy(&inBuf, f); err != nil {
			t.Fatalf("Failed to copy the packfile: %v", err)
		}
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowed,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ng refs/heads/master unknown-commit\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandlePushRestrictedRef(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("0000000000000000000000000000000000000000 88aa3454adb27c3c343ab57564d962a0a7f6a3c1 refs/meta/config\x00report-status\n"))
		pw.Flush()

		f, err := os.Open(packFilename)
		if err != nil {
			t.Fatalf("Failed to open the packfile: %v", err)
		}
		defer f.Close()
		if _, err = io.Copy(&inBuf, f); err != nil {
			t.Fatalf("Failed to copy the packfile: %v", err)
		}
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowedRestricted,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ng refs/meta/config restricted-ref\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandlePushMerge(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
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
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowedRestricted,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ok refs/heads/master\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandlePushMultipleCommits(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
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
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowedRestricted,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ok refs/heads/master\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}

func TestHandleNonFastForward(t *testing.T) {
	var inBuf, outBuf bytes.Buffer
	dir, err := ioutil.TempDir("", "protocol_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)
	m := NewLockfileManager()
	defer m.Clear()

	{
		repo, err := git.InitRepository(dir, true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	{
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
	}

	log, _ := log15.New("info", false)
	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowedRestricted,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}
	expected := []PktLineResponse{
		{"unpack ok\n", nil},
		{"ok refs/heads/master\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}

	inBuf.Reset()
	outBuf.Reset()
	{
		// Taken from git 2.14.1
		pw := NewPktLineWriter(&inBuf)
		pw.WritePktLine([]byte("55260393bc770a8488b305a5f8e47ab6540f49e8 6d4fad66ff6271a19aee1bfab1172b34ee05f43f refs/heads/master\x00report-status\n"))
		pw.Flush()

		f, err := os.Open("testdata/pack-merge-commit.pack")
		if err != nil {
			t.Fatalf("Failed to open the packfile: %v", err)
		}
		defer f.Close()
		if _, err = io.Copy(&inBuf, f); err != nil {
			t.Fatalf("Failed to copy the packfile: %v", err)
		}
	}

	err = handlePush(
		context.Background(),
		m,
		dir,
		AuthorizationAllowedRestricted,
		NewGitProtocol(GitProtocolOpts{
			Log: log,
		}),
		log,
		&inBuf,
		&outBuf,
	)
	if err != nil {
		t.Fatalf("Failed to push: %v", err)
	}
	expected = []PktLineResponse{
		{"unpack ok\n", nil},
		{"ng refs/heads/master non-fast-forward\n", nil},
		{"", ErrFlush},
	}
	if actual, ok := ComparePktLineResponse(
		&outBuf,
		expected,
	); !ok {
		t.Errorf("pkt-reader expected %q, got %q", expected, actual)
	}
}
