package githttp

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	log15 "github.com/omegaup/go-base/logging/log15"
	base "github.com/omegaup/go-base/v3"

	git "github.com/libgit2/git2go/v33"
)

func TestHandleRefs(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleRefs(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"GET",
	)
	if err != nil {
		t.Fatalf("Error getting the list of refs: %v", err)
	}

	expected := RefsResult{
		"HEAD": &RefResult{
			Value:  "6d2439d2e920ba92d8e485e75d1b740ae51b609a",
			Target: "refs/heads/master",
		},
		"refs/heads/master": &RefResult{
			Value: "6d2439d2e920ba92d8e485e75d1b740ae51b609a",
		},
		"refs/meta/config": &RefResult{
			Value: "d0c442210b72c207637a63e4eda991bc27abc0bd",
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestHandleRefsWithReferenceDiscoveryCallback(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		ReferenceDiscoveryCallback: func(
			ctx context.Context,
			repository *git.Repository,
			referenceName string,
		) bool {
			return referenceName == "refs/heads/public"
		},
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleRefs(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"GET",
	)
	if err != nil {
		t.Fatalf("Error getting the list of refs: %v", err)
	}

	expected := RefsResult{}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestHandleRestrictedRefs(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleRefs(
		context.Background(),
		repository,
		AuthorizationAllowedRestricted,
		protocol,
		"GET",
	)
	if err != nil {
		t.Fatalf("Error getting the list of refs: %v", err)
	}

	expected := RefsResult{
		"HEAD": &RefResult{
			Value:  "6d2439d2e920ba92d8e485e75d1b740ae51b609a",
			Target: "refs/heads/master",
		},
		"refs/heads/master": &RefResult{
			Value: "6d2439d2e920ba92d8e485e75d1b740ae51b609a",
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestHandleArchiveCommitZip(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	response := httptest.NewRecorder()
	if err := handleArchive(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"/+archive/88aa3454adb27c3c343ab57564d962a0a7f6a3c1.zip",
		"GET",
		response,
	); err != nil {
		t.Fatalf("Error getting archive: %v", err)
	}

	z, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
	if err != nil {
		t.Fatalf("Error opening zip from response: %v", err)
	}

	if 1 != len(z.File) {
		t.Errorf("Expected %d, got %d", 1, len(z.File))
	} else if "empty" != z.File[0].Name {
		t.Errorf("Expected %s, got %v", "empty", z.File[0])
	}
}

func TestHandleArchiveCommitTarball(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	response := httptest.NewRecorder()
	if err := handleArchive(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"/+archive/88aa3454adb27c3c343ab57564d962a0a7f6a3c1.tar.gz",
		"GET",
		response,
	); err != nil {
		t.Fatalf("Error getting archive: %v", err)
	}
	if "application/gzip" != response.Header().Get("Content-Type") {
		t.Fatalf("Content-Type. Expected %s, got %s", "application/gzip", response.Header().Get("Content-Type"))
	}

	gz, err := gzip.NewReader(bytes.NewReader(response.Body.Bytes()))
	if err != nil {
		t.Fatalf("Error opening gzip from response: %v", err)
	}
	defer gz.Close()

	a := tar.NewReader(gz)
	hdr, err := a.Next()
	if err != nil {
		t.Fatalf("Tarball is empty: %v", err)
	}

	if "empty" != hdr.Name {
		t.Errorf("Expected %s, got %v", "empty", hdr.Name)
	}
}

func TestHandleLog(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleLog(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"/+log/",
		"GET",
	)
	if err != nil {
		t.Fatalf("Error getting the log: %v %v", err, result)
	}

	expected := &LogResult{
		Log: []*CommitResult{
			{
				Commit:  "6d2439d2e920ba92d8e485e75d1b740ae51b609a",
				Tree:    "06f8815b4dc1ba5cabf619d8a8ef392d0f88a2f1",
				Parents: []string{"88aa3454adb27c3c343ab57564d962a0a7f6a3c1"},
				Author: &SignatureResult{
					Name:  "lhchavez",
					Email: "lhchavez@lhchavez.com",
					Time:  "Sun, 10 Dec 2017 21:07:21 -0800",
				},
				Committer: &SignatureResult{
					Name:  "lhchavez",
					Email: "lhchavez@lhchavez.com",
					Time:  "Sun, 10 Dec 2017 21:07:21 -0800",
				},
				Message: "Copy\n",
			},
			{
				Commit:  "88aa3454adb27c3c343ab57564d962a0a7f6a3c1",
				Tree:    "417c01c8795a35b8e835113a85a5c0c1c77f67fb",
				Parents: []string{},
				Author: &SignatureResult{
					Name:  "lhchavez",
					Email: "lhchavez@lhchavez.com",
					Time:  "Sun, 10 Dec 2017 11:51:32 -0800",
				},
				Committer: &SignatureResult{
					Name:  "lhchavez",
					Email: "lhchavez@lhchavez.com",
					Time:  "Sun, 10 Dec 2017 11:51:32 -0800",
				},
				Message: "Empty\n",
			},
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestHandleLogCommit(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleLog(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"/+log/88aa3454adb27c3c343ab57564d962a0a7f6a3c1",
		"GET",
	)
	if err != nil {
		t.Fatalf("Error getting the log: %v %v", err, result)
	}

	expected := &LogResult{
		Log: []*CommitResult{
			{
				Commit:  "88aa3454adb27c3c343ab57564d962a0a7f6a3c1",
				Tree:    "417c01c8795a35b8e835113a85a5c0c1c77f67fb",
				Parents: []string{},
				Author: &SignatureResult{
					Name:  "lhchavez",
					Email: "lhchavez@lhchavez.com",
					Time:  "Sun, 10 Dec 2017 11:51:32 -0800",
				},
				Committer: &SignatureResult{
					Name:  "lhchavez",
					Email: "lhchavez@lhchavez.com",
					Time:  "Sun, 10 Dec 2017 11:51:32 -0800",
				},
				Message: "Empty\n",
			},
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestHandleShowCommit(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleShow(
		context.Background(),
		repository,
		AuthorizationAllowed,
		protocol,
		"/+/88aa3454adb27c3c343ab57564d962a0a7f6a3c1",
		"GET",
		"",
	)
	if err != nil {
		t.Fatalf("Error getting the log: %v %v", err, result)
	}

	expected := &CommitResult{
		Commit:  "88aa3454adb27c3c343ab57564d962a0a7f6a3c1",
		Tree:    "417c01c8795a35b8e835113a85a5c0c1c77f67fb",
		Parents: []string{},
		Author: &SignatureResult{
			Name:  "lhchavez",
			Email: "lhchavez@lhchavez.com",
			Time:  "Sun, 10 Dec 2017 11:51:32 -0800",
		},
		Committer: &SignatureResult{
			Name:  "lhchavez",
			Email: "lhchavez@lhchavez.com",
			Time:  "Sun, 10 Dec 2017 11:51:32 -0800",
		},
		Message: "Empty\n",
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestHandleShowTree(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	expected := &TreeResult{
		ID: "417c01c8795a35b8e835113a85a5c0c1c77f67fb",
		Entries: []*TreeEntryResult{
			{
				ID:   "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
				Mode: 0100644,
				Type: "blob",
				Name: "empty",
				Size: 0,
			},
		},
	}

	for _, requestURL := range []string{
		// Use commit+path.
		"/+/88aa3454adb27c3c343ab57564d962a0a7f6a3c1/",
		// Use the object ID directly.
		"/+/417c01c8795a35b8e835113a85a5c0c1c77f67fb",
	} {
		result, err := handleShow(
			context.Background(),
			repository,
			AuthorizationAllowed,
			protocol,
			requestURL,
			"GET",
			"",
		)
		if err != nil {
			t.Fatalf("Error getting showing tree: %v %v", err, result)
		}

		if !reflect.DeepEqual(expected, result) {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	}
}

func TestHandleShowBlob(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	expected := &BlobResult{
		ID:       "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
		Size:     0,
		Contents: "",
	}

	for _, requestURL := range []string{
		// Use commit+path.
		"/+/88aa3454adb27c3c343ab57564d962a0a7f6a3c1/empty",
		// Use the object ID directly.
		"/+/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
	} {
		result, err := handleShow(
			context.Background(),
			repository,
			AuthorizationAllowed,
			protocol,
			requestURL,
			"GET",
			"",
		)
		if err != nil {
			t.Fatalf("Error getting the blob: %v %v", err, result)
		}
		if !reflect.DeepEqual(expected, result) {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	}
}

func TestHandleNotFound(t *testing.T) {
	log, _ := log15.New("info", false)
	protocol := NewGitProtocol(GitProtocolOpts{
		ReferenceDiscoveryCallback: func(
			ctx context.Context,
			repository *git.Repository,
			referenceName string,
		) bool {
			return referenceName == "refs/heads/public"
		},
		Log: log,
	})

	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	paths := []string{
		"/+foo/",          // Invalid type.
		"/+/",             // Missing path.
		"/+/foo",          // Invalid ref.
		"/+/master/foo",   // Path not found.
		"/+/master/empty", // Path exists, but ref not viewable.
		"/+/6d2439d2e920ba92d8e485e75d1b740ae51b609a/empty", // Path exists, but ref not viewable.
		"/+/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391/",      // Valid ref, but is not a commit.
		"/+archive/foo.zip",    // Invalid ref.
		"/+archive/master.zip", // Valid ref, but is not viewable.
		"/+archive/6d2439d2e920ba92d8e485e75d1b740ae51b609a.zip", // Valid ref, but is not viewable.
		"/+log/foo",    // Invalid ref.
		"/+log/master", // Valid ref, but is not viewable.
		"/+log/6d2439d2e920ba92d8e485e75d1b740ae51b609a", // Valid ref, but is not viewable.
		"/+log/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", // Valid ref, but is not a commit.
	}
	for _, path := range paths {
		w := httptest.NewRecorder()

		err := handleBrowse(
			context.Background(),
			"testdata/repo.git",
			AuthorizationAllowed,
			protocol,
			"GET",
			"application/json",
			path,
			w,
		)
		if !base.HasErrorCategory(err, ErrNotFound) {
			t.Errorf("For path %s, expected ErrNotFound, got: %v %v", path, err, w.Body.String())
		}
	}
}
