package githttp

import (
	"archive/zip"
	"bytes"
	git "github.com/lhchavez/git2go"
	base "github.com/omegaup/go-base"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestHandleRefs(t *testing.T) {
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleRefs(repository, AuthorizationAllowed, "GET")
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

func TestHandleRestrictedRefs(t *testing.T) {
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleRefs(repository, AuthorizationAllowedRestricted, "GET")
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

func TestHandleArchiveCommit(t *testing.T) {
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	response := httptest.NewRecorder()
	if err := handleArchive(repository, "/+archive/88aa3454adb27c3c343ab57564d962a0a7f6a3c1.zip", "GET", response); err != nil {
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

func TestHandleLog(t *testing.T) {
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleLog(repository, "/+log/", "GET")
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
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleLog(repository, "/+log/88aa3454adb27c3c343ab57564d962a0a7f6a3c1", "GET")
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
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleShow(repository, "/+/88aa3454adb27c3c343ab57564d962a0a7f6a3c1", "GET", "")
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
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleShow(repository, "/+/88aa3454adb27c3c343ab57564d962a0a7f6a3c1/", "GET", "")
	if err != nil {
		t.Fatalf("Error getting the log: %v %v", err, result)
	}

	expected := &TreeResult{
		ID: "417c01c8795a35b8e835113a85a5c0c1c77f67fb",
		Entries: []*TreeEntryResult{
			{
				ID:   "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
				Mode: 0100644,
				Type: "blob",
				Name: "empty",
			},
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestHandleShowBlob(t *testing.T) {
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	result, err := handleShow(repository, "/+/88aa3454adb27c3c343ab57564d962a0a7f6a3c1/empty", "GET", "")
	if err != nil {
		t.Fatalf("Error getting the log: %v %v", err, result)
	}

	expected := &BlobResult{
		ID:       "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
		Size:     0,
		Contents: "",
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestHandleNotFound(t *testing.T) {
	repository, err := git.OpenRepository("testdata/repo.git")
	if err != nil {
		t.Fatalf("Error opening git repository: %v", err)
	}
	defer repository.Free()

	log := base.StderrLog()

	paths := []string{
		"/+foo/",        // Invalid type.
		"/+/",           // Missing path.
		"/+/foo",        // Invalid ref.
		"/+/master/foo", // Path not found.
		"/+/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", // Valid ref, but is not a commit.
		"/+log/foo", // Invalid ref.
		"/+log/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", // Valid ref, but is not a commit.
	}
	for _, path := range paths {
		w := httptest.NewRecorder()

		err := handleBrowse("testdata/repo.git", AuthorizationAllowed, "GET", "application/json", path, log, w)
		if err != ErrNotFound {
			t.Errorf("For path %s, expected ErrNotFound, got: %v %v", path, err, w.Body.String())
		}
	}
}
