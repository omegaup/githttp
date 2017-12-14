package githttp

import (
	"bytes"
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestServerClone(t *testing.T) {
	gitcmd, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("git not found: %v", err)
	}

	dir, err := ioutil.TempDir("", "server_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	log := log15.New()
	handler := GitServer("testdata", true, nil, nil, nil, log)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")

	cmd := exec.Command(gitcmd, "clone", ts.URL+"/repo/", repoDir)
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "log", "--pretty=%h")
	cmd.Dir = repoDir
	output, err := cmd.CombinedOutput()
	if err != nil || !bytes.Equal(output, []byte("6d2439d\n88aa345\n")) {
		t.Errorf("Failed to clone: %v %q", err, output)
	}
}

func TestServerCloneShallow(t *testing.T) {
	gitcmd, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("git not found: %v", err)
	}

	dir, err := ioutil.TempDir("", "server_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	log := log15.New()
	handler := GitServer("testdata", true, nil, nil, nil, log)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")

	cmd := exec.Command(gitcmd, "clone", "--depth=1", ts.URL+"/repo/", repoDir)
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "log", "--pretty=%h")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil || !bytes.Equal(output, []byte("6d2439d\n")) {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "fetch", "--unshallow")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "log", "--pretty=%h")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil || !bytes.Equal(output, []byte("6d2439d\n88aa345\n")) {
		t.Errorf("Failed to clone: %v %q", err, output)
	}
}

func TestServerPush(t *testing.T) {
	gitcmd, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("git not found: %v", err)
	}

	dir, err := ioutil.TempDir("", "server_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	{
		repo, err := git.InitRepository(filepath.Join(dir, "repo.git"), true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	log := log15.New()
	handler := GitServer(dir, true, nil, nil, nil, log)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")
	upstreamURL := ts.URL + "/repo/"

	cmd := exec.Command(gitcmd, "clone", "--depth=1", upstreamURL, repoDir)
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "remote", "get-url", "--push", "origin")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil || !strings.HasPrefix(string(output), upstreamURL) {
		t.Errorf("Failed to clone: %v %q", err, string(output))
	}

	if err = ioutil.WriteFile(filepath.Join(repoDir, "empty"), []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cmd = exec.Command(gitcmd, "add", "empty")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "commit", "--all", "--message", "Empty")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "show")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "push", "--porcelain")
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}
}
