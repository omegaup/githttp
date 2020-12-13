package githttp

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	git "github.com/lhchavez/git2go/v32"
	base "github.com/omegaup/go-base"
)

var (
	gitCommandEnv = []string{
		"GIT_AUTHOR_EMAIL=githttp@test.com",
		"GIT_AUTHOR_NAME=Git Test User",
		"GIT_COMMITTER_EMAIL=githttp@test.com",
		"GIT_COMMITTER_NAME=Git Test User",
	}
)

func allowAuthorizationCallback(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	repositoryName string,
	operation GitOperation,
) (AuthorizationLevel, string) {
	return AuthorizationAllowed, "test_user"
}

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

	log := base.StderrLog()
	handler := GitServer(
		"testdata",
		".git",
		true,
		NewGitProtocol(allowAuthorizationCallback, nil, nil, nil, false, log),
		nil,
		log,
	)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")

	cmd := exec.Command(gitcmd, "clone", ts.URL+"/repo/", repoDir)
	cmd.Env = gitCommandEnv
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "log", "--pretty=%h")
	cmd.Env = gitCommandEnv
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

	log := base.StderrLog()
	handler := GitServer(
		"testdata",
		".git",
		true,
		NewGitProtocol(allowAuthorizationCallback, nil, nil, nil, false, log),
		nil,
		log,
	)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")

	cmd := exec.Command(gitcmd, "clone", "--depth=1", ts.URL+"/repo/", repoDir)
	cmd.Env = gitCommandEnv
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "log", "--pretty=%h")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil || !bytes.Equal(output, []byte("6d2439d\n")) {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "fetch", "--unshallow")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "log", "--pretty=%h")
	cmd.Env = gitCommandEnv
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
	log := base.StderrLog()
	if os.Getenv("PRESERVE") != "" {
		log.Info("Preserving test directory", "path", dir)
	} else {
		defer os.RemoveAll(dir)
	}

	{
		repo, err := git.InitRepository(filepath.Join(dir, "repo.git"), true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	handler := GitServer(
		dir,
		".git",
		true,
		NewGitProtocol(allowAuthorizationCallback, nil, nil, nil, false, log),
		nil,
		log,
	)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")
	upstreamURL := ts.URL + "/repo/"

	cmd := exec.Command(gitcmd, "clone", "--depth=1", upstreamURL, repoDir)
	cmd.Env = gitCommandEnv
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "remote", "get-url", "--push", "origin")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil || !strings.HasPrefix(string(output), upstreamURL) {
		t.Errorf("Failed to clone: %v %q", err, string(output))
	}

	if err = ioutil.WriteFile(filepath.Join(repoDir, "empty"), []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cmd = exec.Command(gitcmd, "add", "empty")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "commit", "--all", "--message", "Empty")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "show")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "push", "--porcelain", "-u", "origin", "HEAD:changes/initial")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "push", "--porcelain", "-u", "origin", "HEAD:master")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}
}

func TestGitbomb(t *testing.T) {
	gitcmd, err := exec.LookPath("git")
	if err != nil {
		t.Skipf("git not found: %v", err)
	}

	dir, err := ioutil.TempDir("", "server_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	log := base.StderrLog()
	if os.Getenv("PRESERVE") != "" {
		log.Info("Preserving test directory", "path", dir)
	} else {
		defer os.RemoveAll(dir)
	}

	{
		repo, err := git.InitRepository(filepath.Join(dir, "repo.git"), true)
		if err != nil {
			t.Fatalf("Failed to initialize git repository: %v", err)
		}
		repo.Free()
	}

	handler := GitServer(
		dir,
		".git",
		true,
		NewGitProtocol(allowAuthorizationCallback, nil, nil, nil, false, log),
		nil,
		log,
	)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	repoDir := filepath.Join(dir, "repo")
	upstreamURL := ts.URL + "/repo/"

	cmd := exec.Command(gitcmd, "clone", "--depth=1", upstreamURL, repoDir)
	cmd.Env = gitCommandEnv
	cmd.Stdin = strings.NewReader("foo\nbar\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run git clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "remote", "get-url", "--push", "origin")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil || !strings.HasPrefix(string(output), upstreamURL) {
		t.Errorf("Failed to clone: %v %q", err, string(output))
	}

	if err = ioutil.WriteFile(filepath.Join(repoDir, "empty"), []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cmd = exec.Command(gitcmd, "add", "empty")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "commit", "--all", "--message", "Empty")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "show")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "push", "--porcelain", "-u", "origin", "HEAD:changes/initial")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}

	cmd = exec.Command(gitcmd, "push", "--porcelain", "-u", "origin", "HEAD:master")
	cmd.Env = gitCommandEnv
	cmd.Dir = repoDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("Failed to clone: %v %q", err, output)
	}
}
