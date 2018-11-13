package githttp

import (
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSplitTrees(t *testing.T) {
	dir, err := ioutil.TempDir("", "commits_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	repository, err := git.InitRepository(dir, true)
	if err != nil {
		t.Fatalf("Failed to initialize git repository: %v", err)
	}
	defer repository.Free()

	log := log15.New()

	originalTree, err := BuildTree(
		repository,
		map[string]io.Reader{
			// public
			"examples/0.in":                strings.NewReader("1 2"),
			"examples/0.out":               strings.NewReader("3"),
			"interactive/Main.distrib.cpp": strings.NewReader("int main() {}"),
			"statements/es.markdown":       strings.NewReader("Sumas"),
			"statements/images/foo.png":    strings.NewReader(""),
			// protected
			"solution/es.markdown": strings.NewReader("Sumas"),
			"tests/tests.json":     strings.NewReader("{}"),
			// private
			"cases/0.in":           strings.NewReader("1 2"),
			"cases/0.out":          strings.NewReader("3"),
			"interactive/Main.cpp": strings.NewReader("int main() {}"),
			"settings.json":        strings.NewReader("{}"),
			"validator.cpp":        strings.NewReader("int main() {}"),
		},
		log,
	)
	if err != nil {
		t.Fatalf("Failed to build source git tree: %v", err)
	}
	defer originalTree.Free()

	for _, paths := range [][]string{
		// public
		[]string{
			"examples/0.in",
			"examples/0.out",
			"interactive/Main.distrib.cpp",
			"statements/es.markdown",
			"statements/images/foo.png",
		},
		// protected
		[]string{
			"solution/es.markdown",
			"tests/tests.json",
		},
		// private
		[]string{
			"cases/0.in",
			"cases/0.out",
			"interactive/Main.cpp",
			"settings.json",
			"validator.cpp",
		},
	} {
		splitTree, err := SplitTree(
			originalTree,
			repository,
			paths,
			repository,
			log,
		)
		if err != nil {
			t.Fatalf("Failed to split git tree for %v: %v", paths, err)
		}
		defer splitTree.Free()

		newPaths := make([]string, 0)
		if err = splitTree.Walk(func(parent string, entry *git.TreeEntry) int {
			path := path.Join(parent, entry.Name)
			log.Debug("Considering", "path", path, "entry", *entry)
			if entry.Type != git.ObjectBlob {
				return 0
			}
			newPaths = append(newPaths, path)
			return 0
		}); err != nil {
			t.Fatalf("Failed to walk the split git tree for %v: %v", paths, err)
		}

		if !reflect.DeepEqual(newPaths, paths) {
			t.Errorf("Failed to split the tree. Expected %v got %v", paths, newPaths)
		}
	}
}

func TestMergeTrees(t *testing.T) {
	dir, err := ioutil.TempDir("", "commits_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if os.Getenv("PRESERVE") == "" {
		defer os.RemoveAll(dir)
	}

	repo, err := git.InitRepository(dir, true)
	if err != nil {
		t.Fatalf("Failed to initialize git repository: %v", err)
	}
	defer repo.Free()

	log := log15.New()

	type testEntry struct {
		trees  []map[string]io.Reader
		result map[string]io.Reader
	}

	for _, entry := range []testEntry{
		// Simple case.
		testEntry{
			trees: []map[string]io.Reader{
				map[string]io.Reader{
					"cases/0.in":  strings.NewReader("1 2"),
					"cases/0.out": strings.NewReader("3"),
				},
				map[string]io.Reader{
					"statements/es.markdown": strings.NewReader("Sumas"),
				},
			},
			result: map[string]io.Reader{
				"cases/0.in":             strings.NewReader("1 2"),
				"cases/0.out":            strings.NewReader("3"),
				"statements/es.markdown": strings.NewReader("Sumas"),
			},
		},
		// Merging three trees.
		testEntry{
			trees: []map[string]io.Reader{
				map[string]io.Reader{
					"cases/0.in": strings.NewReader("1 2"),
				},
				map[string]io.Reader{
					"cases/0.out": strings.NewReader("3"),
				},
				map[string]io.Reader{
					"statements/es.markdown": strings.NewReader("Sumas"),
				},
			},
			result: map[string]io.Reader{
				"cases/0.in":             strings.NewReader("1 2"),
				"cases/0.out":            strings.NewReader("3"),
				"statements/es.markdown": strings.NewReader("Sumas"),
			},
		},
		// Merging a subtree.
		testEntry{
			trees: []map[string]io.Reader{
				map[string]io.Reader{
					"cases/0.in": strings.NewReader("1 2"),
				},
				map[string]io.Reader{
					"cases/0.out": strings.NewReader("3"),
				},
			},
			result: map[string]io.Reader{
				"cases/0.in":  strings.NewReader("1 2"),
				"cases/0.out": strings.NewReader("3"),
			},
		},
		// One of the files is overwritten / ignored.
		testEntry{
			trees: []map[string]io.Reader{
				map[string]io.Reader{
					"cases/0.in":  strings.NewReader("1 2"),
					"cases/0.out": strings.NewReader("3"),
				},
				map[string]io.Reader{
					"cases/0.out": strings.NewReader("5"),
				},
			},
			result: map[string]io.Reader{
				"cases/0.in":  strings.NewReader("1 2"),
				"cases/0.out": strings.NewReader("3"),
			},
		},
	} {
		sourceTrees := make([]*git.Tree, len(entry.trees))
		for i, treeContents := range entry.trees {
			sourceTrees[i], err = BuildTree(repo, treeContents, log)
			if err != nil {
				t.Fatalf("Failed to build git tree for %v, %v: %v", entry, treeContents, err)
			}
			defer sourceTrees[i].Free()
		}

		expectedTree, err := BuildTree(repo, entry.result, log)
		if err != nil {
			t.Fatalf("Failed to build expected tree for %v, %v: %v", entry, entry.result, err)
		}
		defer expectedTree.Free()

		tree, err := MergeTrees(repo, log, sourceTrees...)
		if err != nil {
			t.Fatalf("Failed to build merged tree for %v, %v: %v", entry, entry.result, err)
		}
		defer tree.Free()
		if !expectedTree.Id().Equal(tree.Id()) {
			t.Errorf("Expected %v, got %v", expectedTree.Id(), tree.Id())
		}
	}
}

func TestSpliceCommit(t *testing.T) {
	dir, err := ioutil.TempDir("", "commits_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if os.Getenv("PRESERVE") == "" {
		defer os.RemoveAll(dir)
	}

	repository, err := git.InitRepository(dir, true)
	if err != nil {
		t.Fatalf("Failed to initialize git repository: %v", err)
	}
	defer repository.Free()

	log := log15.New()

	originalTree, err := BuildTree(
		repository,
		map[string]io.Reader{
			// public
			"examples/0.in":                strings.NewReader("1 2"),
			"examples/0.out":               strings.NewReader("3"),
			"interactive/Main.distrib.cpp": strings.NewReader("int main() {}"),
			"statements/es.markdown":       strings.NewReader("Sumaz"),
			"statements/images/foo.png":    strings.NewReader(""),
			// protected
			"solution/es.markdown": strings.NewReader("Sumaz"),
			"tests/tests.json":     strings.NewReader("{}"),
			// private
			"cases/0.in":           strings.NewReader("1 2"),
			"cases/0.out":          strings.NewReader("3"),
			"interactive/Main.cpp": strings.NewReader("int main() {}"),
			"settings.json":        strings.NewReader("{}"),
			"validator.cpp":        strings.NewReader("int main() {}"),
		},
		log,
	)
	if err != nil {
		t.Fatalf("Failed to build source git tree: %v", err)
	}
	defer originalTree.Free()

	originalCommitID, err := repository.CreateCommit(
		"",
		&git.Signature{
			Name:  "author",
			Email: "author@test.test",
			When:  time.Unix(0, 0),
		},
		&git.Signature{
			Name:  "author",
			Email: "author@test.test",
			When:  time.Unix(0, 0),
		},
		"Initial commit",
		originalTree,
	)
	if err != nil {
		t.Fatalf("Failed to create initial commit: %v", err)
	}
	originalCommit, err := repository.LookupCommit(originalCommitID)
	if err != nil {
		t.Fatalf("Failed to lookup initial commit: %v", err)
	}

	newPackPath := path.Join(dir, "new.pack")
	newCommands, err := SpliceCommit(
		repository,
		originalCommit,
		nil,
		map[string]io.Reader{
			"solution/es.markdown":   strings.NewReader("Sumas"),
			"statements/es.markdown": strings.NewReader("Sumas"),
		},
		[]SplitCommitDescription{
			SplitCommitDescription{
				PathRegexps: []*regexp.Regexp{
					regexp.MustCompile("^cases$"),
				},
				ReferenceName: "refs/heads/private",
			},
			SplitCommitDescription{
				PathRegexps: []*regexp.Regexp{
					regexp.MustCompile("^statements$"),
				},
				ReferenceName: "refs/heads/public",
			},
		},
		&git.Signature{
			Name:  "author",
			Email: "author@test.test",
			When:  time.Unix(0, 0),
		},
		&git.Signature{
			Name:  "committer",
			Email: "committer@test.test",
			When:  time.Unix(0, 0),
		},
		"refs/heads/master",
		nil,
		"Reviewed-In: http://localhost/review/1/",
		newPackPath,
		log,
	)
	if err != nil {
		t.Fatalf("Error splicing commit: %v", err)
	}

	log.Debug("Commands changed", "newCommands", newCommands)
}
