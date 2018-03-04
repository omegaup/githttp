package githttp

import (
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
)

// buildTree recursively builds a tree based on a static map of paths and file
// contents.
func buildTree(
	repository *git.Repository,
	files map[string]string,
	log log15.Logger,
) (*git.Tree, error) {
	treebuilder, err := repository.TreeBuilder()
	if err != nil {
		log.Error("Error creating treebuilder", "err", err)
		return nil, err
	}
	defer treebuilder.Free()

	children := make(map[string]map[string]string)

	for name, contents := range files {
		components := strings.SplitN(name, "/", 2)
		if len(components) == 1 {
			oid, err := repository.CreateBlobFromBuffer([]byte(contents))
			if err != nil {
				log.Error("Error creating blob", "path", name, "contents", contents, "err", err)
				return nil, err
			}
			log.Info("Creating blob", "path", name, "contents", contents, "id", oid)
			if err = treebuilder.Insert(name, oid, 0100644); err != nil {
				log.Error("Error inserting entry in treebuilder", "name", name, "err", err)
				return nil, err
			}
		} else {
			if _, ok := children[components[0]]; !ok {
				children[components[0]] = make(map[string]string)
			}
			children[components[0]][components[1]] = contents
		}
	}

	for name, subfiles := range children {
		tree, err := buildTree(repository, subfiles, log)
		if err != nil {
			log.Error("Error creating subtree", "path", name, "contents", subfiles, "err", err)
			return nil, err
		}
		defer tree.Free()

		if err = treebuilder.Insert(name, tree.Id(), 040000); err != nil {
			log.Error("Error inserting entry in treebuilder", "name", name, "err", err)

			return nil, err
		}
	}

	mergedTreeID, err := treebuilder.Write()
	if err != nil {
		log.Error("Error creating tree", "err", err)
		return nil, err
	}
	log.Info("Creating tree", "id", mergedTreeID)
	return repository.LookupTree(mergedTreeID)
}

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

	originalTree, err := buildTree(
		repository,
		map[string]string{
			// public
			"examples/0.in":                "1 2",
			"examples/0.out":               "3",
			"interactive/Main.distrib.cpp": "int main() {}",
			"statements/es.markdown":       "Sumas",
			"statements/images/foo.png":    "",
			// protected
			"solution/es.markdown": "Sumas",
			"tests/tests.json":     "{}",
			// private
			"cases/0.in":           "1 2",
			"cases/0.out":          "3",
			"interactive/Main.cpp": "int main() {}",
			"settings.json":        "{}",
			"validator.cpp":        "int main() {}",
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
	defer os.RemoveAll(dir)

	repo, err := git.InitRepository(dir, true)
	if err != nil {
		t.Fatalf("Failed to initialize git repository: %v", err)
	}
	defer repo.Free()

	log := log15.New()

	type testEntry struct {
		trees  []map[string]string
		result map[string]string
	}

	for _, entry := range []testEntry{
		// Simple case.
		testEntry{
			trees: []map[string]string{
				map[string]string{
					"cases/0.in":  "1 2",
					"cases/0.out": "3",
				},
				map[string]string{
					"statements/es.markdown": "Sumas",
				},
			},
			result: map[string]string{
				"cases/0.in":             "1 2",
				"cases/0.out":            "3",
				"statements/es.markdown": "Sumas",
			},
		},
		// Merging three trees.
		testEntry{
			trees: []map[string]string{
				map[string]string{
					"cases/0.in": "1 2",
				},
				map[string]string{
					"cases/0.out": "3",
				},
				map[string]string{
					"statements/es.markdown": "Sumas",
				},
			},
			result: map[string]string{
				"cases/0.in":             "1 2",
				"cases/0.out":            "3",
				"statements/es.markdown": "Sumas",
			},
		},
		// Merging a subtree.
		testEntry{
			trees: []map[string]string{
				map[string]string{
					"cases/0.in": "1 2",
				},
				map[string]string{
					"cases/0.out": "3",
				},
			},
			result: map[string]string{
				"cases/0.in":  "1 2",
				"cases/0.out": "3",
			},
		},
		// One of the files is overwritten / ignored.
		testEntry{
			trees: []map[string]string{
				map[string]string{
					"cases/0.in":  "1 2",
					"cases/0.out": "3",
				},
				map[string]string{
					"cases/0.out": "5",
				},
			},
			result: map[string]string{
				"cases/0.in":  "1 2",
				"cases/0.out": "3",
			},
		},
	} {
		sourceTrees := make([]*git.Tree, len(entry.trees))
		for i, treeContents := range entry.trees {
			sourceTrees[i], err = buildTree(repo, treeContents, log)
			if err != nil {
				t.Fatalf("Failed to build git tree for %v, %v: %v", entry, treeContents, err)
			}
			defer sourceTrees[i].Free()
		}

		expectedTree, err := buildTree(repo, entry.result, log)
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
