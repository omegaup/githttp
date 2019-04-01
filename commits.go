package githttp

import (
	stderrors "errors"
	"fmt"
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
)

const (
	// objectLimit is the maximum number of objects a tree can contain.
	objectLimit = 10000
)

var (
	// ErrObjectLimitExceeded is the error that's returned when a git tree has
	// more objects than ObjectLimit.
	ErrObjectLimitExceeded = stderrors.New("Tree exceeded object limit")
)

type mergeEntry struct {
	entry      *git.TreeEntry
	objectType git.ObjectType
	trees      []*git.Tree
}

// MergeTrees recursively merges a set of trees. If there are any conflicts in
// files, the resolution is to take the contents of the file in the first tree
// provided. If there are any conflicts in object types (i.e. a path is a tree
// in one tree and a blob in another), the operation fails.
func MergeTrees(
	repository *git.Repository,
	log log15.Logger,
	trees ...*git.Tree,
) (*git.Tree, error) {
	treebuilder, err := repository.TreeBuilder()
	if err != nil {
		log.Error("Error creating treebuilder", "err", err)
		return nil, err
	}
	defer treebuilder.Free()

	entries := make(map[string]*mergeEntry)

	for _, tree := range trees {
		for i := uint64(0); i < tree.EntryCount(); i++ {
			entry := tree.EntryByIndex(i)
			object, err := repository.Lookup(entry.Id)
			if err != nil {
				log.Error("Error looking up tree entry", "entry", entry, "err", err)
				return nil, err
			}
			defer object.Free()

			oldMergeEntry, ok := entries[entry.Name]
			if !ok {
				oldMergeEntry = &mergeEntry{
					entry:      entry,
					objectType: object.Type(),
					trees:      make([]*git.Tree, 0),
				}
				entries[entry.Name] = oldMergeEntry
			} else if oldMergeEntry.objectType != object.Type() {
				log.Error("Type mismatch for path", "entry", entry, "object type", object.Type())
				return nil, errors.New("object type mismatch")
			}

			if object.Type() == git.ObjectTree {
				tree, err := object.AsTree()
				if err != nil {
					log.Error("Error converting object to tree", "entry", entry, "err", err)
					return nil, err
				}
				defer tree.Free()

				oldMergeEntry.trees = append(oldMergeEntry.trees, tree)
			}
		}
	}

	entryNames := make([]string, 0)
	for name := range entries {
		entryNames = append(entryNames, name)
	}
	sort.Strings(entryNames)

	for _, name := range entryNames {
		entry := entries[name]
		if entry.objectType == git.ObjectTree && len(entry.trees) > 1 {
			tree, err := MergeTrees(
				repository,
				log,
				entry.trees...,
			)
			if err != nil {
				log.Error("Error merging subtrees", "entry", entry, "err", err)
				return nil, err
			}
			defer tree.Free()

			if err = treebuilder.Insert(name, tree.Id(), entry.entry.Filemode); err != nil {
				log.Error("Error inserting entry in treebuilder", "name", name, "err", err)
				return nil, err
			}
		} else {
			if err = treebuilder.Insert(name, entry.entry.Id, entry.entry.Filemode); err != nil {
				log.Error("Error inserting entry in treebuilder", "name", name, "err", err)
				return nil, err
			}
		}
	}

	mergedTreeID, err := treebuilder.Write()
	if err != nil {
		log.Error("Error creating merged tree", "err", err)
		return nil, err
	}
	return repository.LookupTree(mergedTreeID)
}

func copyBlob(
	originalRepository *git.Repository,
	blobID *git.Oid,
	repository *git.Repository,
	log log15.Logger,
) error {
	blob, err := originalRepository.LookupBlob(blobID)
	if err != nil {
		log.Error("Error looking up blob", "id", blobID, "err", err)
		return err
	}
	defer blob.Free()

	oid, err := repository.CreateBlobFromBuffer(blob.Contents())
	if err != nil || !blobID.Equal(oid) {
		log.Error("Error creating blob", "id", blobID, "new id", oid, "err", err)
		return err
	}
	return nil
}

func copyTree(
	originalRepository *git.Repository,
	treeID *git.Oid,
	repository *git.Repository,
	log log15.Logger,
) error {
	tree, err := originalRepository.LookupTree(treeID)
	if err != nil {
		log.Error("Error looking up tree", "id", treeID, "err", err)
		return err
	}
	defer tree.Free()

	treebuilder, err := repository.TreeBuilder()
	if err != nil {
		log.Error("Error creating treebuilder", "err", err)
		return err
	}
	defer treebuilder.Free()

	for i := uint64(0); i < tree.EntryCount(); i++ {
		entry := tree.EntryByIndex(i)
		if entry.Type == git.ObjectBlob {
			err = copyBlob(originalRepository, entry.Id, repository, log)
			if err != nil {
				return err
			}
		} else if entry.Type == git.ObjectTree {
			err = copyTree(originalRepository, entry.Id, repository, log)
			if err != nil {
				return err
			}
		}
		if err = treebuilder.Insert(entry.Name, entry.Id, entry.Filemode); err != nil {
			log.Error("Error inserting entry in treebuilder", "name", entry.Name, "err", err)
			return err
		}
	}

	oid, err := treebuilder.Write()
	if err != nil || !treeID.Equal(oid) {
		log.Error("Error creating tree", "id", treeID, "new id", oid, "err", err)
		return err
	}
	return nil
}

// SplitTree extracts a tree from another, potentially in a different
// repository. It recursively copies all the entries given in paths.
func SplitTree(
	originalTree *git.Tree,
	originalRepository *git.Repository,
	paths []string,
	repository *git.Repository,
	log log15.Logger,
) (*git.Tree, error) {
	treebuilder, err := repository.TreeBuilder()
	if err != nil {
		log.Error("Error creating treebuilder", "err", err)
		return nil, err
	}
	defer treebuilder.Free()

	children := make(map[string][]string)

	for _, path := range paths {
		components := strings.SplitN(path, "/", 2)
		if len(components) == 2 {
			if _, ok := children[components[0]]; !ok {
				children[components[0]] = make([]string, 0)
			}
			children[components[0]] = append(children[components[0]], components[1])
			continue
		}

		originalEntry, err := originalTree.EntryByPath(path)
		if err != nil {
			log.Error("Error looking up original tree", "path", path, "err", err)
			return nil, err
		}
		if originalEntry.Type == git.ObjectBlob {
			err = copyBlob(originalRepository, originalEntry.Id, repository, log)
			if err != nil {
				return nil, err
			}
		} else if originalEntry.Type == git.ObjectTree {
			err = copyTree(originalRepository, originalEntry.Id, repository, log)
			if err != nil {
				return nil, err
			}
		}
		if err = treebuilder.Insert(path, originalEntry.Id, originalEntry.Filemode); err != nil {
			log.Error("Error inserting entry in treebuilder", "name", path, "err", err)
			return nil, err
		}
	}

	for name, subpaths := range children {
		if err := (func() error {
			originalEntry, err := originalTree.EntryByPath(name)
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to look up original tree at %s",
					name,
				)
			}

			originalSubtree, err := originalRepository.LookupTree(originalEntry.Id)
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to look up original tree at %s",
					name,
				)
			}
			defer originalSubtree.Free()

			tree, err := SplitTree(originalSubtree, originalRepository, subpaths, repository, log)
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to create new split tree at %s (%v)",
					name,
					subpaths,
				)
			}
			defer tree.Free()

			log.Debug("Creating subtree", "name", name, "contents", subpaths, "id", tree.Id().String())
			if err = treebuilder.Insert(name, tree.Id(), originalEntry.Filemode); err != nil {
				return errors.Wrapf(
					err,
					"failed to insert %s in treebuilder",
					name,
				)
			}
			return nil
		})(); err != nil {
			return nil, err
		}
	}

	oid, err := treebuilder.Write()
	if err != nil {
		log.Error("Error creating tree", "paths", paths, "err", err)
		return nil, err
	}
	return repository.LookupTree(oid)
}

// SplitCommitDescription describes the contents of a split commit.
type SplitCommitDescription struct {
	PathRegexps   []*regexp.Regexp
	ParentCommit  *git.Commit
	ReferenceName string
	Reference     *git.Reference
}

// ContainsPath returns whether a SplitCommitDescription contains a regexp that
// matches a particular path.
func (s *SplitCommitDescription) ContainsPath(path string) bool {
	for _, pathRegexp := range s.PathRegexps {
		if pathRegexp.MatchString(path) {
			return true
		}
	}
	return false
}

// SplitCommitResult contains the result of a split operation. It contains the
// git commit hash and its associated tree hash.
type SplitCommitResult struct {
	CommitID *git.Oid
	TreeID   *git.Oid
}

// SplitCommit splits a commit into several commits, based on the provided
// descriptions. The new commit will be added to a potentially different
// repository than the one it was originally created on.
func SplitCommit(
	originalCommit *git.Commit,
	originalRepository *git.Repository,
	descriptions []SplitCommitDescription,
	repository *git.Repository,
	author, committer *git.Signature,
	commitMessageTag string,
	log log15.Logger,
) ([]SplitCommitResult, error) {
	originalTree, err := originalCommit.Tree()
	if err != nil {
		log.Error("Error creating git tree", "err", err)
		return nil, err
	}
	defer originalTree.Free()

	treePaths := make([][]string, len(descriptions))
	for i := range descriptions {
		treePaths[i] = make([]string, 0)
	}

	objectCount := 0
	var walkErr error
	if err := originalTree.Walk(func(parent string, entry *git.TreeEntry) int {
		objectCount++
		if objectCount > objectLimit {
			walkErr = ErrObjectLimitExceeded
			return -1
		}
		path := path.Join(parent, entry.Name)
		log.Debug("Considering", "path", path, "entry", *entry)
		for i, description := range descriptions {
			if description.ContainsPath(path) {
				log.Debug("Found a match for a path", "path", path, "entry", *entry, "description", description)
				treePaths[i] = append(treePaths[i], path)
				break
			}
		}
		return 0
	}); err != nil || walkErr != nil {
		return nil, walkErr
	}

	splitResult := make([]SplitCommitResult, 0)
	commitMessage := originalCommit.Message()
	if commitMessageTag != "" {
		commitMessage += "\n" + commitMessageTag
	}

	for i, description := range descriptions {
		currentSplitResult, err := (func() (*SplitCommitResult, error) {
			newTree, err := SplitTree(
				originalTree,
				originalRepository,
				treePaths[i],
				repository,
				log,
			)
			if err != nil {
				return nil, err
			}
			defer newTree.Free()

			parentCommits := make([]*git.Oid, 0)
			if description.ParentCommit != nil {
				parentCommitTree, err := description.ParentCommit.Tree()
				if err != nil {
					return nil, errors.Wrapf(
						err,
						"failed to obtain tree from parent commit %s",
						description.ParentCommit.Id().String(),
					)
				}
				defer parentCommitTree.Free()

				if newTree.Id().Equal(parentCommitTree.Id()) {
					return &SplitCommitResult{
						CommitID: description.ParentCommit.Id(),
						TreeID:   parentCommitTree.Id(),
					}, nil
				}
				newParentCommit, err := repository.LookupCommit(description.ParentCommit.Id())
				if err != nil {
					return nil, errors.Wrapf(
						err,
						"failed to look up parent commit %s in new repository",
						description.ParentCommit.Id().String(),
					)
				}
				defer newParentCommit.Free()

				parentCommits = append(parentCommits, newParentCommit.Id())
			}

			// This cannot use CreateCommit, since the parent commits are not yet in
			// the repository. We are yet to create a packfile with them.
			newCommitID, err := repository.CreateCommitFromIds(
				"",
				author,
				committer,
				commitMessage,
				newTree.Id(),
				parentCommits...,
			)
			if err != nil {
				log.Error("Error creating commit", "tree", newTree.Id(), "err", err)
				return nil, err
			}
			return &SplitCommitResult{
				CommitID: newCommitID,
				TreeID:   newTree.Id(),
			}, nil
		})()
		if err != nil {
			return nil, err
		}
		splitResult = append(splitResult, *currentSplitResult)
	}

	return splitResult, nil
}

// SpliceCommit creates a packfile at newPackPath from a commit in a repository
// that will contain split commits based on the provided array of
// SplitCommitDescriptions and will create a merge commit based of the split
// commits.
//
// Note that a lockfile is not acquired in this method since it's assumed that
// the caller already has acquired one.
func SpliceCommit(
	repository *git.Repository,
	commit, parentCommit *git.Commit,
	overrides map[string]io.Reader,
	descriptions []SplitCommitDescription,
	author, committer *git.Signature,
	referenceName string,
	reference *git.Reference,
	commitMessageTag string,
	newPackPath string,
	log log15.Logger,
) ([]*GitCommand, error) {
	newRepository, err := git.OpenRepository(repository.Path())
	if err != nil {
		log.Error(
			"Error opening git repository",
			"path", repository.Path(),
			"err", err,
		)
		return nil, err
	}
	defer newRepository.Free()

	odb, err := newRepository.Odb()
	if err != nil {
		log.Error("Error opening git odb", "err", err)
		return nil, err
	}
	defer odb.Free()

	looseObjectsDir, err := ioutil.TempDir("", fmt.Sprintf("loose_objects_%s", path.Base(repository.Path())))
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to create temporary directory for loose objects",
		)
	}
	defer os.RemoveAll(looseObjectsDir)

	looseObjectsBackend, err := git.NewOdbBackendLoose(looseObjectsDir, -1, false, 0, 0)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to create new loose object backend",
		)
	}
	if err := odb.AddBackend(looseObjectsBackend, 999); err != nil {
		looseObjectsBackend.Free()
		return nil, errors.Wrap(
			err,
			"failed to register loose object backend",
		)
	}

	originalTree, err := commit.Tree()
	if err != nil {
		log.Error("Error obtaining the original tree", "err", err)
		return nil, err
	}
	defer originalTree.Free()

	if len(overrides) != 0 {
		overrideTree, err := BuildTree(
			newRepository,
			overrides,
			log,
		)
		if err != nil {
			log.Error("Error creating the override tree", "err", err)
			return nil, err
		}
		defer overrideTree.Free()
		originalTree, err := commit.Tree()
		if err != nil {
			log.Error("Error obtaining the original commit tree", "err", err)
			return nil, err
		}
		defer originalTree.Free()
		if err = copyTree(repository, originalTree.Id(), newRepository, log); err != nil {
			log.Error("Error copying the original tree to the new repository", "err", err)
			return nil, err
		}
		mergedOverrideTree, err := MergeTrees(
			newRepository,
			log,
			overrideTree,
			originalTree,
		)
		if err != nil {
			log.Error("Error creating merged override tree", "err", err)
			return nil, err
		}
		defer mergedOverrideTree.Free()

		var overrideCommitParents []*git.Oid
		if parentCommit != nil {
			overrideCommitParents = append(overrideCommitParents, parentCommit.Id())
		}
		overrideCommitID, err := newRepository.CreateCommitFromIds(
			"",
			commit.Author(),
			commit.Committer(),
			commit.Message(),
			mergedOverrideTree.Id(),
			overrideCommitParents...,
		)
		if err != nil {
			log.Error("Error creating the override commit", "err", err)
			return nil, err
		}
		if commit, err = newRepository.LookupCommit(overrideCommitID); err != nil {
			log.Error("Error looking up the overridden commit", "err", err)
			return nil, err
		}
		defer commit.Free()

		// Now that all the objects needed are in the new repository, we can just
		// do everything in that one.
		repository = newRepository
	}

	splitCommits, err := SplitCommit(
		commit,
		repository,
		descriptions,
		newRepository,
		author,
		committer,
		commitMessageTag,
		log,
	)
	if err != nil {
		log.Error("Error splitting commit", "err", err)
		return nil, err
	}

	newCommands := make([]*GitCommand, 0)
	newTrees := make([]*git.Tree, 0)
	parentCommits := make([]*git.Oid, 0)
	if parentCommit != nil {
		parentCommits = append(parentCommits, parentCommit.Id())
	}

	for i, splitCommit := range splitCommits {
		newCommit, err := newRepository.LookupCommit(splitCommit.CommitID)
		if err != nil {
			log.Error(
				"Error looking up new private commit",
				"id", splitCommit.CommitID,
				"err", err,
			)
			return nil, err
		}
		defer newCommit.Free()
		var oldCommit *git.Commit
		var oldCommitID *git.Oid
		var oldTreeID *git.Oid
		if descriptions[i].ParentCommit != nil {
			oldCommit = descriptions[i].ParentCommit
			oldCommitID = descriptions[i].ParentCommit.Id()
			oldTreeID = descriptions[i].ParentCommit.TreeId()
		}

		newTree, err := newRepository.LookupTree(splitCommit.TreeID)
		if err != nil {
			log.Error(
				"Error looking up new private tree",
				"id", splitCommit.TreeID,
				"err", err,
			)
			return nil, err
		}
		defer newTree.Free()
		newTrees = append(newTrees, newTree)

		if oldTreeID != nil && splitCommit.TreeID.Equal(oldTreeID) {
			parentCommits = append(parentCommits, oldCommit.Id())
		} else {
			newCommands = append(
				newCommands,
				&GitCommand{
					Old:           oldCommitID,
					OldTree:       oldTreeID,
					New:           splitCommit.CommitID,
					NewTree:       splitCommit.TreeID,
					ReferenceName: descriptions[i].ReferenceName,
					Reference:     descriptions[i].Reference,
					err:           nil,
					logMessage:    newCommit.Message(),
				},
			)
			parentCommits = append(parentCommits, newCommit.Id())
		}
	}

	mergedTree, err := MergeTrees(
		newRepository,
		log,
		newTrees...,
	)
	if err != nil {
		log.Error("Error creating merged tree", "err", err)
		return nil, err
	}
	defer mergedTree.Free()

	commitMessage := commit.Message()
	if commitMessageTag != "" {
		commitMessage += "\n" + commitMessageTag
	}

	// This cannot use CreateCommit, since the parent commits are not yet in the
	// repository. We are yet to create a packfile with them.
	mergedID, err := newRepository.CreateCommitFromIds(
		"",
		author,
		committer,
		commitMessage,
		mergedTree.Id(),
		parentCommits...,
	)
	if err != nil {
		log.Error("Error committing merged data", "err", err)
		return nil, err
	}
	var oldCommitID *git.Oid
	var oldTreeID *git.Oid
	if parentCommit != nil {
		oldCommitID = parentCommit.Id()
		oldTreeID = parentCommit.TreeId()
	}
	newCommands = append(
		newCommands,
		&GitCommand{
			Old:           oldCommitID,
			OldTree:       oldTreeID,
			New:           mergedID,
			NewTree:       mergedTree.Id(),
			ReferenceName: referenceName,
			Reference:     reference,
			err:           nil,
			logMessage:    commitMessage,
		},
	)

	walk, err := newRepository.Walk()
	if err != nil {
		log.Error("Error creating revwalk", "err", err)
		return nil, err
	}
	defer walk.Free()

	if parentCommit != nil {
		if err := walk.Hide(parentCommit.Id()); err != nil {
			log.Error("Error hiding commit", "commit", *parentCommit.Id(), "err", err)
			return nil, err
		}
	}

	if err := walk.Push(mergedID); err != nil {
		log.Error("Error pushing commit", "commit", *mergedID, "err", err)
		return nil, err
	}

	f, err := os.Create(newPackPath)
	if err != nil {
		log.Error("Error opening file for writing", "path", newPackPath, "err", err)
		return nil, err
	}
	defer f.Close()

	pb, err := newRepository.NewPackbuilder()
	if err != nil {
		log.Error("Error creating packbuilder", "err", err)
		return nil, err
	}
	defer pb.Free()

	if err := pb.InsertWalk(walk); err != nil {
		log.Error("Error inserting walk into packbuilder", "err", err)
		return nil, err
	}

	if err := pb.Write(f); err != nil {
		log.Error("Error writing packfile", "path", newPackPath, "err", err)
		return nil, err
	}

	return newCommands, nil
}

// BuildTree recursively builds a tree based on a static map of paths and file
// contents.
func BuildTree(
	repository *git.Repository,
	files map[string]io.Reader,
	log log15.Logger,
) (*git.Tree, error) {
	treebuilder, err := repository.TreeBuilder()
	if err != nil {
		log.Error("Error creating treebuilder", "err", err)
		return nil, err
	}
	defer treebuilder.Free()

	children := make(map[string]map[string]io.Reader)

	for name, reader := range files {
		components := strings.SplitN(name, "/", 2)
		if len(components) == 1 {
			contents, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Error("Error reading contents", "path", name, "err", err)
				return nil, err
			}
			oid, err := repository.CreateBlobFromBuffer(contents)
			if err != nil {
				log.Error("Error creating blob", "path", name, "err", err)
				return nil, err
			}
			log.Debug("Creating blob", "path", name, "len", len(contents), "id", oid)
			if err = treebuilder.Insert(name, oid, 0100644); err != nil {
				log.Error("Error inserting entry in treebuilder", "name", name, "err", err)
				return nil, err
			}
		} else {
			if _, ok := children[components[0]]; !ok {
				children[components[0]] = make(map[string]io.Reader)
			}
			children[components[0]][components[1]] = reader
		}
	}

	for name, subfiles := range children {
		if err := (func() error {
			tree, err := BuildTree(repository, subfiles, log)
			if err != nil {
				return errors.Wrapf(
					err,
					"failed to create subtree %s with files %v",
					name,
					subfiles,
				)
			}
			defer tree.Free()

			if err = treebuilder.Insert(name, tree.Id(), 040000); err != nil {
				return errors.Wrapf(
					err,
					"failed to insert %s in treebuilder",
					name,
				)
			}
			return nil
		})(); err != nil {
			return nil, err
		}
	}

	mergedTreeID, err := treebuilder.Write()
	if err != nil {
		log.Error("Error creating tree", "err", err)
		return nil, err
	}
	log.Debug("Creating tree", "id", mergedTreeID)
	return repository.LookupTree(mergedTreeID)
}
