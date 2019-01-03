package githttp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// BlobDisplayMaxSize is the maximum size that a blob can be in order to
	// display it.
	BlobDisplayMaxSize = 1 * 1024 * 1024
)

// A RefResult represents a single reference in a git repository.
type RefResult struct {
	Value  string `json:"value,omitempty"`
	Peeled string `json:"peeled,omitempty"`
	Target string `json:"target,omitempty"`
}

// A RefsResult represents the mapping of ref names to RefResult.
type RefsResult map[string]*RefResult

func (r *RefsResult) String() string {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(r)
	return buf.String()
}

// A SignatureResult represents one of the signatures of the commit.
type SignatureResult struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Time  string `json:"time"`
}

// A CommitResult represents a git commit.
type CommitResult struct {
	Commit    string           `json:"commit"`
	Tree      string           `json:"tree"`
	Parents   []string         `json:"parents"`
	Author    *SignatureResult `json:"author"`
	Committer *SignatureResult `json:"committer"`
	Message   string           `json:"message"`
}

func (r *CommitResult) String() string {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(r)
	return buf.String()
}

// A LogResult represents the result of a git log operation.
type LogResult struct {
	Log  []*CommitResult `json:"log,omitempty"`
	Next string          `json:"next,omitempty"`
}

func (r *LogResult) String() string {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(r)
	return buf.String()
}

// A TreeEntryResult represents one entry in a git tree.
type TreeEntryResult struct {
	Mode git.Filemode `json:"mode"`
	Type string       `json:"type"`
	ID   string       `json:"id"`
	Name string       `json:"name"`
}

// A TreeResult represents a git tree.
type TreeResult struct {
	ID      string             `json:"id"`
	Entries []*TreeEntryResult `json:"entries"`
}

func (r *TreeResult) String() string {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(r)
	return buf.String()
}

// A BlobResult represents a git blob.
type BlobResult struct {
	ID       string `json:"id"`
	Size     int64  `json:"size"`
	Contents string `json:"contents,omitempty"`
}

func (r *BlobResult) String() string {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(r)
	return buf.String()
}

func formatSignature(
	signature *git.Signature,
) *SignatureResult {
	return &SignatureResult{
		Name:  signature.Name,
		Email: signature.Email,
		Time:  signature.When.Format(time.RFC1123Z),
	}
}

func formatCommit(
	commit *git.Commit,
) *CommitResult {
	result := &CommitResult{
		Commit:    commit.Id().String(),
		Author:    formatSignature(commit.Author()),
		Committer: formatSignature(commit.Committer()),
		Message:   commit.Message(),
		Parents:   make([]string, commit.ParentCount()),
		Tree:      commit.TreeId().String(),
	}
	for i := uint(0); i < commit.ParentCount(); i++ {
		result.Parents[i] = commit.ParentId(i).String()
	}
	return result
}

func formatTreeEntry(
	entry *git.TreeEntry,
) *TreeEntryResult {
	return &TreeEntryResult{
		Mode: entry.Filemode,
		Type: strings.ToLower(entry.Type.String()),
		ID:   entry.Id.String(),
		Name: entry.Name,
	}
}

func formatTree(
	tree *git.Tree,
) *TreeResult {
	result := &TreeResult{
		ID:      tree.Id().String(),
		Entries: make([]*TreeEntryResult, tree.EntryCount()),
	}
	for i := uint64(0); i < tree.EntryCount(); i++ {
		result.Entries[i] = formatTreeEntry(tree.EntryByIndex(i))
	}
	return result
}

func formatBlob(
	blob *git.Blob,
) *BlobResult {
	result := &BlobResult{
		ID:   blob.Id().String(),
		Size: blob.Size(),
	}
	if result.Size < BlobDisplayMaxSize {
		result.Contents = base64.StdEncoding.EncodeToString(blob.Contents())
	}
	return result
}

func handleRefs(
	repository *git.Repository,
	level AuthorizationLevel,
	method string,
) (RefsResult, error) {
	it, err := repository.NewReferenceIterator()
	if err != nil {
		return nil, err
	}
	defer it.Free()

	result := make(RefsResult)

	head, err := repository.Head()
	if err == nil {
		defer head.Free()
		result["HEAD"] = &RefResult{
			Target: head.Name(),
			Value:  head.Target().String(),
		}
	}

	for {
		ref, err := it.Next()
		if err != nil {
			if git.IsErrorCode(err, git.ErrIterOver) {
				break
			}
			return nil, err
		}
		defer ref.Free()

		if level == AuthorizationAllowedRestricted && isRestrictedRef(ref.Name()) {
			continue
		}
		refResult := &RefResult{}
		if ref.Type() == git.ReferenceSymbolic {
			refResult.Target = ref.SymbolicTarget()
			target, err := ref.Resolve()
			if err != nil {
				return nil, err
			}
			defer target.Free()
			refResult.Value = target.Target().String()
		} else if ref.Type() == git.ReferenceOid {
			refResult.Value = ref.Target().String()
		}
		result[ref.Name()] = refResult
	}

	return result, nil
}

func handleLog(
	repository *git.Repository,
	requestPath string,
	method string,
) (*LogResult, error) {
	splitPath := strings.SplitN(requestPath, "/", 3)
	if len(splitPath) < 2 {
		return nil, ErrNotFound
	}
	rev := "HEAD"
	if len(splitPath) == 3 && len(splitPath[2]) != 0 {
		rev = splitPath[2]
	}
	obj, err := repository.RevparseSingle(rev)
	if err != nil {
		return nil, ErrNotFound
	}
	defer obj.Free()
	if obj.Type() != git.ObjectCommit {
		return nil, ErrNotFound
	}

	if method == "HEAD" {
		return nil, nil
	}

	walk, err := repository.Walk()
	if err != nil {
		return nil, err
	}
	defer walk.Free()
	walk.SimplifyFirstParent()
	if err = walk.Push(obj.Id()); err != nil {
		return nil, err
	}
	result := &LogResult{
		Log: make([]*CommitResult, 0),
	}
	err = walk.Iterate(func(commit *git.Commit) bool {
		defer commit.Free()
		if len(result.Log) > 100 {
			result.Next = commit.Id().String()
			return false
		}
		result.Log = append(result.Log, formatCommit(commit))
		return true
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func handleShow(
	repository *git.Repository,
	requestPath string,
	method string,
	acceptMIMEType string,
) (interface{}, error) {
	splitPath := strings.SplitN(requestPath, "/", 4)
	if len(splitPath) < 3 {
		return nil, ErrNotFound
	}
	rev := splitPath[2]
	if len(splitPath) == 3 {
		// Show commit
		obj, err := repository.RevparseSingle(rev)
		if err != nil {
			return nil, ErrNotFound
		}
		defer obj.Free()
		if obj.Type() != git.ObjectCommit {
			return nil, ErrNotFound
		}
		commit, err := obj.AsCommit()
		if err != nil {
			return nil, err
		}
		defer commit.Free()

		return formatCommit(commit), nil
	}

	// Show path
	obj, err := repository.RevparseSingle(fmt.Sprintf("%s:%s", rev, splitPath[3]))
	if err != nil {
		return nil, ErrNotFound
	}
	defer obj.Free()

	if method == "HEAD" {
		return nil, nil
	}

	if obj.Type() == git.ObjectTree {
		tree, err := obj.AsTree()
		if err != nil {
			return nil, err
		}
		defer tree.Free()

		return formatTree(tree), nil
	} else if obj.Type() == git.ObjectBlob {
		blob, err := obj.AsBlob()
		if err != nil {
			return nil, err
		}
		defer blob.Free()

		if acceptMIMEType == "application/octet-stream" {
			return blob.Contents(), nil
		}

		return formatBlob(blob), nil
	}

	return nil, ErrNotFound
}

func handleBrowse(
	repositoryPath string,
	level AuthorizationLevel,
	method string,
	acceptMIMEType string,
	requestPath string,
	log log15.Logger,
	w http.ResponseWriter,
) error {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		log.Error("Error opening git repository", "err", err)
		return err
	}
	defer repository.Free()

	lockfile := NewLockfile(repository.Path())
	if ok, err := lockfile.TryRLock(); !ok {
		log.Info("Waiting for the lockfile", "err", err)
		if err := lockfile.RLock(); err != nil {
			log.Crit("Failed to acquire the lockfile", "err", err)
			return err
		}
	}
	defer lockfile.Unlock()

	var result interface{}
	if requestPath == "/+refs" || requestPath == "/+refs/" {
		result, err = handleRefs(repository, level, method)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(requestPath, "/+log/") {
		result, err = handleLog(repository, requestPath, method)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(requestPath, "/+/") {
		result, err = handleShow(repository, requestPath, method, acceptMIMEType)
		if err != nil {
			return err
		}
	} else {
		return ErrNotFound
	}

	if method == "HEAD" {
		return nil
	}

	if rawBytes, ok := result.([]byte); ok {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(rawBytes)))
		_, err := w.Write(rawBytes)
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
