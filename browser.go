package githttp

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	git "github.com/lhchavez/git2go/v32"
	base "github.com/omegaup/go-base/v2"
	"github.com/pkg/errors"
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
	Size int64        `json:"size"`
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

// formatTree reads the raw git tree data, parses it, and looks up the file
// size for all the blobs in the tree. This is done to avoid having to make ~5
// cgo calls per entry, which makes things a bit faster.
func formatTree(
	repository *git.Repository,
	treeID *git.Oid,
) (*TreeResult, error) {
	odb, err := repository.Odb()
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to get odb for repository",
		)
	}
	defer odb.Free()
	odbObj, err := odb.Read(treeID)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to lookup %s",
			treeID,
		)
	}
	defer odbObj.Free()

	result := &TreeResult{
		ID: treeID.String(),
	}
	treeData := odbObj.Data()
	for len(treeData) > 0 {
		idx := bytes.IndexRune(treeData, ' ')
		if idx == -1 {
			return nil, fmt.Errorf("malformed tree %s: no space", treeID)
		}
		mode, err := strconv.ParseInt(string(treeData[:idx]), 8, 32)
		if err != nil {
			return nil, fmt.Errorf("malformed tree %s: no mode", treeID)
		}
		treeData = treeData[idx+1:]
		idx = bytes.IndexByte(treeData, 0)
		if idx == -1 || len(treeData) < idx+1+len(git.Oid{}) {
			return nil, fmt.Errorf("malformed tree %s: no name", treeID)
		}
		name := string(treeData[:idx])
		treeData = treeData[idx+1:]
		oid := git.NewOidFromBytes(treeData)
		treeData = treeData[len(oid):]

		treeEntryResult := &TreeEntryResult{
			Mode: git.Filemode(mode),
			ID:   oid.String(),
			Name: name,
		}
		result.Entries = append(result.Entries, treeEntryResult)

		if mode == 0o160000 {
			treeEntryResult.Type = "commit"
		} else if (mode & 0o100000) != 0 {
			treeEntryResult.Type = "blob"
			size, _, err := odb.ReadHeader(oid)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to lookup blob %s:%s", oid, name)
			}
			treeEntryResult.Size = int64(size)
		} else {
			treeEntryResult.Type = "tree"
		}
	}
	return result, nil
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

// isCommitIDReachable returns whether a particular commit ID is reachable from any
// of the refs that are viewable by the requestor.
func isCommitIDReachable(
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	protocol *GitProtocol,
	commitID *git.Oid,
) error {
	it, err := repository.NewReferenceIterator()
	if err != nil {
		return errors.Wrap(
			err,
			"failed to create a reference iterator",
		)
	}
	defer it.Free()

	oids := []*git.Oid{commitID}
	for {
		ref, err := it.Next()
		if err != nil {
			if git.IsErrorCode(err, git.ErrorCodeIterOver) {
				break
			}
			return errors.Wrap(
				err,
				"failed to get an entry from the reference iterator",
			)
		}
		defer ref.Free()

		if level == AuthorizationAllowedRestricted && isRestrictedRef(ref.Name()) {
			continue
		}
		if !protocol.ReferenceDiscoveryCallback(ctx, repository, ref.Name()) {
			continue
		}

		oids = append(oids, ref.Target())
	}

	_, err = repository.MergeBaseMany(oids)
	if err != nil {
		// Even though the commit itself exists, we tell the caller that it
		// doesn't, since it was not reachable from any of the references that they
		// can view.
		return base.ErrorWithCategory(
			ErrNotFound,
			errors.Errorf(
				"commit %s not reachable from any of the viewable references: %v %v",
				commitID.String(),
				oids,
				err,
			),
		)
	}

	return nil
}

func handleRefs(
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	protocol *GitProtocol,
	method string,
) (RefsResult, error) {
	it, err := repository.NewReferenceIterator()
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to create a reference iterator",
		)
	}
	defer it.Free()

	result := make(RefsResult)

	head, err := repository.Head()
	if err == nil {
		defer head.Free()
	}

	for {
		ref, err := it.Next()
		if err != nil {
			if git.IsErrorCode(err, git.ErrorCodeIterOver) {
				break
			}
			return nil, errors.Wrap(
				err,
				"failed to get an entry from the reference iterator",
			)
		}
		defer ref.Free()

		if level == AuthorizationAllowedRestricted && isRestrictedRef(ref.Name()) {
			continue
		}
		if !protocol.ReferenceDiscoveryCallback(ctx, repository, ref.Name()) {
			continue
		}
		if head != nil && head.Name() == ref.Name() {
			result["HEAD"] = &RefResult{
				Target: head.Name(),
				Value:  head.Target().String(),
			}
		}
		refResult := &RefResult{}
		if ref.Type() == git.ReferenceSymbolic {
			refResult.Target = ref.SymbolicTarget()
			target, err := ref.Resolve()
			if err != nil {
				return nil, errors.Wrapf(
					err,
					"failed to resolve the symbolic target for %s(%s)",
					ref.Name(),
					ref.Target(),
				)
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
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	protocol *GitProtocol,
	requestPath string,
	method string,
) (*LogResult, error) {
	splitPath := strings.SplitN(requestPath, "/", 3)
	if len(splitPath) < 2 {
		return nil, base.ErrorWithCategory(
			ErrNotFound,
			errors.Errorf("invalid path: %s", requestPath),
		)
	}
	rev := "HEAD"
	if len(splitPath) == 3 && len(splitPath[2]) != 0 {
		rev = splitPath[2]
	}
	obj, err := repository.RevparseSingle(rev)
	if err != nil {
		return nil, base.ErrorWithCategory(
			ErrNotFound,
			errors.Wrapf(
				err,
				"failed to parse revision %s",
				rev,
			),
		)
	}
	defer obj.Free()
	if obj.Type() != git.ObjectCommit {
		return nil, base.ErrorWithCategory(
			ErrNotFound,
			errors.Wrapf(
				err,
				"revision %s is not a commit: %v",
				rev,
				obj.Type(),
			),
		)
	}

	if err := isCommitIDReachable(
		ctx,
		repository,
		level,
		protocol,
		obj.Id(),
	); err != nil {
		return nil, err
	}

	if method == "HEAD" {
		return nil, nil
	}

	walk, err := repository.Walk()
	if err != nil {
		return nil, errors.Wrap(
			err,
			"failed to create the repository revwalk",
		)
	}
	defer walk.Free()
	walk.SimplifyFirstParent()
	if err = walk.Push(obj.Id()); err != nil {
		return nil, errors.Wrap(
			err,
			"failed to add the original object to the revwalk",
		)
	}
	result := &LogResult{
		Log: make([]*CommitResult, 0),
	}
	if err := walk.Iterate(func(commit *git.Commit) bool {
		defer commit.Free()
		if len(result.Log) > 100 {
			result.Next = commit.Id().String()
			return false
		}
		result.Log = append(result.Log, formatCommit(commit))
		return true
	}); err != nil {
		return nil, errors.Wrap(
			err,
			"failed to walk the repository",
		)
	}

	return result, nil
}

func handleArchive(
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	protocol *GitProtocol,
	requestPath string,
	method string,
	w http.ResponseWriter,
) error {
	splitPath := strings.SplitN(requestPath, "/", 3)
	if len(splitPath) < 3 {
		return base.ErrorWithCategory(
			ErrNotFound,
			errors.Errorf("invalid path: %s", requestPath),
		)
	}
	rev := ""
	contentType := ""
	for extension, mimeType := range map[string]string{
		".zip": "application/zip",
	} {
		if !strings.HasSuffix(splitPath[2], extension) {
			continue
		}

		rev = strings.TrimSuffix(splitPath[2], extension)
		contentType = mimeType
		break
	}
	if rev == "" {
		return base.ErrorWithCategory(
			ErrNotFound,
			errors.New("empty revision"),
		)
	}
	obj, err := repository.RevparseSingle(rev)
	if err != nil {
		return base.ErrorWithCategory(
			ErrNotFound,
			errors.Wrapf(
				err,
				"failed to parse revision %s",
				rev,
			),
		)
	}
	defer obj.Free()
	if obj.Type() != git.ObjectCommit {
		return base.ErrorWithCategory(
			ErrNotFound,
			errors.Wrapf(
				err,
				"revision %s is not a commit: %v",
				rev,
				obj.Type(),
			),
		)
	}

	if err := isCommitIDReachable(
		ctx,
		repository,
		level,
		protocol,
		obj.Id(),
	); err != nil {
		return err
	}

	if method == "HEAD" {
		return nil
	}

	commit, err := obj.AsCommit()
	if err != nil {
		return errors.Wrapf(
			err,
			"failed to get object for %s",
			rev,
		)
	}
	defer commit.Free()
	tree, err := commit.Tree()
	if err != nil {
		return errors.Wrap(
			err,
			"failed to get the commit's tree",
		)
	}
	defer tree.Free()

	w.Header().Set("Content-Type", contentType)
	z := zip.NewWriter(w)
	defer z.Close()

	err = tree.Walk(func(parent string, entry *git.TreeEntry) error {
		fullPath := path.Join(parent, entry.Name)
		if entry.Type == git.ObjectTree {
			_, err := z.CreateHeader(&zip.FileHeader{
				Name: fullPath + "/",
			})
			if err != nil {
				return errors.Wrap(
					err,
					"failed to create zip header",
				)
			}
			return nil
		}

		// Object is a blob.
		w, err := z.Create(fullPath)
		if err != nil {
			return errors.Wrap(
				err,
				"failed to create zip writer",
			)
		}

		blob, err := repository.LookupBlob(entry.Id)
		if err != nil {
			return errors.Wrapf(
				err,
				"failed to lookup object %s",
				entry.Id,
			)
		}
		defer blob.Free()

		if _, err := w.Write(blob.Contents()); err != nil {
			return errors.Wrapf(
				err,
				"failed to write object %s",
				entry.Id,
			)
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(
			err,
			"failed to walk the repository",
		)
	}
	return nil
}

func handleShow(
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	protocol *GitProtocol,
	requestPath string,
	method string,
	acceptMIMEType string,
) (interface{}, error) {
	splitPath := strings.SplitN(requestPath, "/", 4)
	if len(splitPath) < 3 {
		return nil, base.ErrorWithCategory(
			ErrNotFound,
			errors.Errorf("invalid path: %q", requestPath),
		)
	}
	rev := splitPath[2]

	obj, err := repository.RevparseSingle(rev)
	if err != nil {
		return nil, base.ErrorWithCategory(
			ErrNotFound,
			errors.Wrapf(
				err,
				"failed to parse revision %s",
				rev,
			),
		)
	}
	defer obj.Free()

	if obj.Type() == git.ObjectCommit {
		if err := isCommitIDReachable(
			ctx,
			repository,
			level,
			protocol,
			obj.Id(),
		); err != nil {
			fmt.Printf("%v\n", rev)
			return nil, err
		}

		if len(splitPath) > 3 {
			// URLs of the form /+/rev/path. This shows either a tree or a blob.
			rev = fmt.Sprintf("%s:%s", rev, splitPath[3])
			obj, err = repository.RevparseSingle(rev)
			if err != nil {
				return nil, base.ErrorWithCategory(
					ErrNotFound,
					errors.Wrapf(
						err,
						"failed to parse revision %s",
						rev,
					),
				)
			}
			defer obj.Free()
		}
	} else {
		// URLs of the form /+/objectid. Shows an object, typically a commit referenced
		// by one of the named revisions (the ones that gitrevisions(7) supports),
		// or any other object by its SHA-1 name.
		if len(splitPath) != 3 {
			return nil, base.ErrorWithCategory(
				ErrNotFound,
				errors.Wrapf(
					err,
					"cannot use paths with an object-id for %q",
					splitPath,
				),
			)
		}
	}

	if method == "HEAD" {
		return nil, nil
	}

	if obj.Type() == git.ObjectCommit {
		commit, err := obj.AsCommit()
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"failed to get the commit for %s",
				rev,
			)
		}
		defer commit.Free()

		return formatCommit(commit), nil
	} else if obj.Type() == git.ObjectTree {
		return formatTree(repository, obj.Id())
	} else if obj.Type() == git.ObjectBlob {
		blob, err := obj.AsBlob()
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"failed to get blob for %s",
				rev,
			)
		}
		defer blob.Free()

		if acceptMIMEType == "application/octet-stream" {
			return blob.Contents(), nil
		}

		return formatBlob(blob), nil
	}

	return nil, base.ErrorWithCategory(
		ErrNotFound,
		errors.Errorf(
			"invalid show action for object type %s for revision %s",
			obj.Type(),
			rev,
		),
	)
}

func handleBrowse(
	ctx context.Context,
	repositoryPath string,
	level AuthorizationLevel,
	protocol *GitProtocol,
	method string,
	acceptMIMEType string,
	requestPath string,
	w http.ResponseWriter,
) error {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return errors.Wrap(
			err,
			"failed to open git repository",
		)
	}
	defer repository.Free()

	lockfile := NewLockfile(repository.Path())
	if ok, err := lockfile.TryRLock(); !ok {
		protocol.log.Info("Waiting for the lockfile", "err", err)
		if err := lockfile.RLock(); err != nil {
			protocol.log.Crit("Failed to acquire the lockfile", "err", err)
			return err
		}
	}
	defer lockfile.Unlock()

	var result interface{}
	if requestPath == "/+refs" || requestPath == "/+refs/" {
		result, err = handleRefs(ctx, repository, level, protocol, method)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(requestPath, "/+log/") {
		result, err = handleLog(ctx, repository, level, protocol, requestPath, method)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(requestPath, "/+archive/") {
		err = handleArchive(ctx, repository, level, protocol, requestPath, method, w)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(requestPath, "/+/") {
		result, err = handleShow(ctx, repository, level, protocol, requestPath, method, acceptMIMEType)
		if err != nil {
			return err
		}
	} else {
		return base.ErrorWithCategory(
			ErrNotFound,
			errors.Errorf(
				"handler not found for path %s",
				requestPath,
			),
		)
	}

	if method == "HEAD" || result == nil {
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
