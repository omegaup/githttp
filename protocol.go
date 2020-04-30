package githttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go/v29"
	base "github.com/omegaup/go-base"
	"github.com/pkg/errors"
)

const (
	symrefHeadPrefix = "symref=HEAD:"

	// revWalkLimit is the maximum number of commits that will be considered to
	// determine whether this is a fast-forward push.
	revWalkLimit = 10000
)

var (
	pullCapabilities = Capabilities{"agent=gohttp", "allow-tip-sha1-in-want", "ofs-delta", "shallow", "thin-pack"}
	pushCapabilities = Capabilities{"agent=gohttp", "atomic", "ofs-delta", "report-status"}
)

// A Capabilities represents a set of git protocol capabilities.
type Capabilities []string

// Contains returns true if the provided capability name is contained within
// the Capabilities set.
func (c *Capabilities) Contains(capability string) bool {
	for _, cap := range *c {
		if cap == capability {
			return true
		}
	}
	return false
}

// Equal returns true if both capability sets are equal.
func (c *Capabilities) Equal(other Capabilities) bool {
	if len(*c) != len(other) {
		return false
	}
	for _, cap := range other {
		if !c.Contains(cap) {
			return false
		}
	}
	return true
}

// A GitCommand describes one of the smart protocol's update commands.
type GitCommand struct {
	Old, New         *git.Oid
	OldTree, NewTree *git.Oid
	ReferenceName    string
	Reference        *git.Reference
	err              error
	logMessage       string
}

// An UpdatedRef describes a reference that was updated.
type UpdatedRef struct {
	Name     string `json:"name"`
	From     string `json:"from"`
	To       string `json:"to"`
	FromTree string `json:"from_tree"`
	ToTree   string `json:"to_tree"`
}

// IsCreate returns whether the command creates a ref.
func (c *GitCommand) IsCreate() bool {
	return c.Old.IsZero()
}

// IsUpdate returns whether the command updates a ref.
func (c *GitCommand) IsUpdate() bool {
	return !c.Old.IsZero() && !c.New.IsZero()
}

// IsDelete returns whether the command deletes a ref.
func (c *GitCommand) IsDelete() bool {
	return c.New.IsZero()
}

// IsStaleRequest returns whether the command is requesting a stale operation:
// if this is a create command but the reference does exist, or it's not
// replacing the current branch's HEAD.
func (c *GitCommand) IsStaleRequest() bool {
	if c.IsCreate() {
		return c.Reference != nil
	}
	return !c.Old.Equal(c.Reference.Target())
}

func (c *GitCommand) String() string {
	return fmt.Sprintf(
		"{old: %s, oldTree: %s, new: %s, newTree: %s, reference: %s}",
		c.Old,
		c.OldTree,
		c.New,
		c.NewTree,
		c.ReferenceName,
	)
}

// A GitProtocol contains the callbacks needed to customize the behavior of
// GitServer.
type GitProtocol struct {
	AuthCallback               AuthorizationCallback
	ReferenceDiscoveryCallback ReferenceDiscoveryCallback
	UpdateCallback             UpdateCallback
	PreprocessCallback         PreprocessCallback
	AllowNonFastForward        bool
	log                        log15.Logger
}

// NewGitProtocol returns a new instance of GitProtocol.
func NewGitProtocol(
	authCallback AuthorizationCallback,
	referenceDiscoveryCallback ReferenceDiscoveryCallback,
	updateCallback UpdateCallback,
	preprocessCallback PreprocessCallback,
	allowNonFastForward bool,
	log log15.Logger,
) *GitProtocol {
	if authCallback == nil {
		authCallback = noopAuthorizationCallback
	}

	if referenceDiscoveryCallback == nil {
		referenceDiscoveryCallback = noopReferenceDiscoveryCallback
	}

	if updateCallback == nil {
		updateCallback = noopUpdateCallback
	}

	if preprocessCallback == nil {
		preprocessCallback = noopPreprocessCallback
	}

	return &GitProtocol{
		AuthCallback:               authCallback,
		ReferenceDiscoveryCallback: referenceDiscoveryCallback,
		UpdateCallback:             updateCallback,
		PreprocessCallback:         preprocessCallback,
		AllowNonFastForward:        allowNonFastForward,
		log:                        log,
	}
}

// PushPackfile unpacks the provided packfile (provided as an io.Reader), and
// updates the refs provided as commands into the repository.
func (p *GitProtocol) PushPackfile(
	ctx context.Context,
	repository *git.Repository,
	lockfile *Lockfile,
	level AuthorizationLevel,
	commands []*GitCommand,
	r io.Reader,
) (updatedRefs []UpdatedRef, err, unpackErr error) {
	odb, err := repository.Odb()
	if err != nil {
		err = errors.Wrap(err, "failed to open git odb")
		return nil, err, err
	}
	defer odb.Free()

	writepack, err := odb.NewWritePack(nil)
	if err != nil {
		err = errors.Wrap(err, "failed to create writepack")
		return nil, err, err
	}
	defer writepack.Free()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("packfile_%s", path.Base(repository.Path())))
	if err != nil {
		err = errors.Wrap(err, "failed to create temporary directory")
		return nil, err, err
	}
	defer os.RemoveAll(tmpDir)

	_, packPath, err := UnpackPackfile(odb, r, tmpDir, nil)

	if err != nil {
		err = errors.Wrap(err, "failed to unpack")
		return nil, err, err
	}

	for _, command := range commands {
		if command.err == nil {
			commit, err := repository.LookupCommit(command.New)
			if err != nil {
				command.err = ErrUnknownCommit
			} else {
				command.NewTree = commit.TreeId()
				command.logMessage = commit.Summary()
				// These error don't need wrapping since they are presented in the
				// context of the ref they refer to.
				if !ValidateFastForward(repository, commit, command.Reference) && !p.AllowNonFastForward {
					command.err = ErrNonFastForward
				} else if level == AuthorizationAllowedRestricted && isRestrictedRef(command.ReferenceName) {
					p.log.Info("restricted ref", "ref", command.ReferenceName)
					command.err = ErrRestrictedRef
				} else if !p.ReferenceDiscoveryCallback(ctx, repository, command.ReferenceName) {
					p.log.Info("user does not have access", "ref", command.ReferenceName)
					command.err = ErrRestrictedRef
				} else {
					parentCommit := commit.Parent(0)
					if err = p.UpdateCallback(
						ctx,
						repository,
						level,
						command,
						parentCommit,
						commit,
					); err != nil {
						command.err = err
					}
					if parentCommit != nil {
						parentCommit.Free()
					}
				}
				commit.Free()
			}
		}
		if command.err != nil {
			return nil, base.ErrorWithCategory(ErrBadRequest, command.err), nil
		}
	}

	originalCommands := commands
	packPath, commands, err = p.PreprocessCallback(
		ctx,
		repository,
		tmpDir,
		packPath,
		originalCommands,
	)
	if err != nil {
		return nil, base.ErrorWithCategory(ErrBadRequest, err), nil
	}

	if ok, err := lockfile.TryLock(); !ok {
		p.log.Info("Waiting for the lockfile", "err", err)
		if err := lockfile.Lock(); err != nil {
			return nil, errors.Wrap(
				err,
				"failed to acquire the lockfile",
			), nil
		}
	}

	err = commitPackfile(packPath, writepack)
	if err != nil {
		return nil, errors.Wrap(err, "failed to commit packfile"), nil
	}

	err = odb.Refresh()
	if err != nil {
		return nil, errors.Wrap(err, "failed to refresh odb"), nil
	}
	err = odb.WriteMultiPackIndex()
	if err != nil {
		return nil, errors.Wrap(err, "failed to write multi-pack-index"), nil
	}

	updatedRefs = make([]UpdatedRef, 0)
	for _, command := range commands {
		ref, err := repository.References.Create(
			command.ReferenceName,
			command.New,
			true,
			command.logMessage,
		)
		if err != nil {
			command.err = err
			return nil, base.ErrorWithCategory(ErrBadRequest, errors.Wrapf(
				err,
				"failed to update reference %s",
				command.ReferenceName,
			)), nil
		}
		updatedRef := UpdatedRef{
			Name:   command.ReferenceName,
			To:     command.New.String(),
			ToTree: command.NewTree.String(),
		}
		if command.Old != nil && !command.Old.IsZero() {
			updatedRef.From = command.Old.String()
			if command.OldTree != nil {
				updatedRef.FromTree = command.OldTree.String()
			}
		} else {
			updatedRef.From = (&git.Oid{}).String()
			updatedRef.FromTree = (&git.Oid{}).String()
		}
		updatedRefs = append(updatedRefs, updatedRef)
		ref.Free()
		p.log.Info("Ref successfully updated", "command", command)
	}

	return updatedRefs, nil, nil
}

// A ReferenceDiscovery represents the result of the reference discovery
// negotiation in git's pack protocol.
type ReferenceDiscovery struct {
	References   map[string]git.Oid
	Capabilities Capabilities
	HeadSymref   string
}

// DiscoverReferences returns the result of the reference discovery negotiation
// in git's pack protocol. This negotiation is documented in
// https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt
func DiscoverReferences(r io.Reader) (*ReferenceDiscovery, error) {
	discovery := &ReferenceDiscovery{
		References:   make(map[string]git.Oid),
		Capabilities: make(Capabilities, 0),
	}
	pr := NewPktLineReader(r)
	for {
		line, err := pr.ReadPktLine()
		if err != nil {
			if err == ErrFlush {
				break
			}
			return nil, err
		}
		if bytes.HasPrefix(line, []byte("# service=")) {
			// This is most likely the first line of the reference discovery. Skip
			// this line and the next one, which _must_ be a flush.
			if _, err = pr.ReadPktLine(); err != ErrFlush {
				return nil, err
			}
			continue
		}
		// Only the first line will have a '\x00' byte, that separates the
		// reference name from the capabilities, but this is simpler.
		tokens := strings.FieldsFunc(
			strings.Trim(string(line), "\n"),
			func(r rune) bool {
				return r == ' ' || r == '\x00'
			},
		)
		oid, err := git.NewOid(tokens[0])
		if err != nil {
			return nil, err
		}
		discovery.References[tokens[1]] = *oid
		if len(tokens) >= 3 {
			discovery.Capabilities = tokens[2:]
		}
	}

	// The server can optionally tell the client what branch to check out upon
	// cloning.
	for _, capability := range discovery.Capabilities {
		if strings.HasPrefix(capability, symrefHeadPrefix) {
			discovery.HeadSymref = capability[len(symrefHeadPrefix):]
			break
		}
	}

	return discovery, nil
}

// ValidateFastForward returns whether there is a chain of left parent commits
// that lead to:
// * The target of the reference (if it exists).
// * The commit pointed to by HEAD (if it is an unborn branch, and it exists).
// * An unborn branch if there is no HEAD.
func ValidateFastForward(
	repository *git.Repository,
	commit *git.Commit,
	ref *git.Reference,
) bool {
	if ref == nil {
		// This is an unborn branch.
		return true
	}
	target := ref.Target()
	// There should be a chain of first parents that lead to the branch's current
	// HEAD.
	parentID := commit.ParentId(0)
	revWalkCount := 1
	for parentID != nil {
		revWalkCount++
		if revWalkCount > revWalkLimit {
			// Bail out, this check was too expensive.
			return false
		}
		if parentID.Equal(target) {
			return true
		}
		parentCommit, err := repository.LookupCommit(parentID)
		if err != nil {
			return false
		}
		if commit.ParentCount() == 0 {
			parentID = nil
		} else {
			parentID = parentCommit.ParentId(0)
		}
		parentCommit.Free()
	}
	return false
}

// isRestrictedRef returns whether a ref name is restricted. Only
// `refs/meta/config` is restricted.
func isRestrictedRef(name string) bool {
	return name == "refs/meta/config"
}

// commitPackfile commits the packfile into the repository.
func commitPackfile(packPath string, writepack *git.OdbWritepack) error {
	f, err := os.Open(packPath)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", packPath)
	}
	defer f.Close()

	if _, err := io.Copy(writepack, f); err != nil {
		return errors.Wrap(err, "failed to write into the writepack")
	}

	return writepack.Commit()
}

// handleInfoRefs handles git's pack-protocol reference discovery (or the
// '/info/refs' URL). This tells the client what references the server knows
// aboutells the client what references the server knows about so it can choose
// what references to push/pull.
func handleInfoRefs(
	ctx context.Context,
	repositoryPath string,
	serviceName string,
	capabilities Capabilities,
	sendSymref bool,
	sendCapabilities bool,
	level AuthorizationLevel,
	protocol *GitProtocol,
	log log15.Logger,
	w io.Writer,
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
		log.Info("Waiting for the lockfile", "err", err)
		if err := lockfile.RLock(); err != nil {
			return errors.Wrap(
				err,
				"failed to acquire the lockfile",
			)
		}
	}
	defer lockfile.Unlock()

	it, err := repository.NewReferenceIterator()
	if err != nil {
		return errors.Wrap(
			err,
			"failed to read references",
		)
	}
	defer it.Free()

	head, err := repository.Head()
	if err != nil && !git.IsErrorCode(err, git.ErrUnbornBranch) {
		return errors.Wrap(
			err,
			"failed to read HEAD",
		)
	}
	if head != nil {
		defer head.Free()
	}

	p := NewPktLineWriter(w)
	defer p.Close()

	// As opposed to the git protocol, the HTTP protocol sends this comment
	// followed by a flush.
	p.WritePktLine([]byte(fmt.Sprintf("# service=%s\n", serviceName)))
	p.Flush()

	sentCapabilities := false
	if sendSymref && head != nil {
		p.WritePktLine([]byte(fmt.Sprintf(
			"%s HEAD\x00%s %s%s\n",
			head.Target().String(),
			strings.Join(capabilities, " "),
			symrefHeadPrefix,
			head.Name(),
		)))
		sentCapabilities = true
	}
	for {
		ref, err := it.Next()
		if err != nil {
			if !git.IsErrorCode(err, git.ErrIterOver) {
				log.Error("Error getting reference", "err", err)
			}
			break
		}
		defer ref.Free()
		if level == AuthorizationAllowedRestricted && isRestrictedRef(ref.Name()) {
			continue
		}
		if !protocol.ReferenceDiscoveryCallback(ctx, repository, ref.Name()) {
			continue
		}
		if sentCapabilities {
			p.WritePktLine([]byte(fmt.Sprintf(
				"%s %s\n",
				ref.Target().String(),
				ref.Name(),
			)))
		} else {
			p.WritePktLine([]byte(fmt.Sprintf(
				"%s %s\x00%s\n",
				ref.Target().String(),
				ref.Name(),
				strings.Join(capabilities, " "),
			)))
			sentCapabilities = true
		}
	}
	if sendCapabilities && !sentCapabilities {
		p.WritePktLine([]byte(fmt.Sprintf(
			"%s capabilities^{}\x00%s\n",
			(&git.Oid{}).String(),
			strings.Join(capabilities, " "),
		)))
	}
	return nil
}

// handlePrePull handles git's pack-protocol pre-pull (or 'git-upload-pack'
// service with /info/refs URL). This performs the server-side reference
// discovery.
func handlePrePull(
	ctx context.Context,
	repositoryPath string,
	level AuthorizationLevel,
	protocol *GitProtocol,
	log log15.Logger,
	w io.Writer,
) error {
	return handleInfoRefs(
		ctx,
		repositoryPath,
		"git-upload-pack",
		pullCapabilities,
		true,
		false,
		level,
		protocol,
		log,
		w,
	)
}

// handlePull handles git's pack-protocol pull (or 'git-upload-pack' with the
// '/git-upload-pack' URL). This performs the negotiation of commits that will
// be sent and replies to the client with a packfile with all the objects
// contained in the requested commits.
func handlePull(
	repositoryPath string,
	level AuthorizationLevel,
	log log15.Logger,
	r io.Reader,
	w io.Writer,
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
		log.Info("Waiting for the lockfile", "err", err)
		if err := lockfile.RLock(); err != nil {
			return errors.Wrap(
				err,
				"failed to acquire the lockfile",
			)
		}
	}
	defer lockfile.Unlock()

	pb, err := repository.NewPackbuilder()
	if err != nil {
		return errors.Wrap(
			err,
			"failed to create packbuilder",
		)
	}
	defer pb.Free()

	pr := NewPktLineReader(r)
	wantMap := make(map[string]*git.Commit)
	commonSet := make(map[string]struct{})
	haveSet := make(map[string]struct{})
	shallowSet := make(map[string]struct{})
	acked := false
	done := false
	maxDepth := uint64(0)
	for {
		line, err := pr.ReadPktLine()
		if err == ErrFlush {
			break
		} else if err != nil {
			return base.ErrorWithCategory(
				ErrBadRequest,
				errors.Wrap(
					err,
					"failed to read the request",
				),
			)
		}
		log.Debug("pktline", "data", strings.Trim(string(line), "\n"))
		tokens := strings.FieldsFunc(
			strings.Trim(string(line), "\n"),
			func(r rune) bool {
				return r == ' ' || r == '\x00'
			},
		)
		if len(tokens) > 2 {
			for _, cap := range tokens[2:] {
				if strings.Contains(cap, "=") {
					continue
				}
				if !pullCapabilities.Contains(cap) {
					return base.ErrorWithCategory(
						ErrBadRequest,
						errors.Errorf(
							"unsupported capability %s",
							cap,
						),
					)
				}
			}
			log.Debug("client capabilities", "list", tokens[2:])
		}
		if tokens[0] == "want" {
			if len(tokens) < 2 {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.New("malformed 'want' pkt-line"),
				)
			}
			oid, err := git.NewOid(tokens[1])
			if err != nil {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.Errorf("invalid OID: %s", tokens[1]),
				)
			}
			commit, err := repository.LookupCommit(oid)
			if err != nil {
				log.Debug("Unknown commit requested", "oid", tokens[1])
				pw := NewPktLineWriter(w)
				pw.WritePktLine([]byte(fmt.Sprintf("ERR upload-pack: not our ref %s", oid.String())))
				return nil
			}
			defer commit.Free()
			wantMap[tokens[1]] = commit
		} else if tokens[0] == "shallow" {
			if len(tokens) < 2 {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.New("malformed 'shallow' pkt-line"),
				)
			}
			shallowSet[tokens[1]] = struct{}{}
		} else if tokens[0] == "deepen" {
			if len(tokens) < 2 {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.New("malformed 'deepen' pkt-line"),
				)
			}
			maxDepth, err = strconv.ParseUint(tokens[1], 10, 64)
			if err != nil {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.Errorf("invalid depth %s", tokens[1]),
				)
			}
		} else {
			log.Debug("unknown command", "command", tokens[0])
		}
	}

	// TODO(lhchavez): Move this after we commit to sending a successful reply.
	pw := NewPktLineWriter(w)
	if maxDepth == 0 {
		maxDepth = uint64(math.MaxUint64)
	} else {
		for _, want := range wantMap {
			depth := maxDepth
			for current := want; current != nil && depth > 0; current = current.Parent(0) {
				if current != want {
					defer current.Free()
				}
				depth--
				if depth == 0 && current.ParentCount() != 0 {
					pw.WritePktLine([]byte(fmt.Sprintf("shallow %s\n", current.Id().String())))
					break
				}
				if _, ok := shallowSet[current.Id().String()]; ok {
					pw.WritePktLine([]byte(fmt.Sprintf("unshallow %s\n", current.Id().String())))
				}
			}
		}
		pw.Flush()
	}

	for {
		line, err := pr.ReadPktLine()
		if err == ErrFlush || err == io.EOF {
			break
		} else if err != nil {
			return base.ErrorWithCategory(
				ErrBadRequest,
				errors.Wrap(
					err,
					"failed to read request",
				),
			)
		}
		log.Debug("pktline", "data", strings.Trim(string(line), "\n"))
		tokens := strings.FieldsFunc(
			strings.Trim(string(line), "\n"),
			func(r rune) bool {
				return r == ' ' || r == '\x00'
			},
		)
		if tokens[0] == "done" {
			done = true
			break
		} else if tokens[0] == "have" {
			if len(tokens) < 2 {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.New("malformed 'have' pkt-line"),
				)
			}
			oid, err := git.NewOid(tokens[1])
			if err != nil {
				return base.ErrorWithCategory(
					ErrBadRequest,
					errors.Errorf("invalid OID: %s", tokens[1]),
				)
			}
			commit, err := repository.LookupCommit(oid)
			if err == nil {
				commit.Free()
				if !acked {
					acked = true
					pw.WritePktLine([]byte(fmt.Sprintf("ACK %s\n", tokens[1])))
				}
				commonSet[tokens[1]] = struct{}{}
			} else {
				haveSet[tokens[1]] = struct{}{}
			}
		}
	}

	log.Debug("Negotiation", "want", wantMap, "have", haveSet, "common", commonSet)

	if !done {
		log.Debug("missing 'done' pkt-line")
		return nil
	}

	for _, want := range wantMap {
		depth := maxDepth
		for current := want; current != nil && depth > 0; current = current.Parent(0) {
			if current != want {
				defer current.Free()
			}
			depth--
			if _, ok := shallowSet[current.Id().String()]; ok {
				log.Debug("Skipping commit", "commit", current.Id().String())
				continue
			}
			if _, ok := commonSet[current.Id().String()]; ok {
				break
			}
			log.Debug("Adding commit", "commit", current.Id().String())
			if err := pb.InsertCommit(current.Id()); err != nil {
				return errors.Wrap(
					err,
					"failed to build packfile",
				)
			}
		}
	}

	if !acked {
		pw.WritePktLine([]byte("NAK\n"))
	}
	if err := pb.Write(w); err != nil {
		log.Error("Error writing pack", "err", err)
	}

	return nil
}

// handlePrePush handles git's pack-protocol pre-push (or 'git-receive-pack'
// with the '/info/refs' URL). This performs the negotiation of commits that
// will be sent to the server and replies to the client with the list of
// references it can update.
func handlePrePush(
	ctx context.Context,
	repositoryPath string,
	level AuthorizationLevel,
	protocol *GitProtocol,
	log log15.Logger,
	w io.Writer,
) error {
	return handleInfoRefs(
		ctx,
		repositoryPath,
		"git-receive-pack",
		pushCapabilities,
		false,
		true,
		level,
		protocol,
		log,
		w,
	)
}

// handlePush handles git's pack-protocol push (or 'git-receive-pack' with the
// '/git-receive-pack' URL). This performs validations on the uploaded packfile
// and commits the change if it is allowed.
func handlePush(
	ctx context.Context,
	repositoryPath string,
	level AuthorizationLevel,
	protocol *GitProtocol,
	log log15.Logger,
	r io.Reader,
	w io.Writer,
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
		log.Info("Waiting for the lockfile", "err", err)
		if err := lockfile.RLock(); err != nil {
			return errors.Wrap(
				err,
				"failed to acquire the lockfile",
			)
		}
	}
	defer lockfile.Unlock()

	pr := NewPktLineReader(r)
	reportStatus := false
	commands := make([]*GitCommand, 0)
	references := make(map[string]*git.Reference)
	for {
		line, err := pr.ReadPktLine()
		if err == ErrFlush {
			break
		} else if err != nil {
			return base.ErrorWithCategory(
				ErrBadRequest,
				errors.Wrap(
					err,
					"failed to read the request",
				),
			)
		}
		tokens := strings.FieldsFunc(
			strings.Trim(string(line), "\n"),
			func(r rune) bool {
				return r == ' ' || r == '\x00'
			},
		)
		if len(tokens) < 3 {
			return base.ErrorWithCategory(
				ErrBadRequest,
				errors.Errorf("failed to parse command %v", tokens),
			)
		}
		if len(tokens) > 3 {
			log.Debug("client capabilities", "list", tokens[3:])
			for _, token := range tokens[3:] {
				if token == "report-status" {
					reportStatus = true
					break
				}
			}
		}
		command := &GitCommand{
			ReferenceName: tokens[2],
		}
		if _, ok := references[command.ReferenceName]; !ok {
			ref, err := repository.References.Lookup(command.ReferenceName)
			if err == nil {
				defer ref.Free()
			}
			references[command.ReferenceName] = ref
		}
		command.Reference = references[command.ReferenceName]
		commands = append(commands, command)
		if command.Old, err = git.NewOid(tokens[0]); err != nil {
			command.err = ErrInvalidOldOid
		} else if command.New, err = git.NewOid(tokens[1]); err != nil {
			command.err = ErrInvalidNewOid
		} else if command.IsStaleRequest() {
			command.err = ErrStaleInfo
		} else if command.IsDelete() {
			command.err = ErrDeleteUnallowed
		}
	}

	log.Debug("Commands", "commands", commands)

	_, err, unpackErr := protocol.PushPackfile(
		ctx,
		repository,
		lockfile,
		level,
		commands,
		r,
	)
	if !reportStatus {
		return err
	}

	pw := NewPktLineWriter(w)
	defer pw.Flush()

	if unpackErr == nil {
		pw.WritePktLine([]byte("unpack ok\n"))
	} else {
		pw.WritePktLine([]byte(fmt.Sprintf("unpack %s\n", unpackErr.Error())))
	}
	for _, command := range commands {
		if command.err != nil {
			pw.WritePktLine([]byte(fmt.Sprintf(
				"ng %s %s\n",
				command.ReferenceName,
				command.err.Error(),
			)))
		} else if unpackErr != nil {
			pw.WritePktLine([]byte(fmt.Sprintf(
				"ng %s unpack-failed\n",
				command.ReferenceName,
			)))
		} else if err != nil {
			pw.WritePktLine([]byte(fmt.Sprintf(
				"ng %s %s\n",
				command.ReferenceName,
				err.Error(),
			)))
		} else {
			pw.WritePktLine([]byte(fmt.Sprintf(
				"ok %s\n",
				command.ReferenceName,
			)))
		}
	}

	return nil
}
