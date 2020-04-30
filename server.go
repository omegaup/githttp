package githttp

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go/v29"
	base "github.com/omegaup/go-base"
)

// A GitOperation describes the current operation
type GitOperation int

const (
	// OperationPull denotes a pull operation.
	OperationPull GitOperation = iota

	// OperationPush denotes a push operation.
	OperationPush

	// OperationBrowse denotes a browse request.
	OperationBrowse
)

var (
	// ErrBadRequest is returned when the client sends a bad request. HTTP 400
	// will be returned to http clients.
	ErrBadRequest = stderrors.New("bad-request")

	// ErrForbidden is returned if an operation is not allowed. HTTP 403 will be
	// returned to http clients.
	ErrForbidden = stderrors.New("forbidden")

	// ErrNotFound is returned if a reference is not found. HTTP 404 will be
	// returned to http clients.
	ErrNotFound = stderrors.New("not-found")

	// ErrDeleteDisallowed is returned when a delete operation is attempted.
	ErrDeleteDisallowed = stderrors.New("delete-disallowed")

	// ErrInvalidRef is returned if a reference that the system does not support
	// is attempted to be modified.
	ErrInvalidRef = stderrors.New("invalid-ref")

	// ErrReadOnlyRef is returned if a read-only reference is attempted to be
	// modified.
	ErrReadOnlyRef = stderrors.New("read-only")

	// ErrRestrictedRef is returned if a restricted reference is attempted to be
	// modified.
	ErrRestrictedRef = stderrors.New("restricted-ref")

	// ErrDeleteUnallowed is returned if a reference is attempted to be deleted.
	ErrDeleteUnallowed = stderrors.New("delete-unallowed")

	// ErrUnknownCommit is returned if the user is attempting to update a ref
	// with an unknown commit.
	ErrUnknownCommit = stderrors.New("unknown-commit")

	// ErrNonFastForward is returned if the user is attempting to update a ref
	// with a commit that is not a direct descendant of the current tip.
	ErrNonFastForward = stderrors.New("non-fast-forward")

	// ErrStaleInfo is returned if the provided old oid does not match the current tip.
	ErrStaleInfo = stderrors.New("stale-info")

	// ErrInvalidOldOid is returned if the provided old oid is not a valid object id.
	ErrInvalidOldOid = stderrors.New("invalid-old-oid")

	// ErrInvalidNewOid is returned if the provided new oid is not a valid object id.
	ErrInvalidNewOid = stderrors.New("invalid-new-oid")
)

func (o GitOperation) String() string {
	switch o {
	case OperationPull:
		return "pull"
	case OperationPush:
		return "push"
	case OperationBrowse:
		return "browse"
	default:
		return ""
	}
}

// AuthorizationLevel describes the result of an authorization attempt.
type AuthorizationLevel int

const (
	//AuthorizationDenied denotes that the operation was not allowed.
	AuthorizationDenied AuthorizationLevel = iota

	// AuthorizationAllowed denotes that the operation was allowed.
	AuthorizationAllowed

	// AuthorizationAllowedRestricted denotes that the operation was allowed
	// (with restrictions).
	AuthorizationAllowedRestricted

	// AuthorizationAllowedReadOnly denotes that the operation was allowed in a
	// read-only fashion.
	AuthorizationAllowedReadOnly
)

// AuthorizationCallback is invoked by GitServer when a user requests to
// perform an action. It returns the authorization level and the username that
// is requesting the action.
type AuthorizationCallback func(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	repositoryName string,
	operation GitOperation,
) (AuthorizationLevel, string)

func noopAuthorizationCallback(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	repositoryName string,
	operation GitOperation,
) (AuthorizationLevel, string) {
	return AuthorizationDenied, ""
}

// ReferenceDiscoveryCallback is invoked by GitServer when performing reference
// discovery or prior to updating a reference. It returhn whether the provided
// reference should be visible to the user.
type ReferenceDiscoveryCallback func(
	ctx context.Context,
	repository *git.Repository,
	referenceName string,
) bool

func noopReferenceDiscoveryCallback(
	ctx context.Context,
	repository *git.Repository,
	referenceName string,
) bool {
	return true
}

// UpdateCallback is invoked by GitServer when a user attempts to update a
// repository. It returns an error if the update request is invalid.
type UpdateCallback func(
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	command *GitCommand,
	oldCommit, newCommit *git.Commit,
) error

func noopUpdateCallback(
	ctx context.Context,
	repository *git.Repository,
	level AuthorizationLevel,
	command *GitCommand,
	oldCommit, newCommit *git.Commit,
) error {
	return nil
}

// PreprocessCallback is invoked by GitServer when a user attempts to update a
// repository. It can perform an arbitrary transformation of the packfile and
// the update commands to be performed. A temporary directory is provided so
// that the new packfile can be stored there, if needed, and will be deleted
// afterwards. It returns the path of the new packfile, a new list of commands,
// and an error in case the operation failed.
type PreprocessCallback func(
	ctx context.Context,
	repository *git.Repository,
	tmpDir string,
	packPath string,
	commands []*GitCommand,
) (string, []*GitCommand, error)

func noopPreprocessCallback(
	ctx context.Context,
	repository *git.Repository,
	tmpDir string,
	packPath string,
	commands []*GitCommand,
) (string, []*GitCommand, error) {
	return packPath, commands, nil
}

// ContextCallback is invoked by GitServer at the beginning of each request. It
// allows for callers to create a context wrapper.
type ContextCallback func(ctx context.Context) context.Context

func noopContextCallback(ctx context.Context) context.Context {
	return ctx
}

// WriteHeader sets the HTTP status code and optionally clears any pending
// headers from the reply. It also returns the cause of the HTTP error.
func WriteHeader(w http.ResponseWriter, err error, clearHeaders bool) error {
	if clearHeaders {
		for k := range w.Header() {
			w.Header().Del(k)
		}
	}
	if base.HasErrorCategory(err, ErrBadRequest) {
		w.WriteHeader(http.StatusBadRequest)
		if cause := base.UnwrapCauseFromErrorCategory(err, ErrBadRequest); cause != nil {
			return cause
		}
		return err
	} else if base.HasErrorCategory(err, ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		if cause := base.UnwrapCauseFromErrorCategory(err, ErrNotFound); cause != nil {
			return cause
		}
		return err
	} else if base.HasErrorCategory(err, ErrForbidden) {
		w.WriteHeader(http.StatusForbidden)
		if cause := base.UnwrapCauseFromErrorCategory(err, ErrForbidden); cause != nil {
			return cause
		}
		return err
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
}

// A gitHTTPHandler implements git's smart protocol.
type gitHTTPHandler struct {
	rootPath         string
	repositorySuffix string
	enableBrowse     bool
	contextCallback  ContextCallback
	protocol         *GitProtocol
	log              log15.Logger
}

func (h *gitHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	splitPath := strings.SplitN(r.URL.Path[1:], "/", 2)
	if len(splitPath) < 2 {
		h.log.Error(
			"Request",
			"Method", r.Method,
			"path", r.URL.Path[1:],
			"error", "not found",
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	repositoryName := splitPath[0]
	if strings.HasPrefix(repositoryName, ".") {
		h.log.Error(
			"Request",
			"Method", r.Method,
			"path", r.URL.Path[1:],
			"error", "repository path starts with .",
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	relativeURL, err := url.Parse(
		fmt.Sprintf("git:///%s?%s", splitPath[1], r.URL.RawQuery),
	)
	if err != nil {
		panic(err)
	}
	ctx := h.contextCallback(r.Context())

	repositoryPath := path.Join(h.rootPath, fmt.Sprintf("%s%s", repositoryName, h.repositorySuffix))
	if _, err := os.Stat(repositoryPath); os.IsNotExist(err) {
		h.log.Error(
			"Request",
			"Method", r.Method,
			"URL", relativeURL,
			"path", repositoryPath,
			"error", err,
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	serviceName := relativeURL.Query().Get("service")
	if r.Method == "GET" && relativeURL.Path == "/info/refs" &&
		serviceName == "git-upload-pack" {
		level, _ := h.protocol.AuthCallback(ctx, w, r, repositoryName, OperationPull)
		if level == AuthorizationDenied {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "authorization denied",
			)
			return
		}

		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePrePull(ctx, repositoryPath, level, h.protocol, h.log, w); err != nil {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", err,
			)
			WriteHeader(w, err, true)
			return
		}
	} else if r.Method == "POST" && relativeURL.Path == "/git-upload-pack" {
		level, _ := h.protocol.AuthCallback(ctx, w, r, repositoryName, OperationPull)
		if level == AuthorizationDenied {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "authorization denied",
			)
			return
		}

		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePull(repositoryPath, level, h.log, r.Body, w); err != nil {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", err,
			)
			WriteHeader(w, err, true)
			return
		}
	} else if r.Method == "GET" && relativeURL.Path == "/info/refs" &&
		serviceName == "git-receive-pack" {
		level, _ := h.protocol.AuthCallback(ctx, w, r, repositoryName, OperationPush)
		if level == AuthorizationDenied {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "authorization denied",
			)
			return
		}
		if level == AuthorizationAllowedReadOnly {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "insufficient permissions to modify repository",
			)
			WriteHeader(w, ErrForbidden, true)
			return
		}

		w.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePrePush(ctx, repositoryPath, level, h.protocol, h.log, w); err != nil {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", err,
			)
			WriteHeader(w, err, true)
			return
		}
	} else if r.Method == "POST" && relativeURL.Path == "/git-receive-pack" {
		level, _ := h.protocol.AuthCallback(ctx, w, r, repositoryName, OperationPush)
		if level == AuthorizationDenied {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "authorization denied",
			)
			return
		}
		if level == AuthorizationAllowedReadOnly {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "insufficient permissions to modify repository",
			)
			WriteHeader(w, ErrForbidden, true)
			return
		}

		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePush(
			ctx,
			repositoryPath,
			level,
			h.protocol,
			h.log,
			r.Body,
			w,
		); err != nil {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", err,
			)
			WriteHeader(w, err, true)
			return
		}
	} else if (r.Method == "GET" || r.Method == "HEAD") && h.enableBrowse {
		level, _ := h.protocol.AuthCallback(ctx, w, r, repositoryName, OperationBrowse)
		if level == AuthorizationDenied {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "authorization denied",
			)
			return
		}
		trailingSlash := strings.HasSuffix(relativeURL.Path, "/")
		cleanedPath := path.Clean(relativeURL.Path)
		if strings.HasPrefix(cleanedPath, ".") {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", "path starts with .",
			)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if trailingSlash && !strings.HasSuffix(cleanedPath, "/") {
			cleanedPath += "/"
		}
		w.Header().Set("Content-Type", "application/json")
		if err := handleBrowse(
			ctx,
			repositoryPath,
			level,
			h.protocol,
			r.Method,
			r.Header.Get("Accept"),
			cleanedPath,
			w,
		); err != nil {
			h.log.Error(
				"Request",
				"Method", r.Method,
				"URL", relativeURL,
				"path", repositoryPath,
				"error", err,
			)
			WriteHeader(w, err, true)
			return
		}
	} else {
		h.log.Error(
			"Request",
			"Method", r.Method,
			"URL", relativeURL,
			"path", repositoryPath,
			"error", "not found",
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h.log.Info(
		"Request",
		"Method", r.Method,
		"URL", relativeURL,
		"path", repositoryPath,
	)
}

// GitServer returns an http.Handler that implements git's smart protocol,
// as documented on
// https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_the_smart_protocol .
// The callbacks will be invoked as a way to allow callers to perform
// additional authorization and pre-upload checks.
func GitServer(
	rootPath string,
	repositorySuffix string,
	enableBrowse bool,
	protocol *GitProtocol,
	contextCallback ContextCallback,
	log log15.Logger,
) http.Handler {
	if contextCallback == nil {
		contextCallback = noopContextCallback
	}

	return &gitHTTPHandler{
		rootPath:         rootPath,
		repositorySuffix: repositorySuffix,
		enableBrowse:     enableBrowse,
		contextCallback:  contextCallback,
		protocol:         protocol,
		log:              log,
	}
}
