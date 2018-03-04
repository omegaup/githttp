package githttp

import (
	"context"
	"errors"
	"fmt"
	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
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
	// ErrNotFound is returned if a reference is not found.
	ErrNotFound = errors.New("not found")

	// ErrDeleteDisallowed is returned when a delete operation is attempted.
	ErrDeleteDisallowed = errors.New("delete-disallowed")

	// ErrForbidden is returned if an operation is not allowed.
	ErrForbidden = errors.New("forbidden")

	// ErrInvalidRef is returned if a reference that the system does not support
	// is attempted to be modified.
	ErrInvalidRef = errors.New("invalid-ref")

	// ErrReadOnlyRef is returned if a read-only reference is attempted to be
	// modified.
	ErrReadOnlyRef = errors.New("read-only")

	// ErrRestrictedRef is returned if a restricted reference is attempted to be
	// modified.
	ErrRestrictedRef = errors.New("restricted-ref")
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
// perform an action.
type AuthorizationCallback func(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	repositoryName string,
	operation GitOperation,
) AuthorizationLevel

func noopAuthorizationCallback(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	repositoryName string,
	operation GitOperation,
) AuthorizationLevel {
	return AuthorizationAllowed
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

// writeHeader clears any pending headers from the reply and sets the HTTP
// status code.
func writeHeader(w http.ResponseWriter, err error) {
	for k := range w.Header() {
		w.Header().Del(k)
	}
	if err == ErrBadRequest {
		w.WriteHeader(http.StatusBadRequest)
	} else if err == ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// A gitHTTPHandler implements git's smart protocol.
type gitHTTPHandler struct {
	rootPath           string
	enableBrowse       bool
	log                log15.Logger
	authCallback       AuthorizationCallback
	updateCallback     UpdateCallback
	preprocessCallback PreprocessCallback
}

func (h *gitHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	splitPath := strings.SplitN(r.URL.Path[1:], "/", 2)
	if len(splitPath) < 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	repositoryName := splitPath[0]
	if strings.HasPrefix(repositoryName, ".") {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	relativeURL, err := url.Parse(
		fmt.Sprintf("git:///%s?%s", splitPath[1], r.URL.RawQuery),
	)
	if err != nil {
		panic(err)
	}
	ctx := r.Context()

	repositoryPath := path.Join(h.rootPath, fmt.Sprintf("%s.git", repositoryName))
	h.log.Info(
		"Request",
		"Method", r.Method,
		"URL", relativeURL,
		"path", repositoryPath,
	)
	if _, err := os.Stat(repositoryPath); os.IsNotExist(err) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	serviceName := relativeURL.Query().Get("service")
	if r.Method == "GET" && relativeURL.Path == "/info/refs" &&
		serviceName == "git-upload-pack" {
		level := h.authCallback(ctx, w, r, repositoryName, OperationPull)
		if level == AuthorizationDenied {
			return
		}

		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePrePull(repositoryPath, level, h.log, w); err != nil {
			writeHeader(w, err)
			return
		}
	} else if r.Method == "POST" && relativeURL.Path == "/git-upload-pack" {
		level := h.authCallback(ctx, w, r, repositoryName, OperationPull)
		if level == AuthorizationDenied {
			return
		}

		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePull(repositoryPath, level, h.log, r.Body, w); err != nil {
			writeHeader(w, err)
			return
		}
	} else if r.Method == "GET" && relativeURL.Path == "/info/refs" &&
		serviceName == "git-receive-pack" {
		level := h.authCallback(ctx, w, r, repositoryName, OperationPush)
		if level == AuthorizationDenied {
			return
		}
		if level == AuthorizationAllowedReadOnly {
			writeHeader(w, ErrForbidden)
		}

		w.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePrePush(repositoryPath, level, h.log, w); err != nil {
			writeHeader(w, err)
			return
		}
	} else if r.Method == "POST" && relativeURL.Path == "/git-receive-pack" {
		level := h.authCallback(ctx, w, r, repositoryName, OperationPush)
		if level == AuthorizationDenied {
			return
		}
		if level == AuthorizationAllowedReadOnly {
			writeHeader(w, ErrForbidden)
		}

		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePush(
			ctx,
			repositoryPath,
			level,
			h.updateCallback,
			h.preprocessCallback,
			h.log,
			r.Body,
			w,
		); err != nil {
			writeHeader(w, err)
			return
		}
	} else if r.Method == "GET" && h.enableBrowse {
		level := h.authCallback(ctx, w, r, repositoryName, OperationBrowse)
		if level == AuthorizationDenied {
			return
		}
		trailingSlash := strings.HasSuffix(relativeURL.Path, "/")
		cleanedPath := path.Clean(relativeURL.Path)
		if strings.HasPrefix(cleanedPath, ".") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if trailingSlash && !strings.HasSuffix(cleanedPath, "/") {
			cleanedPath += "/"
		}
		w.Header().Set("Content-Type", "text/json")
		if err := handleBrowse(repositoryPath, level, cleanedPath, h.log, w); err != nil {
			writeHeader(w, err)
			return
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// GitServer returns an http.Handler that implements git's smart protocol,
// as documented on
// https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_the_smart_protocol .
// The callbacks will be invoked as a way to allow callers to perform
// additional authorization and pre-upload checks.
func GitServer(
	rootPath string,
	enableBrowse bool,
	authCallback AuthorizationCallback,
	updateCallback UpdateCallback,
	preprocessCallback PreprocessCallback,
	log log15.Logger,
) http.Handler {
	handler := &gitHTTPHandler{
		rootPath:           rootPath,
		enableBrowse:       enableBrowse,
		log:                log,
		authCallback:       authCallback,
		updateCallback:     updateCallback,
		preprocessCallback: preprocessCallback,
	}

	if handler.authCallback == nil {
		handler.authCallback = noopAuthorizationCallback
	}

	if handler.updateCallback == nil {
		handler.updateCallback = noopUpdateCallback
	}

	if handler.preprocessCallback == nil {
		handler.preprocessCallback = noopPreprocessCallback
	}

	return handler
}
