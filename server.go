package githttp

import (
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
)

// AuthorizationCallback is invoked by GitServer when a user requests to
// perform an action.
type AuthorizationCallback func(
	w http.ResponseWriter,
	r *http.Request,
	repositoryName string,
	operation GitOperation,
) AuthorizationLevel

func noopAuthorizationCallback(
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
	repository *git.Repository,
	command *GitCommand,
	oldCommit, newCommit *git.Commit,
) error

func noopUpdateCallback(
	repository *git.Repository,
	command *GitCommand,
	oldCommit, newCommit *git.Commit,
) error {
	return nil
}

// A StaticServeCallback is invoked by GitServer when a GET operation is
// issued, and can be used to serve static content that does not depend on the
// requester. It returns true if it handled the request.
type StaticServeCallback func(
	requestPath string,
	w http.ResponseWriter,
	r *http.Request,
) bool

func noopStaticServeCallback(
	requestPath string,
	w http.ResponseWriter,
	r *http.Request,
) bool {
	return false
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

// A gitHttpHandler implements git's smart protocol.
type gitHttpHandler struct {
	rootPath            string
	enableBrowse        bool
	log                 log15.Logger
	authCallback        AuthorizationCallback
	updateCallback      UpdateCallback
	staticServeCallback StaticServeCallback
}

func (h *gitHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	splitPath := strings.SplitN(r.URL.Path[1:], "/", 2)
	if len(splitPath) < 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	repositoryName := splitPath[0]
	relativeURL, err := url.Parse(
		fmt.Sprintf("git:///%s?%s", splitPath[1], r.URL.RawQuery),
	)
	if err != nil {
		panic(err)
	}

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
		level := h.authCallback(w, r, repositoryName, OperationPull)
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
		level := h.authCallback(w, r, repositoryName, OperationPull)
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
		level := h.authCallback(w, r, repositoryName, OperationPush)
		if level == AuthorizationDenied {
			return
		}

		w.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePrePush(repositoryPath, level, h.log, w); err != nil {
			writeHeader(w, err)
			return
		}
	} else if r.Method == "POST" && relativeURL.Path == "/git-receive-pack" {
		level := h.authCallback(w, r, repositoryName, OperationPush)
		if level == AuthorizationDenied {
			return
		}

		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		w.Header().Set("Cache-Control", "no-cache")
		if err := handlePush(repositoryPath, level, h.updateCallback, h.log, r.Body, w); err != nil {
			writeHeader(w, err)
			return
		}
	} else if r.Method == "GET" && h.enableBrowse {
		level := h.authCallback(w, r, repositoryName, OperationBrowse)
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
		if h.staticServeCallback(cleanedPath, w, r) {
			return
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
	staticServeCallback StaticServeCallback,
	log log15.Logger,
) http.Handler {
	handler := &gitHttpHandler{
		rootPath:            rootPath,
		enableBrowse:        enableBrowse,
		log:                 log,
		authCallback:        authCallback,
		updateCallback:      updateCallback,
		staticServeCallback: staticServeCallback,
	}

	if handler.authCallback == nil {
		handler.authCallback = noopAuthorizationCallback
	}

	if handler.updateCallback == nil {
		handler.updateCallback = noopUpdateCallback
	}

	if handler.staticServeCallback == nil {
		handler.staticServeCallback = noopStaticServeCallback
	}

	return handler
}
