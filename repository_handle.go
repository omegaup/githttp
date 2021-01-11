package githttp

import (
	"context"
	"sync"

	"github.com/omegaup/go-base/v3"
	"github.com/omegaup/go-base/v3/logging"
	"github.com/omegaup/go-base/v3/tracing"

	git "github.com/libgit2/git2go/v33"
	"github.com/pkg/errors"
)

var (
	repositoryHandlePool     *base.KeyedPool[*RepositoryHandle]
	repositoryHandlePoolOnce sync.Once
)

// RepositoryHandle contains a reference to an open git repository and its
// lockfile. It also contains lazily-obtained references.
type RepositoryHandle struct {
	Repository        *git.Repository
	Lockfile          *Lockfile
	DoNotReturnToPool bool

	path       string
	references []*RepositoryReference
	log        logging.Logger
}

func newRepositoryHandle(m *LockfileManager, path string, log logging.Logger) (*RepositoryHandle, error) {
	log.Info(
		"Acquiring a repository handle",
		map[string]any{
			"path": path,
		},
	)
	repository, err := git.OpenRepository(path)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to open git repository at %q",
			path,
		)
	}

	return &RepositoryHandle{
		Repository: repository,
		Lockfile:   m.NewLockfile(repository.Path()),

		path: path,
		log:  log,
	}, nil
}

// OpenRepositoryHandle opens a git repository and returns a handle to it.
// Opening git repositories from scratch is moderately expensive, so once the
// caller is done with the handle, they can Release() it into an object pool so
// that it can be reused again in the future (unless the repository has
// changed).
func OpenRepositoryHandle(ctx context.Context, m *LockfileManager, path string, log logging.Logger) (*RepositoryHandle, error) {
	txn := tracing.FromContext(ctx)
	defer txn.StartSegment("OpenRepositoryHandle").End()
	repositoryHandlePoolOnce.Do(func() {
		repositoryHandlePool = base.NewKeyedPool(base.KeyedPoolOptions[*RepositoryHandle]{
			New: func(path string) (*RepositoryHandle, error) {
				return newRepositoryHandle(m, path, log)
			},
			OnEvicted: func(key string, value *RepositoryHandle) {
				value.free()
			},
		})
	})

	handle, err := repositoryHandlePool.Get(path)
	if err != nil {
		return nil, err
	}

	defer txn.StartSegment("acquire lock").End()
	if ok, err := handle.Lockfile.TryRLock(); !ok {
		log.Info(
			"Waiting for the lockfile",
			map[string]any{
				"err": err,
			},
		)
		// If we failed to acquire the read lock immediately, it means that there's
		// another handle that's currently writing to the repository. When that
		// happens, we cannot rely on the cached information from the previously
		// opened handle, so we need to create a brand new one.
		handle.DoNotReturnToPool = true
		handle.Release()

		handle, err = newRepositoryHandle(m, path, log)
		if err != nil {
			return nil, err
		}
		if err := handle.Lockfile.RLock(); err != nil {
			handle.Release()
			return nil, errors.Wrapf(
				err,
				"failed to acquire the lockfile at %q",
				path,
			)
		}
	}

	return handle, nil
}

// EvictRepositoryHandles removes any repository handles that exist in the
// handle pool. This should be called if a particular repository is modified to
// invalidate all cached objects.
func EvictRepositoryHandles(path string) {
	repositoryHandlePool.Remove(path)
}

// Release releases the repositoy and puts it back into the pool unless
// DoNotReturnToPool was set, in which case the resources are immediately freed
// and forgotten.
//
// Once in the pool's ownership, the underlying repository can be freed at some
// point in the future.
func (h *RepositoryHandle) Release() {
	h.Lockfile.Unlock()
	if h.DoNotReturnToPool {
		h.free()
		return
	}
	repositoryHandlePool.Put(h.path, h)
}

func (h *RepositoryHandle) free() {
	h.log.Info(
		"Releasing a repository handle",
		map[string]any{
			"path": h.path,
		},
	)
	h.Repository.Free()
}

// RepositoryReference contains the fully resolved information of a
// git.Reference.
type RepositoryReference struct {
	Name           string
	Target         *git.Oid
	SymbolicTarget string
}

// References returns the list of all references (after fully resolving them)
// in the repository. This information is lazily computed the first time it's
// used and then cached for future usages.
func (h *RepositoryHandle) References() ([]*RepositoryReference, error) {
	if h.references == nil {
		it, err := h.Repository.NewReferenceIterator()
		if err != nil {
			return nil, errors.Wrap(
				err,
				"failed to create a reference iterator",
			)
		}
		defer it.Free()

		// Make sure this is non-nil even if it's empty.
		references := []*RepositoryReference{}
		for {
			ref, err := it.Next()
			if err != nil {
				if git.IsErrorCode(err, git.ErrorCodeIterOver) {
					break
				}

				return nil, errors.Wrap(
					err,
					"failed to read next reference",
				)
			}

			if ref.Type() == git.ReferenceSymbolic {
				target, err := ref.Resolve()
				if err != nil {
					ref.Free()
					return nil, errors.Wrapf(
						err,
						"failed to resolve the symbolic target for %s(%s)",
						ref.Name(),
						ref.Target(),
					)
				}
				references = append(references, &RepositoryReference{
					Name:           ref.Name(),
					Target:         target.Target(),
					SymbolicTarget: ref.SymbolicTarget(),
				})
				target.Free()
			} else if ref.Type() == git.ReferenceOid {
				references = append(references, &RepositoryReference{
					Name:   ref.Name(),
					Target: ref.Target(),
				})
			}
			ref.Free()
		}

		h.references = references
	}

	return h.references, nil
}
