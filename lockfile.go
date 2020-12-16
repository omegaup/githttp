package githttp

import (
	"path/filepath"
	"syscall"

	"github.com/omegaup/go-base/v3"
)

// LockfileState represents the stat of the lockfile.
type LockfileState int

const (
	invalidFD = -1

	// LockfileStateUnlocked represents that a lockfile is not locked.
	LockfileStateUnlocked LockfileState = iota
	// LockfileStateReadLocked represents that a lockfile has acquired a read lock.
	LockfileStateReadLocked
	// LockfileStateLocked represents that a lockfile has acquired a read/write lock.
	LockfileStateLocked
)

// LockfileManager is a container for Lockfiles, which allows them to be reused
// between calls safely.
type LockfileManager struct {
	fdCache *base.KeyedPool[int]
}

// NewLockfileManager returns a new LockfileManager.
func NewLockfileManager() *LockfileManager {
	return &LockfileManager{
		fdCache: base.NewKeyedPool[int](base.KeyedPoolOptions[int]{
			New: func(path string) (int, error) {
				return syscall.Creat(path, 0600)
			},
			OnEvicted: func(path string, value int) {
				syscall.Close(value)
			},
		}),
	}
}

// Clear releases all the lockfiles in the pool.
func (m *LockfileManager) Clear() {
	m.fdCache.Clear()
}

// Lockfile represents a file-based lock that can be up/downgraded.  Since this
// is using the flock(2) system call and the promotion/demotion is non-atomic,
// any attempt to change the lock type must verify any preconditions after
// calling Lock()/RLock().
type Lockfile struct {
	path    string
	fd      int
	state   LockfileState
	fdCache *base.KeyedPool[int]
}

// NewLockfile creates a new Lockfile that is initially unlocked.
func (m *LockfileManager) NewLockfile(repositoryPath string) *Lockfile {
	return &Lockfile{
		path:    filepath.Join(repositoryPath, "githttp.lock"),
		fd:      invalidFD,
		fdCache: m.fdCache,
	}
}

func (l *Lockfile) open() error {
	if l.fd != invalidFD {
		return nil
	}

	// This will reuse a previous (unlocked) lockfile if possible. Otherwise, it
	// will open a new one.
	fd, err := l.fdCache.Get(l.path)
	if err != nil {
		return err
	}
	l.fd = fd

	return nil
}

// TryRLock attempts to acquires a shared lock for the Lockfile's path. More
// than one process / goroutine may hold a shared lock for this Lockfile's path
// at any given time.
func (l *Lockfile) TryRLock() (bool, error) {
	if err := l.open(); err != nil {
		return false, err
	}
	if err := syscall.Flock(l.fd, syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			return false, nil
		}
		return false, err
	}
	l.state = LockfileStateReadLocked
	return true, nil
}

// RLock acquires a shared lock for the Lockfile's path. More than one process
// / goroutine may hold a shared lock for this Lockfile's path at any given
// time.
func (l *Lockfile) RLock() error {
	if err := l.open(); err != nil {
		return err
	}
	if err := syscall.Flock(l.fd, syscall.LOCK_SH); err != nil {
		return err
	}
	l.state = LockfileStateReadLocked
	return nil
}

// TryLock attempts to acquire an exclusive lock for the Lockfile's path and
// returns whether it was able to do so. Only one process / goroutine may hold
// an exclusive lock for this Lockfile's path at any given time.
func (l *Lockfile) TryLock() (bool, error) {
	if err := l.open(); err != nil {
		return false, err
	}
	if err := syscall.Flock(l.fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			return false, nil
		}
		return false, err
	}
	l.state = LockfileStateLocked
	return true, nil
}

// Lock acquires an exclusive lock for the Lockfile's path. Only one process /
// goroutine may hold an exclusive lock for this Lockfile's path at any given
// time.
func (l *Lockfile) Lock() error {
	if err := l.open(); err != nil {
		return err
	}
	if err := syscall.Flock(l.fd, syscall.LOCK_EX); err != nil {
		return err
	}
	l.state = LockfileStateLocked
	return nil
}

// Unlock releases a lock for the Lockfile's path.
func (l *Lockfile) Unlock() error {
	if l.fd == invalidFD {
		return nil
	}
	err := syscall.Flock(l.fd, syscall.LOCK_UN)
	if err != nil {
		// We could not remove the lock, so let's just close the fd.
		syscall.Close(l.fd)
	} else {
		// The file is now unlocked. We can reuse it later.
		l.fdCache.Put(l.path, l.fd)
	}
	l.fd = invalidFD
	l.state = LockfileStateUnlocked
	return err
}

// State returns the Lockfile's current state.
func (l *Lockfile) State() LockfileState {
	return l.state
}
