package githttp

import (
	"path/filepath"
	"syscall"
)

const (
	invalidFd = -1
)

// Lockfile represents a file-based lock that can be up/downgraded.  Since this
// is using the flock(2) system call and the promotion/demotion is non-atomic,
// any attempt to change the lock type must verify any preconditions after
// calling Lock()/RLock().
type Lockfile struct {
	path string
	fd   int
}

// NewLockfile creates a new Lockfile that is initially unlocked.
func NewLockfile(repositoryPath string) *Lockfile {
	return &Lockfile{
		path: filepath.Join(repositoryPath, "githttp.lock"),
		fd:   invalidFd,
	}
}

func (l *Lockfile) open() error {
	if l.fd != invalidFd {
		return nil
	}
	f, err := syscall.Creat(l.path, 0600)
	if err != nil {
		return err
	}
	l.fd = f
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
	return true, nil
}

// RLock acquires a shared lock for the Lockfile's path. More than one process
// / goroutine may hold a shared lock for this Lockfile's path at any given
// time.
func (l *Lockfile) RLock() error {
	if err := l.open(); err != nil {
		return err
	}
	return syscall.Flock(l.fd, syscall.LOCK_SH)
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
	return true, nil
}

// Lock acquires an exclusive lock for the Lockfile's path. Only one process /
// goroutine may hold an exclusive lock for this Lockfile's path at any given
// time.
func (l *Lockfile) Lock() error {
	if err := l.open(); err != nil {
		return err
	}
	return syscall.Flock(l.fd, syscall.LOCK_EX)
}

// Unlock releases a lock for the Lockfile's path.
func (l *Lockfile) Unlock() error {
	if l.fd == invalidFd {
		return nil
	}
	if err := syscall.Close(l.fd); err != nil {
		return err
	}
	l.fd = invalidFd
	return nil
}
