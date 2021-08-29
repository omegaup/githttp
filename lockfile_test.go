package githttp

import (
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpgradeLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "commits_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	l := NewLockfile(dir)
	if err := l.RLock(); err != nil {
		t.Fatalf("Failed to lock git repository for reading: %v", err)
	}
	if err := l.RLock(); err != nil {
		t.Fatalf("Failed to re-lock git repository for reading: %v", err)
	}
	if err := l.Lock(); err != nil {
		t.Fatalf("Failed to lock git repository for writing: %v", err)
	}
	if err := l.Lock(); err != nil {
		t.Fatalf("Failed to re-lock git repository for writing: %v", err)
	}
	if err := l.Unlock(); err != nil {
		t.Fatalf("Failed to unlock git repository: %v", err)
	}

	if ok, err := l.TryRLock(); !ok || err != nil {
		t.Fatalf("Failed to trylock git repository for reading: %v", err)
	}
	if ok, err := l.TryRLock(); !ok || err != nil {
		t.Fatalf("Failed to re-tryock git repository for reading: %v", err)
	}
	if err := l.RLock(); err != nil {
		t.Fatalf("Failed to re-lock git repository for reading: %v", err)
	}
	if ok, err := l.TryLock(); !ok || err != nil {
		t.Fatalf("Failed to trylock git repository for writing: %v", err)
	}
	if ok, err := l.TryLock(); !ok || err != nil {
		t.Fatalf("Failed to re-trylock git repository for writing: %v", err)
	}
	if err := l.Lock(); err != nil {
		t.Fatalf("Failed to re-lock git repository for writing: %v", err)
	}
	if err := l.Unlock(); err != nil {
		t.Fatalf("Failed to unlock git repository: %v", err)
	}
}

func TestMultipleReadersLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "commits_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	l := NewLockfile(dir)
	if err = l.RLock(); err != nil {
		t.Fatalf("Failed to lock git repository for reading: %v", err)
	}
	defer l.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := NewLockfile(dir)
			if err := l.RLock(); err != nil {
				t.Errorf("Failed to lock git repository for reading: %v", err)
				panic(err)
			}
			defer l.Unlock()
		}()
	}

	wg.Wait()
}

func TestSingleWriterLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "commits_test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	var writerCount int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := NewLockfile(dir)
			if err := l.Lock(); err != nil {
				t.Errorf("Failed to lock git repository for reading: %v", err)
				panic(err)
			}
			// Try to make the other goroutines execute.
			time.Sleep(time.Millisecond)
			if new := atomic.AddInt32(&writerCount, 1); new != 1 {
				t.Errorf("More than one concurrent writer!")
				panic("More than one concurrent writer!")
			}
			defer atomic.AddInt32(&writerCount, -1)
			defer l.Unlock()
		}()
	}

	wg.Wait()
}
