package githttp

import (
	"container/list"
	"errors"
	"hash/maphash"
	"sync"
)

var (
	// ErrKeyNotFound will be returned from Get to indicate that the key was not
	// found to prevent returning a zero value.
	ErrKeyNotFound = errors.New("key not found")
)

// KeyedPool is an implementation of a size-bounded set of of objects
// associated with a key. If the objects in the pool exceed the maximum size
// (with a default of 256), the least-recently-used item in the pool will be
// evicted.  Two callbacks can be provided and will be invoked when a new
// object should atomically be created when calling Get() and a suitable object
// is not available, and when an object is evicted due to lack of space.
type KeyedPool[T any] struct {
	seed   maphash.Seed
	shards []*poolShard[T]
}

// KeyedPoolOptions are options that can be passed to NewKeyedPool to customize
// the pool limits and functionality.
type KeyedPoolOptions[T any] struct {
	// MaxEntries is the maximum number of items in the pool before an item is
	// evicted. The default is 256 if unset.
	MaxEntries int

	// Shards is the number of shards the pool will be split into to diminish
	// lock contention. The default is 16 if unset.
	Shards int

	// New is a callback that will be invoked if Get() does not find a
	// previously-created object in the pool.
	New func(key string) (T, error)

	// OnEvicted is a callback that will be invoked when an object is evicted
	// from the pool.
	OnEvicted func(key string, value T)
}

// NewKeyedPool creates a new object pool with the provided options.
func NewKeyedPool[T any](options KeyedPoolOptions[T]) *KeyedPool[T] {
	if options.Shards == 0 {
		options.Shards = 16
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = 256
	}
	pool := &KeyedPool[T]{
		seed:   maphash.MakeSeed(),
		shards: make([]*poolShard[T], options.Shards),
	}
	for i := range pool.shards {
		pool.shards[i] = &poolShard[T]{
			new:        options.New,
			onEvicted:  options.OnEvicted,
			maxEntries: (options.MaxEntries + (options.Shards - 1)) / options.Shards,
			list:       list.New(),
			entries:    make(map[string]*list.List),
		}
	}
	return pool
}

// Get obtains one element from the pool. If it was already present, the
// element is removed from the pool and returned. Otherwise, a new one will be
// created. If the New callback function is missing, it will return
// ErrKeyNotFound.
func (p *KeyedPool[T]) Get(key string) (T, error) {
	return p.shards[p.hash(key)].get(key)
}

// Put inserts an element into the pool. This operation could cause the
// least-recently-used element to be evicted.
func (p *KeyedPool[T]) Put(key string, value T) {
	p.shards[p.hash(key)].put(key, value)
}

// Len returns the number of elements in the pool.
func (p *KeyedPool[T]) Len() int {
	l := 0
	for _, shard := range p.shards {
		shard.RLock()
		l += shard.list.Len()
		shard.RUnlock()
	}
	return l
}

// Remove removes the objects associated with the provided key from the pool.
func (p *KeyedPool[T]) Remove(key string) {
	p.shards[p.hash(key)].remove(key)
}

// Clear removes all stored items from the pool.
func (p *KeyedPool[T]) Clear() {
	for _, shard := range p.shards {
		shard.clear()
	}
}

func (p *KeyedPool[T]) hash(key string) uint64 {
	var h maphash.Hash
	h.SetSeed(p.seed)
	h.WriteString(key)
	return h.Sum64() % uint64(len(p.shards))
}

// poolShard is a single shard of the KeyedPool. This maintains a pool of
// poolEntry objects, and each one of them will be present in exactly two
// lists:
//
//   - list, the global list of poolEntry objects. This is used to know what
//     object is the least-recently used for eviction purposes.
//   - entries, the per-key list of poolEntry objects. This is used to be able to
//     get all the per-key poolEntry objects in a round-robin fashion.
type poolShard[T any] struct {
	sync.RWMutex

	new       func(key string) (T, error)
	onEvicted func(key string, value T)

	// maxEntries is the maximum number of entries that should be in the list of
	// poolEntry objects.
	maxEntries int

	// list holds all the poolEntry objects for this shard, in the order in which
	// they were used (most recent first).
	list *list.List

	// entries is a mapping from keys to a list of poolEntry objects that are
	// associated with that key.
	entries map[string]*list.List
}

type poolEntry[T any] struct {
	key   string
	value T

	// shardElement is the node within the list of all of the elements in the
	// shard, in the order in which they were used.
	shardElement *list.Element

	// entriesElement is the node within the list of all of the elements that
	// have the same key, in the order in which they were used.
	entriesElement *list.Element
}

func (p *poolShard[T]) get(key string) (T, error) {
	p.Lock()
	entryList, ok := p.entries[key]
	if !ok {
		builder := p.new
		p.Unlock()
		if builder == nil {
			var zero T
			return zero, ErrKeyNotFound
		}
		return builder(key)
	}
	entry := entryList.Back().Value.(*poolEntry[T])
	entryList.Remove(entry.entriesElement)
	p.list.Remove(entry.shardElement)
	if entryList.Len() == 0 {
		delete(p.entries, key)
	}
	// clear all references for easier garbage collection.
	entry.entriesElement = nil
	entry.shardElement = nil
	result := entry.value
	p.Unlock()
	return result, nil
}

func (p *poolShard[T]) put(key string, value T) {
	p.Lock()

	var evictedEntry func()
	if p.list.Len() >= p.maxEntries {
		evictedEntry = p.evictOldestLocked()
	}
	entry := &poolEntry[T]{
		key:   key,
		value: value,
	}
	_, ok := p.entries[key]
	if !ok {
		p.entries[key] = list.New()
	}
	entry.entriesElement = p.entries[key].PushFront(entry)
	entry.shardElement = p.list.PushFront(entry)
	p.Unlock()

	if evictedEntry != nil {
		evictedEntry()
	}
}

func (p *poolShard[T]) remove(key string) {
	p.Lock()

	entryList, ok := p.entries[key]
	if !ok {
		p.Unlock()
		return
	}

	var evictedEntries []func()
	for e := entryList.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*poolEntry[T])
		p.list.Remove(entry.shardElement)
		if p.onEvicted != nil {
			cb := p.onEvicted
			k := entry.key
			v := entry.value
			evictedEntries = append(evictedEntries, func() { cb(k, v) })
		}
	}
	delete(p.entries, key)
	p.Unlock()

	for _, evictedEntry := range evictedEntries {
		evictedEntry()
	}
}

func (p *poolShard[T]) clear() {
	p.Lock()
	var evictedEntries []func()
	if p.onEvicted != nil {
		for e := p.list.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*poolEntry[T])
			cb := p.onEvicted
			k := entry.key
			v := entry.value
			evictedEntries = append(evictedEntries, func() { cb(k, v) })
		}
	}
	p.list.Init()
	p.entries = make(map[string]*list.List)
	p.Unlock()

	for _, evictedEntry := range evictedEntries {
		evictedEntry()
	}
}

// evictOldestLocked evicts the oldest entry in the shard. If the eviction
// causes the per-entry list to be empty, it removes the per-entry list from
// the entry mapping. This returns a (possibly nil) func that invokes the
// eviction callback.
func (p *poolShard[T]) evictOldestLocked() func() {
	shardElement := p.list.Back()
	if shardElement == nil {
		panic("list is empty")
	}
	entry := shardElement.Value.(*poolEntry[T])
	entryList := p.entries[entry.key]
	entryList.Remove(entry.entriesElement)
	p.list.Remove(entry.shardElement)
	if entryList.Len() == 0 {
		delete(p.entries, entry.key)
	}
	var evictedEntry func()
	if p.onEvicted != nil {
		cb := p.onEvicted
		k := entry.key
		v := entry.value
		evictedEntry = func() { cb(k, v) }
	}
	var zero T
	entry.value = zero
	entry.entriesElement = nil
	entry.shardElement = nil
	return evictedEntry
}
