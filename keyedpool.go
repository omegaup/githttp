package githttp

import (
	"container/list"
	"hash/maphash"
	"sync"
)

// KeyedPool is an implementation of a size-bounded set of of objects
// associated with a key. If the objects in the pool exceed the maximum size
// (with a default of 256), the least-recently-used item in the pool will be
// evicted.  Two callbacks can be provided and will be invoked when a new
// object should atomically be created when calling Get() and a suitable object
// is not available, and when an object is evicted due to lack of space.
type KeyedPool struct {
	seed   maphash.Seed
	shards []*poolShard
}

// KeyedPoolOptions are options that can be passed to NewKeyedPool to customize
// the pool limits and functionality.
type KeyedPoolOptions struct {
	// MaxEntries is the maximum number of items in the pool before an item is
	// evicted. The default is 256 if unset.
	MaxEntries int

	// Shards is the number of shards the pool will be split into to diminish
	// lock contention. The default is 16 if unset.
	Shards int

	// New is a callback that will be invoked if Get() does not find a
	// previously-created object in the pool.
	New func(key string) (interface{}, error)

	// OnEvicted is a callback that will be invoked when an object is evicted
	// from the pool.
	OnEvicted func(key string, value interface{})
}

// NewKeyedPool creates a new object pool with the provided options.
func NewKeyedPool(options KeyedPoolOptions) *KeyedPool {
	if options.Shards == 0 {
		options.Shards = 16
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = 256
	}
	pool := &KeyedPool{
		seed:   maphash.MakeSeed(),
		shards: make([]*poolShard, options.Shards),
	}
	for i := range pool.shards {
		pool.shards[i] = &poolShard{
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
// created.
func (c *KeyedPool) Get(key string) (interface{}, error) {
	return c.shards[c.hash(key)].get(key)
}

// Put inserts an element into the pool. This operation could cause the
// least-recently-used element to be evicted.
func (c *KeyedPool) Put(key string, value interface{}) {
	if value == nil {
		return
	}
	c.shards[c.hash(key)].put(key, value)
}

// Len returns the number of elements in the pool.
func (c *KeyedPool) Len() int {
	l := 0
	for _, shard := range c.shards {
		shard.RLock()
		l += shard.list.Len()
		shard.RUnlock()
	}
	return l
}

// Remove removes the objects associated with the provided key from the pool.
func (c *KeyedPool) Remove(key string) {
	c.shards[c.hash(key)].remove(key)
}

// Clear removes all stored items from the pool.
func (c *KeyedPool) Clear() {
	for _, shard := range c.shards {
		shard.clear()
	}
}

func (c *KeyedPool) hash(key string) uint64 {
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.WriteString(key)
	return h.Sum64() % uint64(len(c.shards))
}

// poolShard is a single shard of the KeyedPool. This maintains a pool of
// poolEntry objects, and each one of them will be present in exactly two
// lists:
//
// - list, the global list of poolEntry objects. This is used to know what
//   object is the least-recently used for eviction purposes.
// - entries, the per-key list of poolEntry objects. This is used to be able to
//   get all the per-key poolEntry objects in a round-robin fashion.
type poolShard struct {
	sync.RWMutex

	new       func(key string) (interface{}, error)
	onEvicted func(key string, value interface{})

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

type poolEntry struct {
	key   string
	value interface{}

	// shardElement is the node within the list of all of the elements in the
	// shard, in the order in which they were used.
	shardElement *list.Element

	// entriesElement is the node within the list of all of the elements that
	// have the same key, in the order in which they were used.
	entriesElement *list.Element
}

func (s *poolShard) get(key string) (interface{}, error) {
	s.Lock()
	entryList, ok := s.entries[key]
	if !ok {
		builder := s.new
		s.Unlock()
		if builder == nil {
			return nil, nil
		}
		return builder(key)
	}
	entry := entryList.Back().Value.(*poolEntry)
	entryList.Remove(entry.entriesElement)
	s.list.Remove(entry.shardElement)
	if entryList.Len() == 0 {
		delete(s.entries, key)
	}
	// clear all references for easier garbage collection.
	entry.entriesElement = nil
	entry.shardElement = nil
	result := entry.value
	s.Unlock()
	return result, nil
}

func (s *poolShard) put(key string, value interface{}) {
	s.Lock()

	var evictedEntry func()
	if s.list.Len() >= s.maxEntries {
		evictedEntry = s.evictOldestLocked()
	}
	entry := &poolEntry{
		key:   key,
		value: value,
	}
	_, ok := s.entries[key]
	if !ok {
		s.entries[key] = list.New()
	}
	entry.entriesElement = s.entries[key].PushFront(entry)
	entry.shardElement = s.list.PushFront(entry)
	s.Unlock()

	if evictedEntry != nil {
		evictedEntry()
	}
}

func (s *poolShard) remove(key string) {
	s.Lock()

	entryList, ok := s.entries[key]
	if !ok {
		s.Unlock()
		return
	}

	var evictedEntries []func()
	for e := entryList.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*poolEntry)
		s.list.Remove(entry.shardElement)
		if s.onEvicted != nil {
			cb := s.onEvicted
			k := entry.key
			v := entry.value
			evictedEntries = append(evictedEntries, func() { cb(k, v) })
		}
	}
	delete(s.entries, key)
	s.Unlock()

	for _, evictedEntry := range evictedEntries {
		evictedEntry()
	}
}

func (s *poolShard) clear() {
	s.Lock()
	var evictedEntries []func()
	if s.onEvicted != nil {
		for e := s.list.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*poolEntry)
			cb := s.onEvicted
			k := entry.key
			v := entry.value
			evictedEntries = append(evictedEntries, func() { cb(k, v) })
		}
	}
	s.list.Init()
	s.entries = make(map[string]*list.List)
	s.Unlock()

	for _, evictedEntry := range evictedEntries {
		evictedEntry()
	}
}

// evictOldestLocked evicts the oldest entry in the shard. If the eviction
// causes the per-entry list to be empty, it removes the per-entry list from
// the entry mapping. This returns a (possibly nil) func that invokes the
// eviction callback.
func (s *poolShard) evictOldestLocked() func() {
	shardElement := s.list.Back()
	if shardElement == nil {
		panic("list is empty")
	}
	entry := shardElement.Value.(*poolEntry)
	entryList := s.entries[entry.key]
	entryList.Remove(entry.entriesElement)
	s.list.Remove(entry.shardElement)
	if entryList.Len() == 0 {
		delete(s.entries, entry.key)
	}
	var evictedEntry func()
	if s.onEvicted != nil {
		cb := s.onEvicted
		k := entry.key
		v := entry.value
		evictedEntry = func() { cb(k, v) }
	}
	entry.value = nil
	entry.entriesElement = nil
	entry.shardElement = nil
	return evictedEntry
}
