package githttp

import (
	"math/rand"
	"strconv"
	"testing"
)

func BenchmarkKeyedPool_Random(b *testing.B) {
	values := make([]string, b.N*2)
	for i := range values {
		values[i] = strconv.FormatInt(rand.Int63()&0x7FFF, 10)
	}
	p := NewKeyedPool[string](KeyedPoolOptions[string]{
		MaxEntries: 8196,
		New: func(key string) (string, error) {
			return "", ErrKeyNotFound
		},
	})

	b.ResetTimer()

	var hit, miss int
	for i, value := range values {
		if i&1 == 0 {
			p.Put(value, value)
		} else {
			v, err := p.Get(value)
			if err == nil {
				hit++
				p.Put(value, v)
			} else {
				miss++
			}
		}
	}
	b.Logf("n: %d hit: %d miss: %d ratio: %f", b.N, hit, miss, float64(hit)/float64(hit+miss))
}

func BenchmarkKeyedPool_Frequent(b *testing.B) {
	values := make([]string, b.N*2)
	for i := range values {
		if i&1 == 0 {
			values[i] = strconv.FormatInt(rand.Int63()&0x3FFF, 10)
		} else {
			values[i] = strconv.FormatInt(rand.Int63()&0x7FFF, 10)
		}
	}
	p := NewKeyedPool[string](KeyedPoolOptions[string]{
		MaxEntries: 8196,
		New: func(key string) (string, error) {
			return "", ErrKeyNotFound
		},
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.Put(values[i], values[i])
	}
	var hit, miss int
	for i := 0; i < b.N; i++ {
		value := values[i]
		v, err := p.Get(value)
		if err == nil {
			hit++
			p.Put(value, v)
		} else {
			miss++
		}
	}
	b.Logf("n: %d hit: %d miss: %d ratio: %f", b.N, hit, miss, float64(hit)/float64(hit+miss))
}

func TestKeyedPoolEviction(t *testing.T) {
	evictCounter := 0
	p := NewKeyedPool[int](KeyedPoolOptions[int]{
		MaxEntries: 128,
		Shards:     1,
		OnEvicted: func(key string, value int) {
			if value > 128 {
				t.Fatalf("key %s should have not been evicted", key)
			}
			evictCounter++
		},
		New: func(key string) (int, error) {
			return 0, ErrKeyNotFound
		},
	})
	for i := 0; i < 256; i++ {
		value := strconv.Itoa(i)
		p.Put(value, i)
	}
	if p.Len() != 128 {
		t.Fatalf("bad len: %v, want 128", p.Len())
	}
	if evictCounter != 128 {
		t.Fatalf("bad evict count: %v, want 128", evictCounter)
	}
	for i := 0; i < 128; i++ {
		value := strconv.Itoa(i)
		_, err := p.Get(value)
		if err == nil {
			t.Fatalf("key %d should have been evicted", i)
		}
	}
	for i := 128; i < 256; i++ {
		value := strconv.Itoa(i)
		_, err := p.Get(value)
		if err != nil {
			t.Fatalf("key %d should have not be evicted", i)
		}
	}
}

func TestKeyedPoolRemove(t *testing.T) {
	p := NewKeyedPool[string](KeyedPoolOptions[string]{
		MaxEntries: 128,
		Shards:     1,
		New: func(key string) (string, error) {
			return "", ErrKeyNotFound
		},
	})
	for i := 0; i < 128; i++ {
		value := strconv.Itoa(i)
		p.Put(value, value)
	}
	if p.Len() != 128 {
		t.Fatalf("bad len: %v, want 128", p.Len())
	}
	for i := 0; i < 64; i++ {
		value := strconv.Itoa(i)
		p.Remove(value)
		_, err := p.Get(value)
		if err == nil {
			t.Fatalf("key %d should have been deleted", i)
		}
	}
	if p.Len() != 64 {
		t.Fatalf("bad len: %v, want 64", p.Len())
	}
}

func TestKeyedPoolClear(t *testing.T) {
	p := NewKeyedPool[string](KeyedPoolOptions[string]{
		MaxEntries: 128,
		Shards:     1,
	})
	for i := 0; i < 128; i++ {
		value := strconv.Itoa(i)
		p.Put(value, value)
	}
	if p.Len() != 128 {
		t.Fatalf("bad len: %v, want 128", p.Len())
	}
	p.Clear()
	if p.Len() != 0 {
		t.Fatalf("bad len: %v, want 0", p.Len())
	}
}
