package cache

import (
	"sync"
	"time"
)

type item[V any] struct {
	v        V
	activeAt time.Time
}

type Cache[K comparable, V any] struct {
	data map[K]item[V]
	mu   sync.Mutex
	ttl  time.Duration
}

func NewCache[K comparable, V any](ttl time.Duration) *Cache[K, V] {
	c := new(Cache[K, V])
	c.data = map[K]item[V]{}
	c.ttl = ttl

	return c
}

func (c *Cache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = map[K]item[V]{}
}

func (c *Cache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.data[key]
	if !ok {
		return false
	}
	delete(c.data, key)
	return true
}

func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = item[V]{
		v:        value,
		activeAt: time.Now().Add(c.ttl),
	}
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.data[key]
	if !ok {
		return item.v, false
	}
	if item.activeAt.Before(time.Now()) {
		return item.v, false
	}
	return item.v, true
}

func (c *Cache[K, V]) GetOrSet(key K, generator func() (V, error)) (V, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.data[key]
	if ok && item.activeAt.After(time.Now()) {
		return item.v, nil
	}
	var err error
	item.v, err = generator()
	if err != nil {
		return item.v, err
	}
	item.activeAt = time.Now().Add(c.ttl)
	c.data[key] = item
	return item.v, nil
}

type Entry[V any] struct {
	ttl       time.Duration
	generator func() (*V, error)
	mu        sync.RWMutex
	expiredAt time.Time
	value     *V
}

func NewEntry[V any](ttl time.Duration, generator func() (*V, error)) *Entry[V] {
	return &Entry[V]{
		ttl:       ttl,
		generator: generator,
	}
}

func (e *Entry[V]) SetTTL(ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ttl = ttl
}

func (e *Entry[V]) Expired() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.expiredAt.Before(time.Now())
}

func (e *Entry[V]) Get() (*V, error) {
	e.mu.RLock()
	// 检查是否过期
	if e.expiredAt.After(time.Now()) {
		defer e.mu.RUnlock()
		return e.value, nil
	}
	e.mu.RUnlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	// 重新生成数据
	var err error
	e.value, err = e.generator()
	if err != nil {
		// 处理错误
		return nil, err
	}
	e.expiredAt = time.Now().Add(e.ttl)
	return e.value, nil
}

func (e *Entry[V]) Refresh() (*V, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// 重新生成数据
	var err error
	e.value, err = e.generator()
	if err != nil {
		// 处理错误
		return nil, err
	}
	e.expiredAt = time.Now().Add(e.ttl)
	return e.value, nil
}

func (e *Entry[V]) Set(value *V) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value = value
	e.expiredAt = time.Now().Add(e.ttl)
}

func (e *Entry[V]) ExpiredAt() time.Time {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.expiredAt
}

func (e *Entry[V]) SetExpiredAt(expiredAt time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expiredAt = expiredAt
}

func (e *Entry[V]) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expiredAt = time.Time{}
	e.value = nil
}
