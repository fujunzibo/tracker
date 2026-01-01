package mapx

import (
	"encoding/json"
	"sync"
)

type Map[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
}

func NewMapWithCapacity[K comparable, V any](capacity int) *Map[K, V] {
	m := &Map[K, V]{}
	m.data = make(map[K]V, capacity)
	return m
}

func NewMap[K comparable, V any]() *Map[K, V] {
	return NewMapWithCapacity[K, V](8)
}

func New[K comparable, V any]() *Map[K, V] {
	return NewMapWithCapacity[K, V](8)
}

func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func Values[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

func KeyValues[K comparable, V any](m map[K]V) ([]K, []V) {
	keys := make([]K, 0, len(m))
	values := make([]V, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}

func (m *Map[K, V]) Len() int {
	m.mu.RLock()
	size := len(m.data)
	m.mu.RUnlock()
	return size
}

func (m *Map[K, V]) Has(key K) bool {
	m.mu.RLock()
	_, ok := m.data[key]
	m.mu.RUnlock()
	return ok
}

func (m *Map[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	v, ok := m.data[key]
	m.mu.RUnlock()
	return v, ok
}

func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	m.data[key] = value
	m.mu.Unlock()
}

func (m *Map[K, V]) Delete(key K) {
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
}

func (m *Map[K, V]) Pop(key K) (V, bool) {
	m.mu.Lock()
	v, ok := m.data[key]
	if ok {
		delete(m.data, key)
	}
	m.mu.Unlock()
	return v, ok
}

func (m *Map[K, V]) CopyFrom(data map[K]V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range data {
		m.data[k] = v
	}
}

func (m *Map[K, V]) ToMap() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c := make(map[K]V, len(m.data))
	for k, v := range m.data {
		c[k] = v
	}
	return c
}

func (m *Map[K, V]) MarshalJSON() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m.data)
}

func (m *Map[K, V]) UnmarshalJSON(b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[K]V)
	return json.Unmarshal(b, &m.data)
}

func (m *Map[K, V]) Traversal(h func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		if !h(k, v) {
			break
		}
	}
}

func (m *Map[K, V]) Range(h func(K, V) bool) {
	c := m.ToMap()
	for k, v := range c {
		if !h(k, v) {
			break
		}
	}
}

//go:inline
func (m *Map[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Keys(m.data)
}

//go:inline
func (m *Map[K, V]) Values() []V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Values(m.data)
}

//go:inline
func (m *Map[K, V]) KeyValues() ([]K, []V) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return KeyValues(m.data)
}

func (m *Map[K, V]) Reset(data map[K]V) {
	if data == nil {
		data = make(map[K]V)
	}
	m.mu.Lock()
	m.data = data
	m.mu.Unlock()
}

func (m *Map[K, V]) GetOrCreate(key K, c func() V) V {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		v = c()
		m.data[key] = v
	}
	return v
}

func (m *Map[K, V]) Update(key K, c func(V) V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = c(m.data[key])
}
