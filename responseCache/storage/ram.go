package storage

import (
	"io"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type RamBackendConfig struct {
	StorageBackendConfig
	NumItems int
}

type RamStorage struct {
	Base
	config         RamBackendConfig
	cache          *lru.TwoQueueCache[string, *BackendObject]
	numSetsSinceGC atomic.Int64
}

func NewRamStorage(config RamBackendConfig) (*RamStorage, error) {
	cache, err := lru.New2Q[string, *BackendObject](config.NumItems)
	if err != nil {
		return nil, err
	}
	ram := &RamStorage{
		config: config,
		cache:  cache,
	}
	ram.Base.config = ram.config.StorageBackendConfig
	return ram, nil
}

func (ram *RamStorage) Get(key string, fields ...string) (backendObj *BackendObject, err error) {
	obj, ok := ram.cache.Get(key)
	if !ok {
		ram.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	if obj.Metadata.Expires.Before(time.Now()) {
		ram.cache.Remove(key)
		ram.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	ram.counters.hits.Add(1)
	return obj, nil
}

func (ram *RamStorage) WriteBodyToClient(storageObj *StorageObject, w io.Writer) error {
	if storageObj == nil {
		return ErrNotFound
	}
	if len(storageObj.Body) == 0 {
		return nil
	}
	_, err := w.Write(storageObj.Body)
	return err
}

func (ram *RamStorage) Set(key string, backendObj *BackendObject) error {
	if err := ram.IsCacheable(backendObj); err != nil {
		return err
	}
	ram.cache.Add(key, backendObj)
	ram.numSetsSinceGC.Add(1)
	if ram.numSetsSinceGC.Load() > int64(ram.config.NumItems)*2 {
		go ram.GCWorker()
	}
	return nil
}

func (ram *RamStorage) Update(key string, metadata *StorageMetadata) error {
	obj, ok := ram.cache.Peek(key)
	if !ok {
		return ErrNotFound
	}
	obj.Metadata = *metadata
	return nil
}

func (ram *RamStorage) Has(key string) bool {
	return ram.cache.Contains(key)
}

func (ram *RamStorage) GCWorker() {
	for _, key := range ram.cache.Keys() {
		obj, ok := ram.cache.Peek(key)
		if !ok {
			continue
		}
		if obj.Metadata.Expires.Before(time.Now()) {
			ram.cache.Remove(key)
		}
	}
	ram.numSetsSinceGC.Store(0)
}
