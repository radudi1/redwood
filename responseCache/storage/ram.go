package storage

import (
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type RamStorageConfig struct {
	NumItems    int
	MaxItemSize int
}

type RamStorage struct {
	Base
	config         RamStorageConfig
	cache          *lru.TwoQueueCache[string, *StorageObject]
	numSetsSinceGC atomic.Int64
}

func NewRamStorage(config RamStorageConfig) (*RamStorage, error) {
	cache, err := lru.New2Q[string, *StorageObject](config.NumItems)
	if err != nil {
		return nil, err
	}
	return &RamStorage{
		config: config,
		cache:  cache,
	}, nil
}

func (ram *RamStorage) Get(key string, fields ...string) (storageObj *StorageObject, err error) {
	obj, ok := ram.cache.Get(key)
	if !ok {
		ram.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	ram.counters.hits.Add(1)
	return obj, nil
}

func (ram *RamStorage) Set(key string, storageObj *StorageObject) error {
	if len(storageObj.Body) > ram.config.MaxItemSize {
		return ErrTooBig
	}
	ram.cache.Add(key, storageObj)
	ram.numSetsSinceGC.Add(1)
	if ram.numSetsSinceGC.Load() > int64(ram.config.NumItems)*2 {
		go ram.GCWorker()
	}
	return nil
}

func (ram *RamStorage) Update(key string, storageObj *StorageObject) error {
	obj, ok := ram.cache.Peek(key)
	if !ok {
		return ErrNotFound
	}
	obj.Metadata = storageObj.Metadata
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
