package storage

import (
	"log"
	"slices"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

const (
	RedisBackend = 1
	RamBackend   = 2
)

type StorageConfig struct {
	Redis RedisStorageConfig
	Ram   RamStorageConfig
}

type Storage struct {
	redis *RedisStorage
	ram   *RamStorage
}

func NewStorage(config StorageConfig) (s *Storage, err error) {
	s = &Storage{}
	// redis
	s.redis, err = NewRedisStorage(config.Redis)
	if err != nil {
		return
	}
	// ram
	if config.Ram.NumItems > 0 {
		s.ram, err = NewRamStorage(config.Ram)
		if err != nil {
			return
		}
	}
	return
}

func (storage *Storage) Get(key string, fields ...string) (storageObj *StorageObject, fromBackend int, err error) {
	// ram
	if storage.ram != nil {
		storageObj, _ = storage.ram.Get(key, fields...)
		if storageObj != nil {
			return storageObj, RamBackend, nil
		}
	}
	// redis
	storageObj, err = storage.redis.Get(key, fields...)
	if err != nil {
		if err != ErrNotFound {
			log.Println(err)
		}
	} else if storage.ram != nil && slices.Contains(fields, "body") { // cache redis hit to ram only if it's a full object
		storage.ram.Set(key, storageObj)
	}
	fromBackend = RedisBackend
	return
}

func (storage *Storage) Set(key string, storageObj *StorageObject) error {
	// ram
	if storage.ram != nil {
		storage.ram.Set(key, storageObj)
	}
	// redis
	err := storage.redis.Set(key, storageObj)
	return err
}

func (storage *Storage) Update(key string, storageObj *StorageObject) error {
	// ram
	if storage.ram != nil {
		storage.ram.Update(key, storageObj)
	}
	// redis
	err := storage.redis.Update(key, storageObj)
	return err
}

func (storage *Storage) Has(key string) bool {
	//ram
	if storage.ram != nil {
		if storage.ram.Has(key) {
			return true
		}
	}
	//redis
	return storage.redis.Has(key)
}

func (storage *Storage) GetRedisConn() rueidis.Client {
	return storage.redis.wrapper.GetConn()
}

func (storage *Storage) GetRedisCompatConn() rueidiscompat.Cmdable {
	return storage.redis.wrapper.GetCompatConn()
}

func (storage *Storage) GetCounters() Counters {
	counters := storage.redis.GetCounters()
	if storage.ram != nil {
		counters = CounterSum(counters, storage.ram.GetCounters())
	}
	return counters
}

func (storage *Storage) GetBackendCounters(backend string) Counters {
	switch backend {
	case "redis":
		return storage.redis.GetCounters()
	case "ram":
		if storage.ram != nil {
			return storage.ram.GetCounters()
		}
	}
	return Counters{}
}
