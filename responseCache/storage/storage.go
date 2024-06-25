package storage

import (
	"log"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

type StorageConfig struct {
	Redis wrappers.RedisConfig
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

func (storage *Storage) Get(key string, fields ...string) (storageObj *StorageObject, err error) {
	// ram
	if storage.ram != nil {
		storageObj, _ = storage.ram.Get(key, fields...)
		if storageObj != nil {
			return storageObj, nil
		}
	}
	// redis
	storageObj, err = storage.redis.Get(key, fields...)
	if err != nil && err != ErrNotFound {
		log.Println(err)
		return
	}
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
