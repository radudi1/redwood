package storage

import (
	"log"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

type StorageConfig struct {
	Redis wrappers.RedisConfig
}

type Storage struct {
	redis *RedisStorage
}

func NewStorage(config StorageConfig) (s *Storage, err error) {
	s = &Storage{}
	s.redis, err = NewRedisStorage(config.Redis)
	if err != nil {
		return
	}
	return
}

func (storage *Storage) Get(key string, fields ...string) (storageObj *StorageObject, err error) {
	storageObj, err = storage.redis.Get(key, fields...)
	if err != nil && err != rueidis.Nil {
		log.Println(err)
		return
	}
	return
}

func (storage *Storage) Set(key string, storageObj *StorageObject) error {
	err := storage.redis.Set(key, storageObj)
	return err
}

func (storage *Storage) Update(key string, storageObj *StorageObject) error {
	err := storage.redis.Update(key, storageObj)
	return err
}

func (storage *Storage) GetRedisConn() rueidis.Client {
	return storage.redis.wrapper.GetConn()
}

func (storage *Storage) GetRedisCompatConn() rueidiscompat.Cmdable {
	return storage.redis.wrapper.GetCompatConn()
}

func (storage *Storage) GetCounters() Counters {
	return CounterSum(storage.redis.GetCounters())
}
