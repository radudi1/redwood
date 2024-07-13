package storage

import (
	"log"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

const (
	RedisBackend = 1
	RamBackend   = 2
)

// cached bodies will be split in chunks of this size
// the value was chosen empirically appearing to be one of the most space efficient
// different values can grow redis object memory by tens of kB
const BodyChunkLen = 110 * 1024

type StorageMetadata struct {
	// when it was last Updated
	Updated time.Time
	// when it becomes Stale
	Stale time.Time
	// until when we can serve stale while revalidating in background according to standards
	RevalidateDeadline time.Time
	// when it Expires and it will be automatically removed from cache
	Expires time.Time
	// all response Vary headers combined into one string
	Vary string
	// number of body chunks
	BodyChunkCnt int
	// the size of each body chunk
	BodyChunkLen int
}

type StorageObject struct {
	StatusCode int
	Metadata   StorageMetadata
	Headers    http.Header
	Body       []byte
}

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
	// set chunk info
	if len(storageObj.Body) > 0 {
		storageObj.Metadata.BodyChunkLen = BodyChunkLen
		storageObj.Metadata.BodyChunkCnt = len(storageObj.Body)/BodyChunkLen + 1
	}
	// ram
	if storage.ram != nil {
		storage.ram.Set(key, storageObj)
	}
	// redis
	err := storage.redis.Set(key, storageObj)
	return err
}

func (storage *Storage) Update(key string, metadata *StorageMetadata) error {
	// ram
	if storage.ram != nil {
		storage.ram.Update(key, metadata)
	}
	// redis
	err := storage.redis.Update(key, metadata)
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

func GetBodyChunkName(chunkNo int) string {
	if chunkNo == 0 {
		return "body"
	}
	return "body" + strconv.Itoa(chunkNo)
}
