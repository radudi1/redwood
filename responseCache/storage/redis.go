package storage

import (
	"errors"
	"log"
	"strconv"

	"github.com/redis/rueidis"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
)

type RedisStorageConfig struct {
	StorageBackendConfig
	wrappers.RedisConfig
}

type RedisStorage struct {
	Base
	config  RedisStorageConfig
	wrapper *wrappers.RedisWrapper
}

var ErrChunkSizeMismatch = errors.New("chunk size does not match metadata chunk size")

func NewRedisStorage(config RedisStorageConfig) (*RedisStorage, error) {
	r := RedisStorage{
		config: config,
	}
	r.Base.config = r.config.StorageBackendConfig
	var err error
	r.wrapper, err = wrappers.NewRedisWrapper(config.RedisConfig)
	return &r, err
}

func (redis *RedisStorage) Get(key string, fields ...string) (storageObj *StorageObject, err error) {

	var serFields map[string][]byte
	var redisErr error

	serFields, redisErr = redis.wrapper.Hmget(key, fields...)
	if redisErr != nil {
		if redisErr != rueidis.Nil {
			redis.counters.cacheErr.Add(1)
		}
		err = redisErr
		return
	}
	if len(serFields["metadata"]) < 1 { // object was not found
		redis.counters.misses.Add(1)
		return nil, ErrNotFound
	}

	storageObj = &StorageObject{}

	if err = Unserialize(serFields["metadata"], &storageObj.Metadata); err != nil {
		redis.counters.serErr.Add(1)
		return
	}

	if len(serFields["statusCode"]) > 0 {
		var statusCode int64
		statusCode, err = strconv.ParseInt(string(serFields["statusCode"]), 10, 64)
		if err != nil {
			return
		}
		storageObj.StatusCode = int(statusCode)
	}

	if len(serFields["headers"]) > 0 {
		if err = Unserialize(serFields["headers"], &storageObj.Headers); err != nil {
			redis.counters.serErr.Add(1)
			return
		}
	}

	if len(serFields["body"]) > 0 {
		if storageObj.Metadata.BodyChunkCnt > 0 {
			storageObj.Body = make([]byte, storageObj.Metadata.BodyChunkCnt*storageObj.Metadata.BodyChunkLen)
			if copy(storageObj.Body, []byte(serFields["body"])) != storageObj.Metadata.BodyChunkCnt {
				err = ErrChunkSizeMismatch
				redis.counters.serErr.Add(1)
				return
			}
			for i := 1; i < storageObj.Metadata.BodyChunkCnt; i++ {
				chunkName := GetBodyChunkName(i)
				serFields, err = redis.wrapper.Hmget(key, chunkName)
				if err != nil {
					redis.counters.cacheErr.Add(1)
					return
				}
				bodyStart := i * storageObj.Metadata.BodyChunkLen
				if copy(storageObj.Body[bodyStart:], []byte(serFields[chunkName])) != storageObj.Metadata.BodyChunkLen {
					err = ErrChunkSizeMismatch
					redis.counters.serErr.Add(1)
					return
				}
			}
		} else { // old behaviour
			storageObj.Body = []byte(serFields["body"])
		}
	}

	redis.counters.hits.Add(1)
	return
}

func (redis *RedisStorage) Set(key string, storageObj *StorageObject) error {

	if err := redis.IsCacheable(storageObj); err != nil {
		return err
	}

	metadataSer, serErr := Serialize(storageObj.Metadata)
	if serErr != nil {
		redis.counters.serErr.Add(1)
		return serErr
	}

	query := redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue("metadata", string(metadataSer))

	if storageObj.Headers != nil {
		headersSer, serErr := Serialize(storageObj.Headers)
		if serErr != nil {
			redis.counters.serErr.Add(1)
			return serErr
		}
		query = query.FieldValue("statusCode", strconv.Itoa(storageObj.StatusCode)).FieldValue("headers", string(headersSer))
	}

	redisErr := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
	if redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}
	if redisErr = redis.expire(key, &storageObj.Metadata); redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}

	if len(storageObj.Body) > 0 {
		numChunks := len(storageObj.Body)/BodyChunkLen + 1
		for i := 0; i < numChunks; i++ {
			chunkName := GetBodyChunkName(i)
			chunkStart := i * BodyChunkLen
			chunkEnd := min(chunkStart+BodyChunkLen-1, len(storageObj.Body))
			query = redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue(chunkName, string(storageObj.Body[chunkStart:chunkEnd]))
			redisErr := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
			if redisErr != nil {
				redis.counters.cacheErr.Add(1)
				// if we can't write the entire body try to delete the entire key so that we don't have partial (incorrect) objects in cache
				if delErr := redis.Del(key); delErr != nil {
					log.Println("WARNING! Could not delete incomplete redis cache key. You will have INCORRECT responses from redis cache")
				}
				return redisErr
			}
		}
	}

	return nil
}

func (redis *RedisStorage) Update(key string, metadata *StorageMetadata) error {
	metadataSer, serErr := Serialize(metadata)
	if serErr != nil {
		redis.counters.serErr.Add(1)
		return serErr
	}
	// update metadata for the main cache entry
	redisErr := redis.wrapper.GetConn().Do(
		redis.wrapper.Context,
		redis.wrapper.GetConn().B().Hset().
			Key(key).
			FieldValue().
			FieldValue("metadata", string(metadataSer)).
			Build()).
		Error()
	if redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}
	if redisErr = redis.expire(key, metadata); redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}
	return nil
}

func (redis *RedisStorage) Has(key string) bool {
	exists, redisErr := redis.wrapper.GetConn().Do(
		redis.wrapper.Context,
		redis.wrapper.GetConn().B().Exists().
			Key(key).
			Build()).
		AsBool()
	if redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return false
	}
	return exists
}

func (redis *RedisStorage) Del(key string) error {
	query := redis.wrapper.GetConn().B().Del().Key(key)
	err := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
	if err != nil {
		redis.counters.cacheErr.Add(1)
	}
	return err
}

// Sets redis ttl for key according to metadata
func (redis *RedisStorage) expire(key string, metadata *StorageMetadata) error {
	return redis.wrapper.GetConn().Do(
		redis.wrapper.Context,
		redis.wrapper.GetConn().B().Expireat().
			Key(key).
			Timestamp(metadata.Expires.Unix()).
			Build()).
		Error()
}
