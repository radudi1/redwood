package storage

import (
	"strconv"

	"github.com/redis/rueidis"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
)

type RedisStorage struct {
	Base
	wrapper *wrappers.RedisWrapper
}

func NewRedisStorage(config wrappers.RedisConfig) (*RedisStorage, error) {
	r := RedisStorage{}
	var err error
	r.wrapper, err = wrappers.NewRedisWrapper(config)
	return &r, err
}

func (redis *RedisStorage) Get(key string, fields ...string) (storageObj *StorageObject, err error) {

	var serFields map[string]string
	var redisErr error

	serFields, redisErr = redis.wrapper.Hmget(key, fields...)
	if redisErr != nil {
		if redisErr != rueidis.Nil {
			redis.counters.cacheErr.Add(1)
		}
		err = redisErr
		return
	}
	if serFields["metadata"] == "" { // object was not found
		return nil, rueidis.Nil
	}

	storageObj = &StorageObject{}

	if err = Unserialize(serFields["metadata"], &storageObj.Metadata); err != nil {
		redis.counters.serErr.Add(1)
		return
	}

	if len(serFields["statusCode"]) > 0 {
		var statusCode int64
		statusCode, err = strconv.ParseInt(serFields["statusCode"], 10, 64)
		if err != nil {
			return
		}
		storageObj.StatusCode = int(statusCode)
	}

	if serFields["headers"] != "" {
		if err = Unserialize(serFields["headers"], &storageObj.Headers); err != nil {
			redis.counters.serErr.Add(1)
			return
		}
	}

	storageObj.Body = serFields["body"]

	return
}

func (redis *RedisStorage) Set(key string, storageObj *StorageObject) error {

	metadataSer, serErr := Serialize(storageObj.Metadata)
	if serErr != nil {
		redis.counters.serErr.Add(1)
		return serErr
	}

	query := redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue("metadata", metadataSer)

	if storageObj.Headers != nil {
		headersSer, serErr := Serialize(storageObj.Headers)
		if serErr != nil {
			redis.counters.serErr.Add(1)
			return serErr
		}
		query = query.FieldValue("statusCode", strconv.Itoa(storageObj.StatusCode)).FieldValue("headers", headersSer)
		if len(storageObj.Body) > 0 {
			query = query.FieldValue("body", storageObj.Body)
		}
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
	return nil
}

func (redis *RedisStorage) Update(key string, storageObj *StorageObject) error {
	metadataSer, serErr := Serialize(storageObj.Metadata)
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
			FieldValue("metadata", metadataSer).
			Build()).
		Error()
	if redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}
	if redisErr = redis.expire(key, &storageObj.Metadata); redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}
	return nil
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
