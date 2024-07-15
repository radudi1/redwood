package storage

import (
	"errors"
	"io"
	"log"
	"strconv"

	"github.com/redis/rueidis"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
)

type RedisBackendConfig struct {
	StorageBackendConfig
	wrappers.RedisConfig
}

type RedisStorage struct {
	Base
	config  RedisBackendConfig
	wrapper *wrappers.RedisWrapper
}

var ErrChunkSizeMismatch = errors.New("chunk size does not match metadata chunk size")

func NewRedisStorage(config RedisBackendConfig) (*RedisStorage, error) {
	r := RedisStorage{
		config: config,
	}
	r.Base.config = r.config.StorageBackendConfig
	var err error
	r.wrapper, err = wrappers.NewRedisWrapper(config.RedisConfig)
	return &r, err
}

func (redis *RedisStorage) Get(key string, fields ...string) (backendObj *BackendObject, err error) {

	var serFields map[string][]byte
	var redisErr error

	serFields, redisErr = redis.wrapper.Hmget(key, fields...)
	if redisErr != nil {
		if redisErr == rueidis.Nil {
			err = ErrNotFound
			return
		}
		redis.counters.cacheErr.Add(1)
		err = redisErr
		return
	}
	if len(serFields["metadata"]) < 1 { // object was not found
		redis.counters.misses.Add(1)
		return nil, ErrNotFound
	}

	backendObj = &BackendObject{}

	if err = Unserialize(serFields["metadata"], &backendObj.Metadata); err != nil {
		redis.counters.serErr.Add(1)
		return
	}

	if len(serFields["statusCode"]) > 0 {
		var statusCode int64
		statusCode, err = strconv.ParseInt(string(serFields["statusCode"]), 10, 64)
		if err != nil {
			return
		}
		backendObj.StatusCode = int(statusCode)
	}

	if len(serFields["headers"]) > 0 {
		if err = Unserialize(serFields["headers"], &backendObj.Headers); err != nil {
			redis.counters.serErr.Add(1)
			return
		}
	}

	if len(serFields["body"]) > 0 {
		if backendObj.Metadata.BodyChunkLen > 0 {
			chunkCnt := GetBodyChunkCnt(backendObj)
			backendObj.Body = make([]byte, chunkCnt*backendObj.Metadata.BodyChunkLen)
			writtenBytes := copy(backendObj.Body, serFields["body"])
			for i := 1; i < chunkCnt; i++ {
				chunkName := GetBodyChunkName(i)
				serFields, err = redis.wrapper.Hmget(key, chunkName)
				if err != nil {
					redis.counters.cacheErr.Add(1)
					return
				}
				bodyStart := i * backendObj.Metadata.BodyChunkLen
				writtenBytes += copy(backendObj.Body[bodyStart:], serFields[chunkName])
			}
			backendObj.Body = backendObj.Body[:writtenBytes]
		} else { // old behaviour
			backendObj.Body = serFields["body"]
		}
	}

	redis.counters.hits.Add(1)
	return
}

func (redis *RedisStorage) WriteBodyToClient(storageObj *StorageObject, w io.Writer) error {
	if storageObj.Metadata.BodySize == 0 {
		return nil
	}
	nextWrite := make(chan error)
	done := make(chan error)
	go redis.writeBodyChunkToClient(storageObj, 0, nextWrite, done, w)
	nextWrite <- nil
	err := <-done
	close(nextWrite)
	close(done) // detect if incomplete body is stored
	if err == rueidis.Nil {
		return ErrIncompleteBody
	}
	return err
}

func (redis *RedisStorage) writeBodyChunkToClient(storageObj *StorageObject, chunkIdx int, nextWrite chan error, done chan error, w io.Writer) {
	lastChunkIdx := GetBodyChunkCnt(&storageObj.BackendObject) - 1
	query := redis.wrapper.GetConn().B().Hget().Key(storageObj.CacheKey).Field(GetBodyChunkName(chunkIdx)).Build()
	bodyBytes, err := redis.wrapper.GetConn().Do(redis.wrapper.Context, query).AsBytes()
	if err == nil && len(bodyBytes) < 1 {
		err = ErrNotFound
	}
	signalErr := <-nextWrite
	if signalErr != nil {
		done <- signalErr
		return
	}
	if err != nil {
		done <- err
		return
	}
	if chunkIdx < lastChunkIdx {
		go redis.writeBodyChunkToClient(storageObj, chunkIdx+1, nextWrite, done, w)
	}
	_, err = w.Write(bodyBytes)
	if chunkIdx < lastChunkIdx {
		nextWrite <- err
	} else {
		done <- err
	}
}

func (redis *RedisStorage) Set(key string, backendObj *BackendObject) error {

	if err := redis.IsCacheable(backendObj); err != nil {
		return err
	}

	metadataSer, serErr := Serialize(backendObj.Metadata)
	if serErr != nil {
		redis.counters.serErr.Add(1)
		return serErr
	}

	query := redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue("metadata", string(metadataSer))

	if backendObj.Headers != nil {
		headersSer, serErr := Serialize(backendObj.Headers)
		if serErr != nil {
			redis.counters.serErr.Add(1)
			return serErr
		}
		query = query.FieldValue("statusCode", strconv.Itoa(backendObj.StatusCode)).FieldValue("headers", string(headersSer))
	}

	redisErr := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
	if redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}
	if redisErr = redis.expire(key, &backendObj.Metadata); redisErr != nil {
		redis.counters.cacheErr.Add(1)
		return redisErr
	}

	if len(backendObj.Body) > 0 {
		chunkCnt := backendObj.Metadata.BodySize/backendObj.Metadata.BodyChunkLen + 1
		for i := 0; i < chunkCnt; i++ {
			chunkName := GetBodyChunkName(i)
			chunkStart := i * backendObj.Metadata.BodyChunkLen
			chunkEnd := min(chunkStart+backendObj.Metadata.BodyChunkLen, len(backendObj.Body))
			query = redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue(chunkName, string(backendObj.Body[chunkStart:chunkEnd]))
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
