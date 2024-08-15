package storage

import (
	"errors"
	"io"
	"strconv"
	"sync"

	"github.com/redis/rueidis"
	"github.com/zeebo/xxh3"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
)

type RedisBackendConfig struct {
	StorageBackendConfig
	wrappers.RedisConfig
}

type RedisStorage struct {
	Base
	config     RedisBackendConfig
	wrapper    *wrappers.RedisWrapper
	keyMutexes [65536]sync.RWMutex
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

	// lock key for reading
	keyMutex := redis.getMutex(key)
	keyMutex.RLock()
	defer func() {
		if backendObj == nil {
			keyMutex.RUnlock()
		} else {
			backendObj.mutex = keyMutex
		}
	}()

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
	var err error
	if storageObj.Metadata.BodySize > storageObj.Metadata.BodyChunkLen {
		nextWrite := make(chan error)
		done := make(chan error)
		go redis.writeBodyChunkToClient(storageObj, 0, nextWrite, done, w)
		nextWrite <- nil
		err = <-done
		close(nextWrite)
		close(done)
	} else {
		err = redis.writeBodyChunkToClient(storageObj, 0, nil, nil, w)
	}
	// detect if incomplete body is stored
	if err == rueidis.Nil {
		return ErrIncompleteBody
	}
	return err
}

func (redis *RedisStorage) writeBodyChunkToClient(storageObj *StorageObject, chunkIdx int, nextWrite chan error, done chan error, w io.Writer) error {
	lastChunkIdx := GetBodyChunkCnt(&storageObj.BackendObject) - 1
	query := redis.wrapper.GetConn().B().Hget().Key(storageObj.CacheKey).Field(GetBodyChunkName(chunkIdx)).Build()
	bodyBytes, err := redis.wrapper.GetConn().Do(redis.wrapper.Context, query).AsBytes()
	if err == nil && len(bodyBytes) < 1 {
		err = ErrNotFound
	}
	if nextWrite != nil {
		signalErr := <-nextWrite
		if signalErr != nil {
			done <- signalErr
			return signalErr
		}
	}
	if err != nil {
		if done != nil {
			done <- err
		}
		return err
	}
	if chunkIdx < lastChunkIdx {
		if nextWrite != nil && done != nil {
			go redis.writeBodyChunkToClient(storageObj, chunkIdx+1, nextWrite, done, w)
		} else {
			return ErrIncompleteBodyWrite
		}
	}
	_, err = w.Write(bodyBytes)
	if nextWrite != nil && done != nil {
		if chunkIdx < lastChunkIdx {
			nextWrite <- err
		} else {
			done <- err
		}
	}
	return nil
}

func (redis *RedisStorage) Set(key string, backendObj *BackendObject) error {

	// check that object is cacheable - otherwise there's nothing there to do
	if err := redis.IsCacheable(backendObj); err != nil {
		return err
	}

	// lock current key for writing
	keyMutex := redis.getMutex(key)
	keyMutex.Lock()
	defer keyMutex.Unlock()

	// set body first (if any) in order to have complete data on get
	if len(backendObj.Body) > 0 {
		// delete old object in order to simulate cache object write atomicity
		if delErr := redis.del(key); delErr != nil {
			return delErr
		}
		chunkCnt := backendObj.Metadata.BodySize/backendObj.Metadata.BodyChunkLen + 1
		for i := 0; i < chunkCnt; i++ {
			chunkName := GetBodyChunkName(i)
			chunkStart := i * backendObj.Metadata.BodyChunkLen
			chunkEnd := min(chunkStart+backendObj.Metadata.BodyChunkLen, len(backendObj.Body))
			query := redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue(chunkName, string(backendObj.Body[chunkStart:chunkEnd]))
			redisErr := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
			if redisErr != nil {
				redis.counters.cacheErr.Add(1)
				// if we can't write the entire body try to delete the entire key so that we don't have partial (incorrect) objects in cache
				redis.del(key)
				return redisErr
			}
		}
	}

	// serialize metadata
	metadataSer, serErr := Serialize(backendObj.Metadata)
	if serErr != nil {
		redis.counters.serErr.Add(1)
		redis.del(key)
		return serErr
	}

	// build redis query for metadata, headers and statusCode
	query := redis.wrapper.GetConn().B().Hset().Key(key).FieldValue().FieldValue("metadata", string(metadataSer))

	if backendObj.Headers != nil {
		headersSer, serErr := Serialize(backendObj.Headers)
		if serErr != nil {
			redis.counters.serErr.Add(1)
			redis.del(key)
			return serErr
		}
		query = query.FieldValue("statusCode", strconv.Itoa(backendObj.StatusCode)).FieldValue("headers", string(headersSer))
	}

	// execute redis query for metadata, headers and statusCode
	redisErr := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
	if redisErr != nil {
		redis.counters.cacheErr.Add(1)
		redis.del(key)
		return redisErr
	}

	// set expiration
	if redisErr := redis.expire(key, &backendObj.Metadata); redisErr != nil {
		redis.counters.cacheErr.Add(1)
		// if we can't set ttl try to delete the entire key so that we don't forever lingering incomplete objects in cache
		if delErr := redis.del(key); delErr != nil {
			return delErr
		}
		return redisErr
	}

	return nil
}

func (redis *RedisStorage) Update(key string, metadata *StorageMetadata) error {
	// try to lock key for writing but give up if another thread is already writing
	keyMutex := redis.getMutex(key)
	if !keyMutex.TryLock() {
		return nil
	}
	defer keyMutex.Unlock()
	// serialize metadata
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

// delete key without locking - for internal use only
func (redis *RedisStorage) del(key string) error {
	query := redis.wrapper.GetConn().B().Del().Key(key)
	err := redis.wrapper.GetConn().Do(redis.wrapper.Context, query.Build()).Error()
	if err != nil {
		redis.counters.cacheErr.Add(1)
	}
	return err
}

func (redis *RedisStorage) Del(key string) error {

	// lock current key for writing
	keyMutex := redis.getMutex(key)
	keyMutex.Lock()
	defer keyMutex.Unlock()

	// do the real delete
	return redis.del(key)

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

func (redis *RedisStorage) getMutex(key string) *sync.RWMutex {
	idx := uint16(xxh3.HashString(key))
	return &redis.keyMutexes[idx]
}
