package storage

import (
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/andybalholm/redwood/responseCache/storage/wrappers"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
	"github.com/vmihailenco/msgpack"
)

type Counters struct {
	SerErr   uint64
	CacheErr uint64
}

type atomicCounters struct {
	serErr   atomic.Uint64
	cacheErr atomic.Uint64
}

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
}

type StorageObject struct {
	StatusCode int
	Metadata   StorageMetadata
	Headers    http.Header
	Body       string
}

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

func (metadata *StorageMetadata) IsStale() bool {
	return metadata.Stale.Before(time.Now())
}

func (counters *atomicCounters) Get() Counters {
	return Counters{
		SerErr:   counters.serErr.Load(),
		CacheErr: counters.cacheErr.Load(),
	}
}

func CounterSum(counters ...Counters) Counters {
	sum := Counters{}
	for _, c := range counters {
		sum.SerErr += c.SerErr
		sum.CacheErr += c.CacheErr
	}
	return sum
}

func Serialize(data interface{}) (string, error) {
	serData, serErr := msgpack.Marshal(data)
	if serErr != nil {

		log.Println(serErr)
		return "", serErr
	}
	return string(serData), nil
}

func Unserialize(serializedData string, dstData interface{}) error {
	serErr := msgpack.Unmarshal([]byte(serializedData), dstData)
	if serErr != nil {
		log.Println(serErr)
		return serErr
	}
	return nil
}

// Returns Vary response header(s) as string
func varyHeadersAsStr(respHeaders http.Header) string {
	return strings.Join(respHeaders.Values("Vary"), ", ")
}

// Returns a string with all request headers mentioned in the vary string
// These headers are concatenated and returned as a single string
func varyVals(vary string, reqHeaders http.Header) string {
	varyFields := strings.Split(vary, ",")
	var ret []string
	for _, varyField := range varyFields {
		varyField = strings.Trim(varyField, " ")
		ret = append(ret, strings.Join(reqHeaders.Values(varyField), " "))
	}
	return strings.Join(ret, " ")
}
