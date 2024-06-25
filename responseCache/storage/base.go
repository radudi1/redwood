package storage

import (
	"errors"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack"
)

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

type Counters struct {
	SerErr   uint64
	CacheErr uint64
	Hits     uint64
	Misses   uint64
	HitRatio float64
}

type atomicCounters struct {
	serErr   atomic.Uint64
	cacheErr atomic.Uint64
	hits     atomic.Uint64
	misses   atomic.Uint64
}

type Base struct {
	counters atomicCounters
}

var ErrNotFound = errors.New("storage object not found")
var ErrTooBig = errors.New("storage object too big")

func (base *Base) GetCounters() Counters {
	return base.counters.Get()
}

func (counters *atomicCounters) Get() Counters {
	c := Counters{
		SerErr:   counters.serErr.Load(),
		CacheErr: counters.cacheErr.Load(),
		Hits:     counters.hits.Load(),
		Misses:   counters.misses.Load(),
	}
	if numReqs := c.Hits + c.Misses; numReqs != 0 {
		c.HitRatio = float64(c.Hits) / float64(numReqs)
	}
	return c
}

func CounterSum(counters ...Counters) Counters {
	sum := Counters{}
	for _, c := range counters {
		sum.SerErr += c.SerErr
		sum.CacheErr += c.CacheErr
	}
	return sum
}

func (metadata *StorageMetadata) IsStale() bool {
	return metadata.Stale.Before(time.Now())
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
