package storage

import (
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type StorageBackendConfig struct {
	MaxBodySize int
	MinTtl      int
}

// backendObjects must always be released by calling the Close method for every Get operation AFTER the object is not needed anymore
type BackendObject struct {
	io.Writer
	StatusCode int
	Metadata   StorageMetadata
	Headers    http.Header
	Body       []byte
	writerPos  int
	mutex      *sync.RWMutex
}

type BackendCheckResult struct {
	BackendTypeId uint8
	TotalCnt      int
	InvalidCnt    int
	DeletedCnt    int
	CheckErrCnt   int
	DeleteErrCnt  int
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
	StorageBackend
	config   StorageBackendConfig
	counters atomicCounters
}

var (
	ErrNotFound              = errors.New("storage object not found")
	ErrTooBig                = errors.New("storage object too big")
	ErrTtlTooSmall           = errors.New("ttl is smaller than minimum backend storage ttl")
	ErrInvalidBackend        = errors.New("invalid backend")
	ErrIncompleteBody        = errors.New("incomplete body")
	ErrIncompleteBodyWrite   = errors.New("incomplete body write")
	ErrExpired               = errors.New("expired storage object still in storage")
	ErrInvalidMetadata       = errors.New("invalid metadata")
	ErrContentLengthMismatch = errors.New("content length mismatch")
)

func (obj *BackendObject) Close() {
	if obj.mutex != nil {
		obj.mutex.RUnlock()
		obj.mutex = nil
	}
}

func (obj *BackendObject) IsValid() error {
	if obj.Metadata.Expires.Before(time.Now()) {
		return ErrExpired
	}
	if obj.Metadata.Updated.After(time.Now()) {
		return ErrInvalidMetadata
	}
	if len(obj.Body) > 0 {
		if obj.Metadata.BodySize != len(obj.Body) {
			return ErrIncompleteBody
		}
		contentLenStr := obj.Headers.Get("Content-Length")
		if contentLenStr != "" {
			contentLen, err := strconv.Atoi(contentLenStr)
			if err == nil {
				if contentLen != len(obj.Body) {
					return ErrContentLengthMismatch
				}
			}
		}
	}
	return nil
}

func (base *Base) IsCacheable(storageObj *BackendObject) error {
	if base.config.MinTtl > 0 && storageObj.Metadata.Expires.Unix()-time.Now().Unix() < int64(base.config.MinTtl) {
		return ErrTtlTooSmall
	}
	if storageObj.Metadata.BodySize > base.config.MaxBodySize || len(storageObj.Body) > base.config.MaxBodySize {
		return ErrTooBig
	}
	return nil
}

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

func Serialize(data interface{}) ([]byte, error) {
	serData, serErr := msgpack.Marshal(data)
	if serErr != nil {

		log.Println(serErr)
		return nil, serErr
	}
	return serData, nil
}

func Unserialize(serializedData []byte, dstData interface{}) error {
	serErr := msgpack.Unmarshal(serializedData, dstData)
	if serErr != nil {
		log.Println(serErr)
		return serErr
	}
	return nil
}
