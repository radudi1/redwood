package responseCache

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
)

type metadata struct {
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

type cacheObjType struct {
	statusCode       int
	metadata         metadata
	headers          http.Header
	body             string
	metadataCacheKey string
	cacheKey         string
}

type cacheCountersT struct {
	SerErr   atomic.Uint64
	CacheErr atomic.Uint64
}

var cacheCounters cacheCountersT

func cacheGet(req *http.Request, fields []string) (cacheObj *cacheObjType, err error) {

	// init
	cacheObj = &cacheObjType{
		metadataCacheKey: GetKey(req, ""),
	}
	if len(fields) == 0 {
		fields = []string{"statusCode", "metadata", "headers", "body"}
	} else if !slices.Contains(fields, "metadata") { // metadata is mandatory because it stores the vary field
		fields = append(fields, "metadata")
	}
	var serFields map[string]string
	var redisErr error

	serFields, redisErr = Hmget(cacheObj.metadataCacheKey, fields...)
	if redisErr != nil {
		if redisErr != rueidis.Nil {
			cacheCounters.CacheErr.Add(1)
		}
		err = redisErr
		return
	}
	if serFields["metadata"] == "" { // object was not found
		return nil, rueidis.Nil
	}
	if err = Unserialize(serFields["metadata"], &cacheObj.metadata); err != nil {
		cacheCounters.SerErr.Add(1)
		return
	}

	// if there's a vary header then fetch the real response
	if cacheObj.metadata.Vary != "" {
		cacheObj.cacheKey = GetKey(req, varyVals(cacheObj.metadata.Vary, req.Header))
		serFields, redisErr = Hmget(cacheObj.cacheKey, fields...)
		if redisErr != nil {
			if redisErr != rueidis.Nil {
				cacheCounters.CacheErr.Add(1)
			}
			err = redisErr
			return
		}
		if serFields["metadata"] == "" { // object was not found
			return nil, rueidis.Nil
		}
		if err = Unserialize(serFields["metadata"], &cacheObj.metadata); err != nil {
			cacheCounters.SerErr.Add(1)
			return
		}
	} else {
		cacheObj.cacheKey = cacheObj.metadataCacheKey
	}

	var statusCode int64
	statusCode, err = strconv.ParseInt(serFields["statusCode"], 10, 64)
	if err != nil {
		return
	}
	cacheObj.statusCode = int(statusCode)

	if serFields["headers"] != "" {
		if err = Unserialize(serFields["headers"], &cacheObj.headers); err != nil {
			cacheCounters.SerErr.Add(1)
			return
		}
	}

	cacheObj.body = serFields["body"]

	return
}

func cacheSet(statusCode int, metadata metadata, req *http.Request, respHeaders http.Header, body string) error {
	metadataSer, serErr := Serialize(metadata)
	if serErr != nil {
		cacheCounters.SerErr.Add(1)
		return serErr
	}
	headersSer, serErr := Serialize(respHeaders)
	if serErr != nil {
		cacheCounters.SerErr.Add(1)
		return serErr
	}
	// if we have Vary then we store metadata-only
	// we assume that requests that vary have the same caching directives for all variations
	if metadata.Vary != "" {
		cacheKey := GetKey(req, "")
		redisErr := redisConn.Do(
			redisContext,
			redisConn.B().Hset().
				Key(cacheKey).
				FieldValue().
				FieldValue("metadata", metadataSer).
				Build()).
			Error()
		if redisErr != nil {
			cacheCounters.CacheErr.Add(1)
			return redisErr
		}
		if redisErr = cacheExpire(cacheKey, &metadata); redisErr != nil {
			cacheCounters.CacheErr.Add(1)
			return redisErr
		}
	}
	// set the real response with headers and body included
	cacheKey := GetKey(req, varyVals(metadata.Vary, req.Header))
	redisErr := redisConn.Do(
		redisContext,
		redisConn.B().Hset().
			Key(cacheKey).
			FieldValue().
			FieldValue("statusCode", strconv.Itoa(statusCode)).
			FieldValue("metadata", metadataSer).
			FieldValue("headers", headersSer).
			FieldValue("body", body).
			Build()).
		Error()
	if redisErr != nil {
		cacheCounters.CacheErr.Add(1)
		return redisErr
	}
	if redisErr = cacheExpire(cacheKey, &metadata); redisErr != nil {
		cacheCounters.CacheErr.Add(1)
		return redisErr
	}
	return nil
}

func cacheUpdate(req *http.Request, metadata metadata) error {
	metadataSer, serErr := Serialize(metadata)
	if serErr != nil {
		cacheCounters.SerErr.Add(1)
		return serErr
	}
	// update metadata for the main cache entry (the one without vary)
	cacheKey := GetKey(req, "")
	redisErr := redisConn.Do(
		redisContext,
		redisConn.B().Hset().
			Key(cacheKey).
			FieldValue().
			FieldValue("metadata", metadataSer).
			Build()).
		Error()
	if redisErr != nil {
		cacheCounters.CacheErr.Add(1)
		return redisErr
	}
	if redisErr = cacheExpire(cacheKey, &metadata); redisErr != nil {
		cacheCounters.CacheErr.Add(1)
		return redisErr
	}
	// if vary headers are present update metadata for the actual cache object
	if metadata.Vary != "" {
		cacheKey = GetKey(req, varyVals(metadata.Vary, req.Header))
		redisErr = redisConn.Do(
			redisContext,
			redisConn.B().Hset().
				Key(cacheKey).
				FieldValue().
				FieldValue("metadata", metadataSer).
				Build()).
			Error()
		if redisErr != nil {
			cacheCounters.CacheErr.Add(1)
			return redisErr
		}
		if redisErr = cacheExpire(cacheKey, &metadata); redisErr != nil {
			cacheCounters.CacheErr.Add(1)
			return redisErr
		}
	}
	return nil
}

// Sets redis ttl for key according to metadata
func cacheExpire(cacheKey string, metadata *metadata) error {
	return redisConn.Do(
		redisContext,
		redisConn.B().Expireat().
			Key(cacheKey).
			Timestamp(metadata.Expires.Unix()).
			Build()).
		Error()
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

func (metadata *metadata) IsStale() bool {
	return metadata.Stale.Before(time.Now())
}

func (cacheObj *cacheObjType) IsStale() bool {
	return cacheObj.metadata.IsStale()
}
