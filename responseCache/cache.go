package responseCache

import (
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/andybalholm/redwood/responseCache/storage"
	"github.com/zeebo/xxh3"
)

type CacheObject struct {
	storage.StorageObject
	cacheKey         string
	metadataCacheKey string
	backends         int
}

type Cache struct {
	storage *storage.Storage
}

func NewCache(storage *storage.Storage) *Cache {
	cache := &Cache{
		storage: storage,
	}
	return cache
}

func (cache *Cache) GetStorage() *storage.Storage {
	return cache.storage
}

func (cache *Cache) Get(req *http.Request, fields ...string) (cacheObj *CacheObject, err error) {

	// init
	if len(fields) == 0 {
		fields = []string{"statusCode", "metadata", "headers", "body"}
	} else if !slices.Contains(fields, "metadata") { // metadata is mandatory because it stores the vary field
		fields = append(fields, "metadata")
	}
	var storageObj *storage.StorageObject
	var fromBackend int

	// fetch initial object - could be real object or metadata-only
	metadataKey := getCacheKey(req, "")
	storageObj, fromBackend, err = cache.storage.Get(metadataKey, fields...)
	if err != nil {
		return
	}

	// if it's the real object return it
	cacheObj = &CacheObject{
		StorageObject:    *storageObj,
		metadataCacheKey: metadataKey,
		backends:         fromBackend,
	}
	if cacheObj.Metadata.Vary == "" {
		cacheObj.cacheKey = metadataKey
		return
	}

	// if it's not the real object get the real object and return it
	cacheKey := getCacheKey(req, varyVals(cacheObj.Metadata.Vary, req.Header))
	storageObj, fromBackend, err = cache.storage.Get(cacheKey, fields...)
	if err != nil {
		return
	}
	cacheObj.StorageObject = *storageObj
	cacheObj.cacheKey = cacheKey
	cacheObj.backends = fromBackend

	return
}

func (cache *Cache) Set(statusCode int, metadata storage.StorageMetadata, req *http.Request, respHeaders http.Header, body string) error {

	// init
	storageObj := &storage.StorageObject{
		Metadata: metadata,
	}

	// if it has vary headers we need to store metadata-only object
	// we assume that requests that vary have the same caching directives for all variations
	if len(metadata.Vary) > 0 {
		cacheKey := getCacheKey(req, "")
		err := cache.storage.Set(cacheKey, storageObj)
		if err != nil {
			return err
		}
	}

	// set the real cache object
	storageObj.StatusCode = statusCode
	storageObj.Headers = respHeaders
	storageObj.Body = body
	cacheKey := getCacheKey(req, varyVals(metadata.Vary, req.Header))
	err := cache.storage.Set(cacheKey, storageObj)
	return err

}

func (cache *Cache) Update(req *http.Request, metadata storage.StorageMetadata) error {

	storageObj := &storage.StorageObject{
		Metadata: metadata,
	}

	// update metadata for the main cache entry (the one without vary)
	cacheKey := getCacheKey(req, "")
	err := cache.storage.Update(cacheKey, storageObj)
	if err != nil {
		return err
	}

	// if vary headers are present update metadata for the actual cache object
	if len(metadata.Vary) > 0 {
		cacheKey = getCacheKey(req, varyVals(metadata.Vary, req.Header))
		err := cache.storage.Update(cacheKey, storageObj)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cacheObj *CacheObject) IsStale() bool {
	return cacheObj.Metadata.IsStale()
}

func HashKey(key string) string {
	sum := xxh3.HashString128(key)
	hiStr := strconv.FormatUint(sum.Hi, 16)
	loStr := strconv.FormatUint(sum.Lo, 16)
	for i := 0; len(hiStr) < 16; i++ {
		hiStr = "0" + hiStr
	}
	for i := 0; len(loStr) < 16; i++ {
		loStr = "0" + loStr
	}
	return hiStr + loStr
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

func getCacheKey(req *http.Request, varyHeader string) string {
	keyStr := req.Host + " " + req.RequestURI + strings.Join(req.Header.Values("Cookie"), "") + strings.Join(req.Header.Values("X-API-Key"), "")
	if varyHeader != "" {
		varyArr := strings.Split(varyHeader, ",")
		for _, v := range varyArr {
			keyStr += " " + req.Header.Get(v)
		}
	}
	return HashKey(keyStr)
}
