package responseCache

import (
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/andybalholm/redwood/responseCache/storage"
	"github.com/zeebo/xxh3"
)

type CacheObject struct {
	storage.StorageObject
	MetadataCacheKey string
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

	// fetch initial object - could be real object or metadata-only
	metadataKey := getCacheKey(req, "")
	storageObj, err = cache.storage.Get(metadataKey, fields...)
	if err != nil {
		return
	}

	// if it's the real object return it
	cacheObj = &CacheObject{
		StorageObject:    *storageObj,
		MetadataCacheKey: metadataKey,
	}
	cacheObj.CacheKey = getCacheKey(req, varyVals(cacheObj.Metadata.Vary, req.Header))
	if metadataKey == cacheObj.CacheKey {
		return
	}

	// if it's not real object release metadata object
	if storageObj != nil {
		storageObj.Close()
	}

	// if it's not the real object get the real object and return it
	storageObj, err = cache.storage.Get(cacheObj.CacheKey, fields...)
	if err != nil {
		cacheObj = nil
		return
	}
	cacheObj.StorageObject = *storageObj

	return
}

func (cache *Cache) WriteBodyToClient(cacheObj *CacheObject, w io.Writer) error {
	return cache.storage.WriteBodyToClient(&cacheObj.StorageObject, w)
}

func (cache *Cache) Set(statusCode int, metadata storage.StorageMetadata, req *http.Request, respHeaders http.Header, body []byte) error {

	// init
	storageObj := &storage.StorageObject{
		Backends: 0xff,
	}
	storageObj.Metadata = metadata
	metadataKey := getCacheKey(req, "")
	cacheKey := getCacheKey(req, varyVals(metadata.Vary, req.Header))

	// if it has vary headers we need to store metadata-only object
	// we assume that requests that vary have the same caching directives for all variations
	if metadataKey != cacheKey {
		storageObj.CacheKey = metadataKey
		err := cache.storage.Set(storageObj)
		if err != nil && storageObj.Backends == 0 {
			return err
		}
	}

	// set the real cache object
	storageObj.CacheKey = cacheKey
	storageObj.StatusCode = statusCode
	storageObj.Headers = respHeaders
	storageObj.Body = body
	err := cache.storage.Set(storageObj)
	return err

}

func (cache *Cache) Update(req *http.Request, metadata storage.StorageMetadata) error {

	// update metadata for the actual cache object
	cacheKey := getCacheKey(req, varyVals(metadata.Vary, req.Header))
	err := cache.storage.Update(cacheKey, &metadata)
	if err != nil {
		return err
	}

	// update metadata for the metadata-only object
	if metadata.Vary != "" {
		cacheKey = getCacheKey(req, "")
		metadata.BodySize = 0
		metadata.BodyChunkLen = 0
		err = cache.storage.Update(cacheKey, &metadata)
	}

	return err
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
