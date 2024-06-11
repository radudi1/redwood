package responseCache

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/radudi1/prioworkers"
	"github.com/radudi1/stopwatch"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type Counters struct {
	Hits             atomic.Uint64
	HitBytes         atomic.Uint64
	Misses           atomic.Uint64
	MissBytes        atomic.Uint64
	Uncacheable      atomic.Uint64
	UncacheableBytes atomic.Uint64
	Sets             atomic.Uint64
	Updates          atomic.Uint64
	Revalidations    atomic.Uint64
	CacheErr         atomic.Uint64
	SerErr           atomic.Uint64
	EncodeErr        atomic.Uint64
	ReadErr          atomic.Uint64
	WriteErr         atomic.Uint64
}

type stopWatches struct {
	getSw, setSw stopwatch.StopWatch
}

type cacheObjType struct {
	Host       string
	Url        string
	StatusCode int
	Headers    http.Header
	Body       []byte
}

type cacheObjInfoType struct {
	StatusCode int
	Status     string
	IsStale    bool
	CacheKey   string
}

// cacheable http response codes
// ALWAYS ORDERED ASCENDING
var cacheableStatusCodes = [...]int{
	200, // ok
	203, // transformed by proxy
	204, // no content
	301, // moved permanently
	302, // found
	308, // permanent redirect
}

var uncacheableCacheControlDirectives = [...]string{
	"no-cache",
	"no-store",
	"private",
	"must-revalidate",
	"proxy-revalidate",
	"must-understand",
}

var (
	counters Counters

	revalidateReqs      map[string]struct{}
	revalidateReqsMutex sync.Mutex

	httpClient *http.Client
)

func Get(w http.ResponseWriter, req *http.Request) (found bool, stats *stopWatches) {

	if !config.Cache.Enabled {
		return false, nil
	}

	found = false
	responseSent := false
	cacheObjInfo := &cacheObjInfoType{
		StatusCode: http.StatusInternalServerError,
	}
	var cacheObj cacheObjType

	// init
	stats = &stopWatches{}
	stats.getSw = stopwatch.Start()
	defer stats.getSw.Stop()

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	cacheObjInfo.CacheKey = GetKey(req, "")

	// send headers and update stats on function return
	defer (func() {
		if found {
			counters.Hits.Add(1)
			counters.HitBytes.Add(uint64(len(cacheObj.Headers) + len(cacheObj.Body)))
			if !responseSent {
				sendHeaders(w, cacheObj.Headers, req, cacheObjInfo, stats)
			}
		}
	})()

	// we ONLY cache some requests
	if req.Method != "GET" && req.Method != "HEAD" {
		return
	}
	cacheControl := splitHeader(req.Header, "Cache-Control", ",")
	if MapHasAnyKey(cacheControl, uncacheableCacheControlDirectives[:]) || cacheControl["max-age"] == "0" {
		return
	}

	// try to serve from cache
	var cacheObjSer string
	var redisErr error
	cacheObjSer, redisErr = CacheConn().Get(redisContext, cacheObjInfo.CacheKey).Result()
	if redisErr == redis.Nil {
		return
	}
	serErr := msgpack.Unmarshal([]byte(cacheObjSer), &cacheObj)
	if serErr != nil {
		counters.SerErr.Add(1)
		log.Println(serErr)
		return
	}
	// check if this is the real response or just the dummy one used for vary
	if cacheObj.Headers.Get("Vary") != "" {
		// we have a vary header so we fetch the real response according to vary
		cacheObjInfo.CacheKey = GetKey(req, cacheObj.Headers.Get("Vary"))
		cacheObjSer, redisErr = CacheConn().Get(redisContext, cacheObjInfo.CacheKey).Result()
		if redisErr == redis.Nil {
			return
		}
		serErr := msgpack.Unmarshal([]byte(cacheObjSer), &cacheObj)
		if serErr != nil {
			counters.SerErr.Add(1)
			log.Println(serErr)
			return
		}
	}

	// check if cached object is stale and decide what to do if it is
	respCacheControl := splitHeader(cacheObj.Headers, "Cache-Control", ",")
	respMaxAge, maxAgeErr := getMaxAge(respCacheControl, cacheObj.Headers, false)
	respAge := getResponseAge(cacheObj.Headers)
	cacheObjInfo.IsStale = maxAgeErr != nil || respAge > respMaxAge
	// check if cached object is stale and ServeStale is not enabled - we can then use stale-while-revalidate
	// if ServeStale is enabled we always server stale
	if cacheObjInfo.IsStale && (!config.StandardViolations.EnableStandardViolations || !config.StandardViolations.ServeStale) {
		// check if we have stale-while-revalidate and the object is fresh enough for it
		maxStaleAge, _ := MapElemToI(cacheControl, "stale-while-revalidate")
		if respAge > respMaxAge+maxStaleAge { // cached object is stale and we also passed stale-while-revalidate window - we can't serve
			return
		}
	}

	// if it's a HEAD request or has certain response status codes we don't send the body  - RFCs 2616 7230
	// https://stackoverflow.com/questions/78182848/does-http-differentiate-between-an-empty-body-and-no-body
	if req.Method == "HEAD" || int(cacheObj.StatusCode/100) == 1 || cacheObj.StatusCode == 204 || cacheObj.StatusCode == 304 {
		found = true
		cacheObjInfo.StatusCode = cacheObj.StatusCode
		cacheObjInfo.Status = "HIT"
		return
	}
	// if the client just wants validation we can already reply with valid
	if oldETag := req.Header.Get("If-None-Match"); oldETag != "" {
		if oldETag == cacheObj.Headers.Get("ETag") {
			cacheObj.Headers.Add("X-Cache", "NOTMODIF")
			found = true
			cacheObjInfo.StatusCode = http.StatusNotModified
			cacheObjInfo.Status = "NOTMODIF"
			return
		}
	} else if req.Header.Get("If-Modified-Since") != "" {
		cacheObj.Headers.Add("X-Cache", "NOTMODIF")
		found = true
		cacheObjInfo.StatusCode = http.StatusNotModified
		cacheObjInfo.Status = "NOTMODIF"
		return
	}

	// check if body needs reencoding (compression algo not supported by client)
	respEncoding := cacheObj.Headers.Get("Content-Encoding")
	if respEncoding != "" {
		acceptEncoding := splitHeader(req.Header, "Accept-Encoding", ",")
		if !MapHasKey(acceptEncoding, respEncoding) {
			var encodedBuf bytes.Buffer
			encoding, reEncodeErr := reEncode(&encodedBuf, &cacheObj.Body, respEncoding, acceptEncoding)
			if reEncodeErr != nil {
				counters.EncodeErr.Add(1)
				log.Println("Could not reeencode cached response: ", reEncodeErr)
				return
			}
			cacheObj.Body = encodedBuf.Bytes()
			if encoding == "" {
				cacheObj.Headers.Del("Content-Encoding")
			} else {
				cacheObj.Headers.Set("Content-Encoding", encoding)
			}
			cacheObj.Headers.Set("Content-Length", fmt.Sprint(len(cacheObj.Body)))
		}
	}

	// send response headers
	found = true
	responseSent = true
	cacheObjInfo.StatusCode = cacheObj.StatusCode
	cacheObjInfo.Status = "HIT"
	cacheObj.Headers.Add("X-Cache", "HIT")
	sendHeaders(w, cacheObj.Headers, req, cacheObjInfo, stats)
	// send response body
	n, writeErr := w.Write(cacheObj.Body)
	if writeErr != nil {
		counters.WriteErr.Add(1)
		log.Println(writeErr)
		return
	}
	if n != len(cacheObj.Body) {
		counters.WriteErr.Add(1)
		log.Println("Written ", n, " bytes to client instead of ", len(cacheObj.Body), "!!!")
	}
	return
}

func Set(req *http.Request, resp *http.Response, stats *stopWatches) {
	if !config.Cache.Enabled {
		return
	}
	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}
	set(req, resp, stats, srcClient)
}

func set(req *http.Request, resp *http.Response, stats *stopWatches, reqSrc int) {

	var logStatus string
	switch reqSrc {
	case srcClient:
		defer func() {
			cacheLog(req, resp.StatusCode, resp.Header, logStatus, GetKey(req, resp.Header.Get("Vary")), stats)
		}()
	case srcRevalidate:
		defer func() {
			revalidateLog(req, resp.StatusCode, resp.Header, logStatus, GetKey(req, resp.Header.Get("Vary")), stats)
		}()
	default:
		panic("Invalid request source received for responsecache set")
	}

	stats.setSw = stopwatch.Start()
	defer stats.setSw.Stop()

	var cacheObj *cacheObjType

	// update counters when function returns
	defer (func() {
		if logStatus == "MISS" {
			counters.Misses.Add(1)
			counters.MissBytes.Add(uint64(len(cacheObj.Body)))
		} else {
			counters.Uncacheable.Add(1)
			if resp.ContentLength > 0 {
				counters.UncacheableBytes.Add(uint64(resp.ContentLength))
			}
		}
	})()

	// if it's a nobump domain we should not cache it
	if MapHasKey(noBumpDomains, req.Host) {
		logStatus = "UC_NOBUMP"
		return
	}
	// if it's a cloudflare damned domain that has stupid protection (eg: ja3) we add it to nobump domain list so that future requests pass unbumped and allow user to access it properly - stupid stupid but what elese is there to do
	if config.Cache.AutoAddToNoBump && resp.StatusCode == 403 && resp.Header.Get("server") == "cloudflare" {
		noBumpDomains[req.Host] = struct{}{}
		CacheConn().SAdd(redisContext, noBumpDomainsKey, req.Host)
		logStatus = "UC_TONOBUMP"
		return
	}

	// we ONLY cache some requests
	if req.Method != "GET" {
		logStatus = "UC_METHOD"
		return
	}
	if !contains(cacheableStatusCodes[:], resp.StatusCode) {
		logStatus = "UC_RESPCODE"
		return
	}
	if req.Header.Get("Authorization") != "" || resp.Header.Get("WWW-Authenticate") != "" || strings.TrimSpace(req.Header.Get("Vary")) == "*" {
		logStatus = "UC_AUTHVARY"
		return
	}
	cacheControl := splitHeader(resp.Header, "Cache-Control", ",")
	if resp.Header.Get("Set-Cookie") != "" && !MapHasKey(cacheControl, "public") && !MapHasKey(cacheControl, "s-maxage") {
		logStatus = "UC_SETCOOKIE"
		return
	}
	if MapHasAnyKey(cacheControl, uncacheableCacheControlDirectives[:]) || cacheControl["max-age"] == "0" {
		// if dangerous heuristics are not enabled we can't cache it
		if !config.StandardViolations.EnableStandardViolations || !config.StandardViolations.EnableDangerousHeuristics {
			logStatus = "UC_CACHECTRL"
			return
		}
		// we are allowed to use dangerous heuristics
		// we check that we have specific cache control indications that we can cache
		// and that there are no indications that the response is specific to a certain user/session/etc.
		if (!MapHasKey(cacheControl, "s-maxage") && !MapHasKey(cacheControl, "max-age")) ||
			cacheControl["max-age"] == "0" || req.Header.Get("Cookie") != "" || resp.Header.Get("Set-Cookie") != "" {
			logStatus = "UC_CACHECTRL"
			return
		}
	}
	sizeLimit := int(math.Min(float64(config.Cache.MaxSize), 512*1024*1024)) // redis max object size is 512 MB
	if resp.ContentLength > int64(sizeLimit) {
		logStatus = "UC_TOOBIG"
		return
	}

	// Compute cache TTL from response
	maxAge, err := getMaxAge(cacheControl, resp.Header, config.StandardViolations.EnableStandardViolations)
	if err != nil {
		logStatus = "UC_STALE"
		return
	}

	// fetch response body
	lr := &io.LimitedReader{
		R: resp.Body,
		N: int64(sizeLimit),
	}
	body, err := io.ReadAll(lr)
	// Servers that use broken chunked Transfer-Encoding can give us unexpected EOFs,
	// even if we got all the content.
	if err == io.ErrUnexpectedEOF && resp.ContentLength == -1 {
		err = nil
	}
	if err != nil {
		counters.ReadErr.Add(1)
		log.Println("Could not read body for caching")
		logStatus = "UC_RDBODYERR"
		return
	}
	if lr.N == 0 {
		// We read maxLen without reaching the end.
		resp.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), resp.Body))
		logStatus = "UC_TOOBIG"
		return
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if len(body) >= sizeLimit {
		logStatus = "UC_TOOBIG"
		return
	}

	// try to cache
	cacheObj = &cacheObjType{
		Host:       req.Host,
		Url:        req.RequestURI,
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       body,
	}

	go setWorker(cacheObj, req, maxAge, stats)

	logStatus = "MISS"

}

func setWorker(cacheObj *cacheObjType, req *http.Request, maxAge int, stats *stopWatches) {

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(setWPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	var redisErr error

	// if we have a vary header we store a mock response just with headers so we can get the vary header on fetch
	if cacheObj.Headers.Get("Vary") != "" {
		dummyCacheObj := *cacheObj
		dummyCacheObj.Body = nil
		cacheObjSer, serErr := msgpack.Marshal(dummyCacheObj)
		if serErr != nil {
			counters.SerErr.Add(1)
			log.Println(serErr)
			cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SERERR", "", stats)
			return
		}
		redisErr = CacheConn().Set(redisContext, GetKey(req, ""), string(cacheObjSer), time.Duration(maxAge)*time.Second).Err()
		if redisErr != nil {
			counters.CacheErr.Add(1)
			log.Println("Redis set error ", redisErr)
			cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SETERR", "", stats)
			return
		}
	}

	cacheObjSer, serErr := msgpack.Marshal(cacheObj)
	if serErr != nil {
		counters.SerErr.Add(1)
		log.Println(serErr)
		cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SERERR", "", stats)
		return
	}
	// store response with body
	redisErr = CacheConn().Set(redisContext, GetKey(req, cacheObj.Headers.Get("Vary")), string(cacheObjSer), time.Duration(maxAge)*time.Second).Err()
	if redisErr != nil {
		counters.CacheErr.Add(1)
		log.Println("Redis set error ", redisErr)
		cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SETERR", "", stats)
		return
	}
	counters.Sets.Add(1)

}

func updateTtlWorker(req *http.Request, respHeaders http.Header) {
	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(updateWPrio)
		defer prioworkers.WorkEnd(workerId)
	}
	if respHeaders.Get("Date") == "" {
		return
	}
	cacheControl := splitHeader(respHeaders, "Cache-Control", ",")
	maxAge, err := getMaxAge(cacheControl, respHeaders, config.StandardViolations.EnableStandardViolations)
	if err != nil {
		return
	}
	cacheKey := GetKey(req, "")
	var redisErr error
	redisErr = CacheConn().Expire(redisContext, cacheKey, time.Duration(maxAge)*time.Second).Err()
	if redisErr != nil {
		counters.CacheErr.Add(1)
		log.Println("Redis update error ", redisErr)
		return
	}
	if respHeaders.Get("Vary") != "" {
		cacheKey := GetKey(req, respHeaders.Get("Vary"))
		redisErr = CacheConn().Expire(redisContext, cacheKey, time.Duration(maxAge)*time.Second).Err()
		if redisErr != nil {
			counters.CacheErr.Add(1)
			log.Println("Redis update error ", redisErr)
			return
		}
	}
	counters.Updates.Add(1)
}

func revalidateWorker(req *http.Request, respHeaders http.Header) {

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(revalidateWPrio)
		defer prioworkers.WorkEnd(workerId)
	}
	// if this is request is currently revalidating (by another goroutine) we skip it
	cacheKey := GetKey(req, respHeaders.Get("Vary"))
	revalidateReqsMutex.Lock()
	if MapHasKey(revalidateReqs, cacheKey) {
		revalidateReqsMutex.Unlock()
		return
	}
	// add current request to currently revalidating list
	revalidateReqs[cacheKey] = struct{}{}
	revalidateReqsMutex.Unlock()
	// update stats
	counters.Revalidations.Add(1)
	// conditional validation if possible
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	if respHeaders.Get("ETag") != "" {
		req.Header.Add("If-None-Match", strings.Join(respHeaders.Values("ETag"), ", "))
	} else if respHeaders.Get("Date") != "" {
		req.Header.Add("If-Modified-Since", respHeaders.Get("Date"))
	}
	// do request
	if req.ContentLength == 0 {
		req.Body.Close()
		req.Body = nil
	}
	reqURI := req.RequestURI // URI will have to be restored before caching because it's used to compute cache key
	req.RequestURI = ""      // it is required by library that RequestURI is not set
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("Error making HTTP request:", err)
		return
	}
	// process response
	req.RequestURI = reqURI // restore URI before caching because it's used to compute cache key
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	if resp.StatusCode == http.StatusNotModified {
		go updateTtlWorker(req, resp.Header)
	} else {
		set(req, resp, &stopWatches{}, srcRevalidate)
	}
	resp.Body.Close()
	// remove request from currently revalidating list
	revalidateReqsMutex.Lock()
	delete(revalidateReqs, cacheKey)
	revalidateReqsMutex.Unlock()

}

func sendHeaders(w http.ResponseWriter, respHeaders http.Header, req *http.Request, cacheObjInfo *cacheObjInfoType, stats *stopWatches) {
	for k, v := range respHeaders {
		w.Header().Set(k, strings.Join(v, " "))
	}
	w.WriteHeader(cacheObjInfo.StatusCode)
	if req != nil {
		cacheLog(req, cacheObjInfo.StatusCode, respHeaders, cacheObjInfo.Status, cacheObjInfo.CacheKey, stats)
	}
	if cacheObjInfo.IsStale { // if it's stale revalidate
		go revalidateWorker(req.Clone(redisContext), respHeaders.Clone())
	} else { // if not stale update ttl
		go updateTtlWorker(req.Clone(redisContext), respHeaders.Clone())
	}
}

func getMaxAge(cacheControl map[string]string, respHeaders http.Header, withViolations bool) (int, error) {
	if MapHasKey(cacheControl, "immutable") {
		return validateMaxAge(config.Cache.MaxAge, respHeaders)
	}
	// if standard violation is enabled use this algorithm
	if withViolations {
		maxAge := getCacheControlTtl(cacheControl, respHeaders)
		// if OverrideCacheControl is enabled we use this algo
		if config.StandardViolations.OverrideCacheControl {
			maxAge = max(maxAge, min(getLastModifiedTtl(respHeaders), config.StandardViolations.OverrideCacheControlMaxAge))
			if maxAge > 0 {
				return validateMaxAge(maxAge, respHeaders)
			}
			// no cache-control and no last-modified
			if age := getAge(respHeaders); age > 0 { // if age header is present we compute ttl based on that
				return validateMaxAge(age/config.Cache.AgeDivisor, respHeaders)
			}
			// we resort to expire unless it is overriden
			if !config.StandardViolations.OverrideExpire {
				if maxAge = getExpiresTtl(respHeaders); maxAge > 0 {
					return validateMaxAge(maxAge, respHeaders)
				}
			}
			// there are no headers present that we can't compute ttl on so we return the minimum default
			return validateMaxAge(config.StandardViolations.DefaultAge, respHeaders)
		}
		// if OverrideCacheControl is disabled we use this algo
		if maxAge > 0 {
			return validateMaxAge(maxAge, respHeaders)
		}
		if !config.StandardViolations.OverrideExpire {
			if maxAge = getExpiresTtl(respHeaders); maxAge > 0 {
				return validateMaxAge(maxAge, respHeaders)
			}
		}
		return validateMaxAge(getLastModifiedTtl(respHeaders), respHeaders)
	} else { // standard violations are not enabled
		// return from cache-control if possible
		if maxAge := getCacheControlTtl(cacheControl, respHeaders); maxAge > 0 {
			return validateMaxAge(maxAge, respHeaders)
		}
		if maxAge := getExpiresTtl(respHeaders); maxAge > 0 {
			return validateMaxAge(maxAge, respHeaders)
		}
		// otherwise return from last-modified
		return validateMaxAge(getLastModifiedTtl(respHeaders), respHeaders)
	}
}

func validateMaxAge(maxAge int, respHeaders http.Header) (int, error) {
	// this is needed in case we need to update ttl of a previously set cache object
	maxAge = maxAge - getResponseAge(respHeaders)
	// if we have a restricted mime prefix we cap maxAge accordingly
	for _, v := range config.Cache.RestrictedMimePrefixes {
		if strings.HasPrefix(respHeaders.Get("Content-Type"), v) {
			maxAge = min(maxAge, config.Cache.RestrictedMaxAge)
			break
		}
	}
	// make sure maxAge is within range
	if maxAge < 1 {
		return maxAge, fmt.Errorf("invalid maxAge value")
	}
	// cap maxAge to config setting
	maxAge = min(maxAge, config.Cache.MaxAge)
	return maxAge, nil
}

func getCacheControlTtl(cacheControl map[string]string, headers http.Header) int {
	smaxAge, _ := MapElemToI(cacheControl, "s-maxage")
	// if we have s-maxage and s-maxage overriding is not enabled we return it
	if smaxAge > 0 && (!config.StandardViolations.EnableStandardViolations || !config.StandardViolations.OverrideSMaxAge) {
		return smaxAge - getAge(headers)
	}
	// s-maxage is not present or it's overriden so we fetch maxage and return maximum of the 2 values minus age
	maxAge, _ := MapElemToI(cacheControl, "max-age")
	ttl := max(maxAge, smaxAge)
	age := getAge(headers)
	if config.StandardViolations.EnableStandardViolations && ttl-age < 0 { // this is NOT according to standard !!! but some servers send responses which are extensively stale - if response is good for them it should be good for us
		return ttl
	}
	return ttl - age
}

func getExpiresTtl(headers http.Header) int {
	if expires := headers.Get("Expires"); expires != "" {
		tExpires, err := time.Parse(time.RFC1123, expires)
		if err == nil {
			return int(tExpires.Sub(time.Now()).Seconds())
		}
	}
	return 0
}

func getLastModifiedTtl(headers http.Header) int {
	if config.Cache.AgeDivisor < 1 { // if we don't have a valid AgeDivisor we can't compute ttl based on last-modified
		return 0
	}
	if lastModified := headers.Get("Last-Modified"); lastModified != "" {
		tLastModified, err := time.Parse(time.RFC1123, lastModified)
		if err == nil {
			return int(float64(time.Since(tLastModified).Seconds()) / float64(config.Cache.AgeDivisor))
		}
	}
	return 0
}

func getAge(headers http.Header) int {
	age, ageErr := strconv.Atoi(headers.Get("Age"))
	if ageErr == nil {
		return age
	}
	return 0
}

func getResponseAge(headers http.Header) int {
	tDate, err := time.Parse(time.RFC1123, headers.Get("Date"))
	if err != nil {
		return 0
	}
	return int(time.Since(tDate).Seconds())
}
