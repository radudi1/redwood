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

type cacheReqResp struct {
	cacheObj cacheObjType
	req      http.Request
	maxAge   int
	stats    stopWatches
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

var counters Counters
var setChan chan cacheReqResp
var updateChan chan cacheReqResp
var revalidateChan chan http.Request

func Get(w http.ResponseWriter, req *http.Request) (found bool, stats stopWatches) {

	if config.Redis.NumConn < 1 {
		return false, stopWatches{}
	}

	found = false
	statusCode := http.StatusInternalServerError
	var cacheObj cacheObjType
	var cacheStatus string
	var cacheKey string

	// init
	stats.getSw = stopwatch.Start()
	defer stats.getSw.Stop()

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	cacheKey = getKey(req, "")

	// send headers and update stats on function return
	defer (func() {
		if found {
			counters.Hits.Add(1)
			counters.HitBytes.Add(uint64(len(cacheObj.Headers) + len(cacheObj.Body)))
			if cacheStatus != "" {
				sendHeaders(w, statusCode, cacheObj.Headers, cacheStatus, cacheKey, req, stats)
			}
		}
	})()

	// we ONLY cache some requests
	if req.Method != "GET" && req.Method != "HEAD" {
		return
	}
	cacheControl := splitHeader(&req.Header, "Cache-Control", ",")
	if MapHasKey(cacheControl, "no-cache") || MapHasKey(cacheControl, "no-store") || MapHasKey(cacheControl, "must-revalidate") || cacheControl["max-age"] == "0" {
		return
	}

	// try to serve from cache
	cacheObjSer, redisErr := cacheConn().Get(ctx, cacheKey).Result()
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
		cacheKey = getKey(req, cacheObj.Headers.Get("Vary"))
		cacheObjSer, redisErr := cacheConn().Get(ctx, cacheKey).Result()
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
	respCacheControl := splitHeader(&cacheObj.Headers, "Cache-Control", ",")
	respMaxAge, maxAgeErr := getMaxAge(respCacheControl, &cacheObj.Headers, false)
	isStale := maxAgeErr != nil || respMaxAge < getResponseAge(&cacheObj.Headers)
	// check if ServeStale is enabled and act accordingly
	if !config.StandardViolations.EnableStandardViolations || !config.StandardViolations.ServeStale {
		if isStale || MapHasKey(respCacheControl, "must-revalidate") || MapHasKey(respCacheControl, "proxy-revalidate") {
			return
		}
	} else { // ServeStale is enabled
		if isStale {
			revalidateChan <- *req.Clone(ctx)
		}
	}
	// if it's a HEAD request or has certain response status codes we don't send the body  - RFCs 2616 7230
	// https://stackoverflow.com/questions/78182848/does-http-differentiate-between-an-empty-body-and-no-body
	if req.Method == "HEAD" || int(cacheObj.StatusCode/100) == 1 || cacheObj.StatusCode == 204 || cacheObj.StatusCode == 304 {
		found = true
		statusCode = cacheObj.StatusCode
		cacheStatus = "HIT"
		return
	}
	// if the client just wants validation we can already reply with valid
	if oldETag := req.Header.Get("If-None-Match"); oldETag != "" {
		if oldETag == cacheObj.Headers.Get("ETag") {
			cacheObj.Headers.Add("X-Cache", "NOTMODIF")
			found = true
			statusCode = http.StatusNotModified
			cacheStatus = "NOTMODIF"
			return
		}
	} else if req.Header.Get("If-Modified-Since") != "" {
		cacheObj.Headers.Add("X-Cache", "NOTMODIF")
		found = true
		statusCode = http.StatusNotModified
		cacheStatus = "NOTMODIF"
		return
	}

	// check if body needs reencoding (compression algo not supported by client)
	respEncoding := cacheObj.Headers.Get("Content-Encoding")
	if respEncoding != "" {
		acceptEncoding := splitHeader(&req.Header, "Accept-Encoding", ",")
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
	cacheObj.Headers.Add("X-Cache", "HIT")
	sendHeaders(w, cacheObj.StatusCode, cacheObj.Headers, "FROMCACHE", cacheKey, req, stats)
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

func Set(req *http.Request, resp *http.Response, stats stopWatches) {
	set(req, resp, stats, srcClient)
}

func set(req *http.Request, resp *http.Response, stats stopWatches, reqSrc int) {

	if config.Redis.NumConn < 1 {
		return
	}

	var logStatus string
	switch reqSrc {
	case srcClient:
		defer func() { cacheLog(req, resp.StatusCode, resp.Header, logStatus, "", stats) }()
	case srcRevalidate:
		defer func() { revalidateLog(req, resp.StatusCode, resp.Header, logStatus, "", stats) }()
	default:
		panic("Invalid request source received for responsecache set")
	}

	stats.setSw = stopwatch.Start()
	defer stats.setSw.Stop()

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	var cacheObj *cacheObjType

	// try to get body size from content-length header - needed for stats is well as check
	contentLength := int64(-1)
	contenLengthStr := strings.TrimSpace(req.Header.Get("Content-length"))
	if contenLengthStr != "" {
		n, err := strconv.ParseUint(contenLengthStr, 10, 63)
		if err == nil {
			contentLength = int64(n)
		}
	}

	// update counters when function returns
	defer (func() {
		if logStatus == "MISS" {
			counters.Misses.Add(1)
			counters.MissBytes.Add(uint64(len(cacheObj.Body)))
		} else {
			counters.Uncacheable.Add(1)
			if contentLength > 0 {
				counters.UncacheableBytes.Add(uint64(contentLength))
			}
		}
	})()

	// if it's a nobump domain we should not cache it
	if MapHasKey(noBumpDomains, req.Host) {
		logStatus = "UC_NOBUMP"
		return
	}
	// if it's a cloudflare damned domain that has stupid protection (eg: ja3) we add it to nobump domain list so that future requests pass unbumped and allow user to access it properly - stupid stupid but what elese is there to do
	if resp.StatusCode == 403 && resp.Header.Get("server") == "cloudflare" {
		noBumpDomains[req.Host] = struct{}{}
		cacheConn().SAdd(ctx, noBumpDomainsKey, req.Host)
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
	if req.Header.Get("Authorization") != "" || strings.TrimSpace(req.Header.Get("Vary")) == "*" {
		logStatus = "UC_AUTHVARY"
		return
	}
	cacheControl := splitHeader(&resp.Header, "Cache-Control", ",")
	if MapHasKey(cacheControl, "private") || MapHasKey(cacheControl, "no-cache") || MapHasKey(cacheControl, "no-store") {
		logStatus = "UC_CACHECTRL"
		return
	}
	// if req.Header.Get("Cookie") != "" || resp.Header.Get("Set-Cookie") != "" {
	// 	if !mapHasKey(cacheControl, "public") {
	// 		logStatus = "UC_COOKIE"
	// 		return
	// 	}
	// }
	sizeLimit := int(math.Min(float64(config.Cache.MaxSize), 512*1024*1024)) // redis max object size is 512 MB
	if contentLength > int64(sizeLimit) {
		logStatus = "UC_TOOBIG"
		return
	}

	// Compute cache TTL from response
	maxAge, err := getMaxAge(cacheControl, &resp.Header, config.StandardViolations.EnableStandardViolations)
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

	setChan <- cacheReqResp{cacheObj: *cacheObj, req: *req.Clone(ctx), maxAge: maxAge}

	logStatus = "MISS"

}

func setWorker(redisConn *redis.Client) {
	workerId := int64(-1)
	for {

		if config.Workers.PrioritiesEnabled && workerId >= 0 {
			prioworkers.WorkEnd(workerId)
		}
		msg := <-setChan
		if config.Workers.PrioritiesEnabled {
			workerId = prioworkers.WorkStart(setWPrio)
		}

		cacheObj := &msg.cacheObj
		req := &msg.req
		maxAge := msg.maxAge
		stats := msg.stats

		// if we have a vary header we store a mock response just with headers so we can get the vary header on fetch
		if cacheObj.Headers.Get("Vary") != "" {
			dummyCacheObj := msg.cacheObj
			dummyCacheObj.Body = nil
			cacheObjSer, serErr := msgpack.Marshal(dummyCacheObj)
			if serErr != nil {
				counters.SerErr.Add(1)
				log.Println(serErr)
				cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SERERR", "", stats)
				continue
			}
			redisErr := cacheConn().Set(ctx, getKey(req, ""), string(cacheObjSer), time.Duration(maxAge)*time.Second).Err()
			if redisErr != nil {
				counters.CacheErr.Add(1)
				log.Println(redisErr)
				cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_CACHEERR", "", stats)
				continue
			}
		}

		cacheObjSer, serErr := msgpack.Marshal(cacheObj)
		if serErr != nil {
			counters.SerErr.Add(1)
			log.Println(serErr)
			cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SERERR", "", stats)
			continue
		}
		// store response with body
		redisErr := redisConn.Set(ctx, getKey(req, cacheObj.Headers.Get("Vary")), string(cacheObjSer), time.Duration(maxAge)*time.Second).Err()
		if redisErr != nil {
			counters.CacheErr.Add(1)
			log.Println(redisErr)
			cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_CACHEERR", "", stats)
			continue
		}
		counters.Sets.Add(1)

	}
}

func updateTtlWorker(redisConn *redis.Client) {
	workerId := int64(-1)
	for {
		if config.Workers.PrioritiesEnabled && workerId >= 0 {
			prioworkers.WorkEnd(workerId)
		}
		msg := <-updateChan
		if config.Workers.PrioritiesEnabled {
			workerId = prioworkers.WorkStart(updateWPrio)
		}
		req := &msg.req
		respHeaders := &msg.cacheObj.Headers
		if respHeaders.Get("Date") == "" {
			continue
		}
		cacheControl := splitHeader(respHeaders, "Cache-Control", ",")
		maxAge, err := getMaxAge(cacheControl, respHeaders, config.StandardViolations.EnableStandardViolations)
		if err != nil {
			continue
		}
		cacheKey := getKey(req, "")
		redisErr := redisConn.Expire(ctx, cacheKey, time.Duration(maxAge)*time.Second).Err()
		if redisErr != nil {
			counters.CacheErr.Add(1)
			log.Println("Could not update TTL for key ", cacheKey, " to ", time.Duration(maxAge)*time.Second, " seconds")
		}
		if respHeaders.Get("Vary") != "" {
			cacheKey := getKey(req, respHeaders.Get("Vary"))
			redisErr := redisConn.Expire(ctx, cacheKey, time.Duration(maxAge)*time.Second).Err()
			if redisErr != nil {
				counters.CacheErr.Add(1)
				log.Println("Could not update TTL for key ", cacheKey, " to ", time.Duration(maxAge)*time.Second, " seconds")
			}
		}
		counters.Updates.Add(1)
	}
}

func revalidateWorker() {
	workerId := int64(-1)
	for {
		if config.Workers.PrioritiesEnabled && workerId >= 0 {
			prioworkers.WorkEnd(workerId)
		}
		req := <-revalidateChan
		if config.Workers.PrioritiesEnabled {
			workerId = prioworkers.WorkStart(revalidateWPrio)
		}
		counters.Revalidations.Add(1)
		httpClient := &http.Client{}
		httpClient.Timeout = 30 * time.Second
		req.RequestURI = "" // it is required by library that RequestURI is not set
		resp, err := httpClient.Do(&req)
		if err != nil {
			log.Println("Error making HTTP request:", err)
			continue
		}
		set(&req, resp, stopWatches{}, srcRevalidate)
		resp.Body.Close()
	}
}

func sendHeaders(w http.ResponseWriter, statusCode int, respHeaders http.Header, cacheStatus string, cacheKey string, req *http.Request, stats stopWatches) {
	for k, v := range respHeaders {
		w.Header().Set(k, strings.Join(v, " "))
	}
	w.WriteHeader(statusCode)
	if req != nil {
		cacheLog(req, statusCode, respHeaders, cacheStatus, cacheKey, stats)
	}
	updateChan <- cacheReqResp{cacheObj: cacheObjType{Headers: respHeaders.Clone()}, req: *req}
}

func getMaxAge(cacheControl map[string]string, respHeaders *http.Header, withViolations bool) (int, error) {
	if MapHasKey(cacheControl, "immutable") {
		return validateMaxAge(config.Cache.MaxAge, respHeaders)
	}
	// if standard violation is enabled use this algorithm
	if withViolations {
		maxAge := max(getCacheControlTtl(cacheControl, respHeaders), getLastModifiedTtl(respHeaders))
		if config.StandardViolations.OverrideCacheControl {
			return validateMaxAge(max(maxAge, min(getLastModifiedTtl(respHeaders)), config.StandardViolations.OverrideCacheControlMaxAge), respHeaders)
		}
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

func validateMaxAge(maxAge int, respHeaders *http.Header) (int, error) {
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

func getCacheControlTtl(cacheControl map[string]string, headers *http.Header) int {
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

func getExpiresTtl(headers *http.Header) int {
	if expires := headers.Get("Expires"); expires != "" {
		tExpires, err := time.Parse(time.RFC1123, expires)
		if err == nil {
			return int(tExpires.Sub(time.Now()).Seconds())
		}
	}
	return 0
}

func getLastModifiedTtl(headers *http.Header) int {
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

func getAge(headers *http.Header) int {
	age, ageErr := strconv.Atoi(headers.Get("Age"))
	if ageErr == nil {
		return age
	}
	return 0
}

func getResponseAge(headers *http.Header) int {
	tDate, err := time.Parse(time.RFC1123, headers.Get("Date"))
	if err != nil {
		return 0
	}
	return int(time.Since(tDate).Seconds())
}
