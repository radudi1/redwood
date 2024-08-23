package responseCache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andybalholm/redwood/responseCache/storage"
	"github.com/radudi1/prioworkers"
	"github.com/radudi1/stopwatch"
)

type countersT struct {
	Hits             atomic.Uint64
	HitBytes         atomic.Uint64
	Misses           atomic.Uint64
	MissBytes        atomic.Uint64
	Uncacheable      atomic.Uint64
	UncacheableBytes atomic.Uint64
	Sets             atomic.Uint64
	Updates          atomic.Uint64
	Revalidations    atomic.Uint64
	EncodeErr        atomic.Uint64
	ReadErr          atomic.Uint64
	WriteErr         atomic.Uint64
}

type stopWatches struct {
	getSw, setSw stopwatch.StopWatch
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
	counters countersT

	revalidateReqs      map[string]struct{}
	revalidateReqsMutex sync.Mutex

	httpClient *http.Client

	ErrInvalidMaxAge     = errors.New("invalid maxAge value")
	ErrComputeMaxAge     = errors.New("cannot compute maxAge")
	ErrComputeExpireTime = errors.New("cannot compute expire time")
)

func Get(w http.ResponseWriter, req *http.Request) (found bool, stats *stopWatches) {

	if !config.Cache.Enabled {
		return false, nil
	}

	// init
	stats = &stopWatches{}
	stats.getSw = stopwatch.Start()
	defer stats.getSw.Stop()

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	// we ONLY cache some requests
	if req.Method != "GET" && req.Method != "HEAD" {
		return
	}
	cacheControl := splitHeader(req.Header, "Cache-Control", ",")
	if MapHasAnyKey(cacheControl, uncacheableCacheControlDirectives[:]) || cacheControl["max-age"] == "0" {
		return
	}
	if req.Header.Get("Expect") != "" || req.Header.Get("Range") != "" {
		return
	}

	// fetch object from cache
	cacheObj, cacheObjFound := fetchFromCache(req, "statusCode", "metadata", "headers")
	if cacheObj != nil {
		defer cacheObj.Close()
	}
	if !cacheObjFound {
		return false, stats
	}

	// if the client just wants validation we just check if object is present and valid
	if reqETag := req.Header.Get("If-None-Match"); reqETag != "" {
		reqETags := splitHeader(req.Header, "ETag", ",")
		if MapHasKey(reqETags, cacheObj.Headers.Get("ETag")) {
			return sendResponse(req, cacheObj, http.StatusNotModified, w, stats)
		}
	}
	if req.Header.Get("If-Modified-Since") != "" {
		modifiedSinceTime, err := HeaderValToTime(req.Header, "If-Modified-Since")
		if err == nil {
			lastModifiedTime, err := HeaderValToTime(cacheObj.Headers, "Last-Modified")
			if err == nil && (lastModifiedTime.Equal(modifiedSinceTime) || lastModifiedTime.Before(modifiedSinceTime)) {
				return sendResponse(req, cacheObj, http.StatusNotModified, w, stats)
			}
		}
	}

	// if we get here we send full response to client
	return sendResponse(req, cacheObj, cacheObj.StatusCode, w, stats)

}

func fetchFromCache(req *http.Request, fields ...string) (cacheObj *CacheObject, foundAndValid bool) {
	var err error
	sentToRevalidation := false
	cacheObj, err = cache.Get(req, fields...)
	if err != nil {
		foundAndValid = false
		return
	}
	// check sanity
	if cacheObj == nil {
		foundAndValid = false
		return
	}
	if cacheObj.Headers == nil {
		log.Println("Nil cache headers received for", req.Host+req.RequestURI)
		foundAndValid = false
		return
	}
	// check staleness
	if cacheObj.IsStale() { // object is stale
		if !config.StandardViolations.EnableStandardViolations || !config.StandardViolations.ServeStale { // serve stale is not enabled - we obey standards
			if cacheObj.Metadata.RevalidateDeadline.Before(time.Now()) { // we exceeded deadline until we could serve stale and revalidate in background according to standards
				foundAndValid = false
				return
			}
			// object is stale but it's within revalidation deadline
			sentToRevalidation = true
			go revalidateWorker(req.Clone(context.Background()), cacheObj)
		} else { // serve stale is enabled - we serve and revalidate
			sentToRevalidation = true
			go revalidateWorker(req.Clone(context.Background()), cacheObj)
		}
	}
	// if we get here we can serve the object and we also update ttl
	foundAndValid = true
	if !sentToRevalidation {
		go updateTtlWorker(cacheObj.Metadata, *req.Clone(context.Background()), cacheObj.Headers)
	}
	return
}

func sendResponse(req *http.Request, cacheObj *CacheObject, toClientStatusCode int, w http.ResponseWriter, stats *stopWatches) (ok bool, s *stopWatches) {

	// update stats
	counters.Hits.Add(1)
	counters.HitBytes.Add(uint64(len(cacheObj.Headers) + len(cacheObj.Body)))

	// set log status (will be modified if necessary) and hit source
	backendName := "REDIS"
	if (cacheObj.Backends & storage.RamBackend) != 0 {
		backendName = "RAM"
	}
	logStatus := "HIT"

	// log on exit
	defer func() {
		cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, logStatus+"_"+backendName, cacheObj.CacheKey, stats)
	}()

	// if not modified there's nothing there to send except status code
	if toClientStatusCode == http.StatusNotModified {
		logStatus = "NOTMODIF"
		w.WriteHeader(toClientStatusCode)
		return true, stats
	}

	// if it's a HEAD request or has certain response status codes we don't send the body  - RFCs 2616 7230
	// https://stackoverflow.com/questions/78182848/does-http-differentiate-between-an-empty-body-and-no-body
	if req.Method == "HEAD" || int(toClientStatusCode/100) == 1 || toClientStatusCode == 204 || toClientStatusCode == 304 {
		// send headers
		sendHeaders(cacheObj, w, stats, backendName)
		w.WriteHeader(toClientStatusCode)
		return true, stats
	}

	// check if body needs reencoding only if there is one
	if cacheObj.Metadata.BodySize > 0 {

		// check if body needs reencoding (compression algo not supported by client)
		respEncoding := cacheObj.Headers.Get("Content-Encoding")
		if respEncoding != "" {
			acceptEncoding := splitHeader(req.Header, "Accept-Encoding", ",")
			if !MapHasKey(acceptEncoding, respEncoding) {
				// refetch cached object with body
				var err error
				cacheObj, err = cache.Get(req)
				if err != nil {
					log.Println("Error while fetching object from cache for re-encoding")
					return false, stats
				}
				// re-encode
				var encodedBuf bytes.Buffer
				encoding, reEncodeErr := reEncode(&encodedBuf, []byte(cacheObj.Body), respEncoding, acceptEncoding)
				if reEncodeErr != nil {
					counters.EncodeErr.Add(1)
					log.Println("Could not reeencode cached response: ", reEncodeErr)
					return false, stats
				}
				cacheObj.Body = encodedBuf.Bytes()
				if encoding == "" {
					cacheObj.Headers.Del("Content-Encoding")
				} else {
					cacheObj.Headers.Set("Content-Encoding", encoding)
				}
				cacheObj.Headers.Set("Content-Length", strconv.Itoa(len(cacheObj.Body)))
			}
		}
	}

	// send headers
	sendHeaders(cacheObj, w, stats, backendName)

	// send body only if there is one
	if cacheObj.Metadata.BodySize > 0 {
		if toClientStatusCode != http.StatusOK {
			w.WriteHeader(toClientStatusCode)
		}
		// send body directly if we already have it (possibly due to reencoding)
		// otherwise use writer for improved efficiency and memory consumption
		if len(cacheObj.Body) > 0 {
			w.Write(cacheObj.Body)
			return true, stats
		} else {
			writeErr := cache.WriteBodyToClient(cacheObj, w)
			if writeErr != nil {
				counters.WriteErr.Add(1)
				log.Println(writeErr, "for", req.Host+req.RequestURI, "from cache backends", cacheObj.Backends, cacheObj.MetadataCacheKey, cacheObj.CacheKey)
				return true, stats
			}
		}
	} else {
		w.WriteHeader(toClientStatusCode)
	}

	return true, stats
}

func sendHeaders(cacheObj *CacheObject, w http.ResponseWriter, stats *stopWatches, backendName string) {
	// copy response headers
	for headerName, valSlice := range cacheObj.Headers {
		for _, headerValue := range valSlice {
			w.Header().Add(headerName, headerValue)
		}
	}

	// set X-Cache headers
	if config.Cache.XCacheHeaders != 0 {
		xCachePrefix := "X-Cache"
		if config.Cache.XCacheHeaders&1 > 0 {
			w.Header().Add(xCachePrefix, hostname+":HIT")
		}
		xCachePrefix += "-" + hostname + "-"
		if config.Cache.XCacheHeaders&2 > 0 {
			w.Header().Add(xCachePrefix+"backend", backendName)
		}
		if config.Cache.XCacheHeaders&4 > 0 {
			w.Header().Add(xCachePrefix+"keys", cacheObj.MetadataCacheKey+" "+cacheObj.CacheKey)
		}
		if config.Cache.XCacheHeaders&8 > 0 {
			w.Header().Add(xCachePrefix+"updated", cacheObj.Metadata.Updated.Format(time.RFC1123))
			w.Header().Add(xCachePrefix+"stale", cacheObj.Metadata.Stale.Format(time.RFC1123))
			w.Header().Add(xCachePrefix+"revalidate", cacheObj.Metadata.RevalidateDeadline.Format(time.RFC1123))
			w.Header().Add(xCachePrefix+"expires", cacheObj.Metadata.Expires.Format(time.RFC1123))
		}
		if config.Cache.XCacheHeaders&16 > 0 {
			w.Header().Add(xCachePrefix+"bodySize", strconv.FormatInt(int64(cacheObj.Metadata.BodySize), 10))
			w.Header().Add(xCachePrefix+"bodyChunkLen", strconv.FormatInt(int64(cacheObj.Metadata.BodyChunkLen), 10))
		}

		if config.Cache.XCacheHeaders&128 > 0 {
			w.Header().Add(xCachePrefix+"timings", stats.getSw.GetDurationSinceStart().String())
		}
	}

}

func Set(req *http.Request, resp *http.Response, stats *stopWatches) {
	if !config.Cache.Enabled {
		return
	}
	set(req, resp, stats, srcClient)
}

func set(req *http.Request, resp *http.Response, stats *stopWatches, reqSrc int) {

	var logStatus string
	switch reqSrc {
	case srcClient:
		defer func() {
			cacheLog(req, resp.StatusCode, resp.Header, logStatus, getCacheKey(req, resp.Header.Get("Vary")), stats)
		}()
	case srcRevalidate:
		defer func() {
			revalidateLog(req, resp.StatusCode, resp.Header, logStatus, getCacheKey(req, resp.Header.Get("Vary")), stats)
		}()
	default:
		panic("Invalid request source received for responsecache set")
	}

	stats.setSw = stopwatch.Start()
	defer stats.setSw.Stop()

	var body bytes.Buffer
	var err error

	// create metadata
	metadata := storage.StorageMetadata{}

	// update counters when function returns
	defer (func() {
		if logStatus == "MISS" {
			counters.Misses.Add(1)
			counters.MissBytes.Add(uint64(body.Len()))
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
		cache.storage.GetRedisCompatConn().SAdd(context.Background(), noBumpDomainsKey, req.Host)
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
	if req.Header.Get("Authorization") != "" || resp.Header.Get("WWW-Authenticate") != "" {
		logStatus = "UC_AUTH"
		return
	}
	if strings.TrimSpace(req.Header.Get("Vary")) == "*" {
		logStatus = "UC_VARY"
		return
	}
	if req.Header.Get("Expect") != "" {
		logStatus = "UC_EXPECT"
		return
	}
	cacheControl := splitHeader(resp.Header, "Cache-Control", ",")
	// if response sets cookie it could be a response specific to a single client and there's no way to store separately from other clients
	// but if cache-control says it's public or allows shared storage we take its' word for it
	// also if there's no data transmited in request url or body we can assume that this request is not specific to a certain client
	// (auth headers are already handled; cookies and x-api-key headers are already handled through cache key generation)
	if resp.Header.Get("Set-Cookie") != "" && !MapHasKey(cacheControl, "public") && !MapHasKey(cacheControl, "s-maxage") &&
		!(req.URL.RawQuery == "" || req.ContentLength == 0) {
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
			req.Header.Get("Cookie") != "" || resp.Header.Get("Set-Cookie") != "" {
			logStatus = "UC_CACHECTRL"
			return
		}
	}
	sizeLimit := int(math.Min(float64(config.Cache.MaxBodySize), 512*1024*1024)) // redis max object size is 512 MB
	if resp.ContentLength > int64(sizeLimit) {
		logStatus = "UC_TOOBIG"
		return
	}

	// set updated time
	metadata.Updated = time.Now()

	// set other metadata times
	if setMetadataTimes(&metadata, resp.Header) != nil {
		logStatus = "UC_STALE"
		return
	}
	if metadata.Expires.Unix() < time.Now().Unix()+int64(config.Cache.MinTtl) {
		logStatus = "UC_LOWTTL"
		return
	}

	// fetch response body
	lr := &io.LimitedReader{
		R: resp.Body,
		N: int64(sizeLimit),
	}
	_, err = BufferedCopy(&body, lr)
	// body, err = io.ReadAll(lr)
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
		resp.Body = io.NopCloser(io.MultiReader(strings.NewReader(body.String()), resp.Body))
		logStatus = "UC_TOOBIG"
		return
	}
	resp.Body = io.NopCloser(strings.NewReader(body.String()))
	if body.Len() >= sizeLimit {
		logStatus = "UC_TOOBIG"
		return
	}

	// send to cache
	metadata.Vary = varyHeadersAsStr(resp.Header)
	go setWorker(resp.StatusCode, metadata, req.Clone(context.Background()), resp.Header.Clone(), body.Bytes())

	logStatus = "MISS"

}

func setWorker(statusCode int, metadata storage.StorageMetadata, req *http.Request, respHeaders http.Header, body []byte) {
	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}
	respHeaders.Del("Age") // remove age because it will be incorrect when presented to client
	if err := cache.Set(statusCode, metadata, req, respHeaders, body); err != nil && err != storage.ErrTtlTooSmall && err != storage.ErrTooBig {
		log.Println(err)
		log.Println("Error setting cache object for ", req.RequestURI)
		return
	}
	counters.Sets.Add(1)
}

func updateTtlWorker(cachedMetadata storage.StorageMetadata, req http.Request, respHeaders http.Header) {
	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(updateWPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	newMetadata := cachedMetadata
	if setMetadataTimes(&newMetadata, respHeaders) != nil {
		return
	}

	// if we can't extend expiration time at all there's no point to update
	if cachedMetadata.Expires.Equal(newMetadata.Expires) {
		return
	}
	// if we can't extend expiration time with at least ExpirePercentUpdate percent then we don't update
	cachedExpireSeconds := cachedMetadata.Expires.Sub(cachedMetadata.Updated).Seconds()
	newExpireSeconds := newMetadata.Expires.Sub(cachedMetadata.Updated).Seconds()
	if float64((newExpireSeconds-cachedExpireSeconds))/cachedExpireSeconds < (float64(config.Cache.ExpirePercentUpdate) / 100) {
		return
	}

	cache.Update(&req, cachedMetadata)

	counters.Updates.Add(1)
}

func revalidateWorker(req *http.Request, cacheObj *CacheObject) {

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(revalidateWPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	// timers
	stats := &stopWatches{}
	stats.getSw = stopwatch.Start()
	defer stats.getSw.Stop()

	// if this is request is currently revalidating (by another goroutine) we skip it
	cacheKey := getCacheKey(req, cacheObj.Headers.Get("Vary"))
	revalidateReqsMutex.Lock()
	if MapHasKey(revalidateReqs, cacheKey) {
		revalidateReqsMutex.Unlock()
		return
	}

	// add current request to currently revalidating list
	revalidateReqs[cacheKey] = struct{}{}
	revalidateReqsMutex.Unlock()
	defer func() {
		// remove request from currently revalidating list
		revalidateReqsMutex.Lock()
		delete(revalidateReqs, cacheKey)
		revalidateReqsMutex.Unlock()
	}()

	// update stats
	counters.Revalidations.Add(1)

	// conditional validation if possible
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	if cacheObj.Headers.Get("ETag") != "" {
		req.Header.Add("If-None-Match", strings.Join(cacheObj.Headers.Values("ETag"), ", "))
	} else if cacheObj.Headers.Get("Date") != "" {
		req.Header.Add("If-Modified-Since", cacheObj.Headers.Get("Date"))
	}

	// do request
	if req.ContentLength == 0 {
		req.Body.Close()
		req.Body = nil
	}
	reqURI := req.RequestURI // URI will have to be restored before caching because it's used to compute cache key
	req.RequestURI = ""      // it is required by library that RequestURI is not set
	resp, err := httpClient.Do(req)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		log.Println("Error making HTTP (revalidate) request:", err)
		return
	}

	// process response
	req.RequestURI = reqURI // restore URI before caching because it's used to compute cache key
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	if resp.StatusCode == http.StatusNotModified {
		// update cache metadata
		metadata := cacheObj.Metadata
		metadata.Updated = time.Now()
		if setMetadataTimes(&metadata, cacheObj.Headers) != nil {
			return
		}
		if err := cache.Update(req, metadata); err != nil {
			log.Println("Error while updating cache (revalidate):", err)
		}
		revalidateLog(req, resp.StatusCode, resp.Header, "NOTMODIF", cacheKey, stats)
	} else {
		// set new response
		set(req, resp, stats, srcRevalidate)
	}

}

func setMetadataTimes(metadata *storage.StorageMetadata, respHeaders http.Header) error {
	cacheControl := splitHeader(respHeaders, "cache-control", ",")

	// set expire time
	// object can expire at stale time if we follow standards or at a later time if standard violations are enabled
	ttl, err := getTtl(cacheControl, respHeaders, config.StandardViolations.EnableStandardViolations)
	if err != nil || ttl <= 0 {
		return ErrComputeExpireTime
	}
	metadata.Expires = NowPlusSeconds(ttl)

	// set stale time
	// object becomes stale when headers tell us (according to standards)
	ttl, _ = getTtl(cacheControl, respHeaders, false)
	metadata.Stale = NowPlusSeconds(ttl)

	// set revalidate deadline to be FreshPercentRevalidate percent of freshness time
	// if we have "stale-while-revalidate" it will be used only if it's sooner
	freshDuration := metadata.Stale.Sub(metadata.Updated)
	revalidateAfterSeconds := freshDuration.Seconds() * (float64(config.Cache.FreshPercentRevalidate) / 100)
	metadata.RevalidateDeadline = NowPlusSeconds(int(revalidateAfterSeconds))
	if MapHasKey(cacheControl, "stale-while-revalidate") {
		revalidateWindow, ok := MapElemToI(cacheControl, "stale-while-revalidate")
		if ok {
			revalidateTime := metadata.Stale.Add(time.Duration(revalidateWindow) * time.Second)
			if revalidateTime.Before(metadata.RevalidateDeadline) {
				metadata.RevalidateDeadline = revalidateTime
			}
		}
	}

	// make sure cache ttl (expires) is at least revalidateDeadline
	if metadata.Expires.Before(metadata.RevalidateDeadline) {
		metadata.Expires = metadata.RevalidateDeadline
	}

	return nil
}

func getTtl(cacheControl map[string]string, respHeaders http.Header, withViolations bool) (int, error) {
	if MapHasKey(cacheControl, "immutable") {
		return validateTtl(config.Cache.MaxTtl, respHeaders, withViolations)
	}
	// if standard violation is enabled use this algorithm
	if withViolations {
		maxAge := getCacheControlTtl(cacheControl, withViolations)
		// if OverrideCacheControl is enabled we use this algo
		if config.StandardViolations.OverrideCacheControl {
			maxAge = max(maxAge, min(getLastModifiedTtl(respHeaders), config.StandardViolations.OverrideCacheControlMaxAge))
			if maxAge > 0 {
				return validateTtl(maxAge, respHeaders, withViolations)
			}
			// no cache-control and no last-modified
			if age := getAge(respHeaders); age > 0 { // if age header is present we compute ttl based on that
				return validateTtl(age/config.Cache.AgeDivisor, respHeaders, withViolations)
			}
			// we resort to expire unless it is overriden
			if !config.StandardViolations.OverrideExpire {
				if maxAge = getExpiresTtl(respHeaders); maxAge > 0 {
					return validateTtl(maxAge, respHeaders, withViolations)
				}
			}
			// there are no headers present that we can't compute ttl on so we return the minimum default
			return config.StandardViolations.DefaultAge, nil
		}
		// if OverrideCacheControl is disabled we use this algo
		if maxAge > 0 {
			return validateTtl(maxAge, respHeaders, withViolations)
		}
		if !config.StandardViolations.OverrideExpire {
			if maxAge = getExpiresTtl(respHeaders); maxAge > 0 {
				return validateTtl(maxAge, respHeaders, withViolations)
			}
		}
		return validateTtl(getLastModifiedTtl(respHeaders), respHeaders, withViolations)
	} else { // standard violations are not enabled
		// return from cache-control if possible
		if maxAge := getCacheControlTtl(cacheControl, withViolations); maxAge > 0 {
			return validateTtl(maxAge, respHeaders, withViolations)
		}
		if maxAge := getExpiresTtl(respHeaders); maxAge > 0 {
			return validateTtl(maxAge, respHeaders, withViolations)
		}
		// otherwise return from last-modified
		if maxAge := getLastModifiedTtl(respHeaders); maxAge > 0 {
			return validateTtl(maxAge, respHeaders, withViolations)
		}
	}
	return 0, ErrComputeMaxAge
}

func validateTtl(maxAge int, respHeaders http.Header, withViolations bool) (int, error) {
	// if violations are enabled we already apply different heuristics to determine ttl - therefore we ignore age here
	// otherwise age must be substracted
	if !withViolations {
		maxAge = maxAge - getAge(respHeaders)
	}
	// if we have a restricted mime prefix we cap maxAge accordingly
	for _, v := range config.Cache.RestrictedMimePrefixes {
		if strings.HasPrefix(respHeaders.Get("Content-Type"), v) {
			maxAge = min(maxAge, config.Cache.RestrictedMaxAge)
			break
		}
	}
	// make sure maxAge is within range
	if maxAge < 1 {
		return maxAge, ErrInvalidMaxAge
	}
	// cap maxAge to config setting
	maxAge = min(maxAge, config.Cache.MaxTtl)
	return maxAge, nil
}

func getCacheControlTtl(cacheControl map[string]string, withViolations bool) int {
	smaxAge, _ := MapElemToI(cacheControl, "s-maxage")
	// if we have s-maxage and s-maxage overriding is not enabled we return it
	if smaxAge > 0 && (!withViolations || !config.StandardViolations.OverrideSMaxAge) {
		return smaxAge
	}
	// s-maxage is not present or it's overriden so we fetch maxage and return maximum of the 2 values minus age
	maxAge, _ := MapElemToI(cacheControl, "max-age")
	ttl := max(maxAge, smaxAge)
	if config.StandardViolations.EnableStandardViolations && ttl < 0 { // this is NOT according to standard !!! but some servers send responses which are extensively stale - if response is good for them it should be good for us
		return ttl
	}
	return ttl
}

func getExpiresTtl(headers http.Header) int {
	if expires := headers.Get("Expires"); expires != "" {
		tExpires, err := time.Parse(time.RFC1123, expires)
		if err == nil {
			return int(time.Until(tExpires).Seconds())
		}
	}
	return 0
}

func getLastModifiedTtl(headers http.Header) int {
	if config.Cache.AgeDivisor < 1 { // if we don't have a valid AgeDivisor we can't compute ttl based on last-modified
		return 0
	}
	lastModifiedTime, err := HeaderValToTime(headers, "Last-Modified")
	if err != nil {
		return 0
	}
	return int(float64(time.Since(lastModifiedTime).Seconds()) / float64(config.Cache.AgeDivisor))
}

func getAge(headers http.Header) int {
	tDate, dateErr := time.Parse(time.RFC1123, headers.Get("Date"))
	ageSeconds, ageErr := strconv.Atoi(headers.Get("Age"))
	if dateErr != nil {
		return ageSeconds
	}
	dateSeconds := int(time.Since(tDate).Seconds())
	if ageErr != nil {
		return dateSeconds
	}
	return max(dateSeconds, ageSeconds)
}

// Returns Vary response header(s) as string
func varyHeadersAsStr(respHeaders http.Header) string {
	return strings.Join(respHeaders.Values("Vary"), ", ")
}
