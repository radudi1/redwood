package responseCache

import (
	"bytes"
	"context"
	"errors"
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

	// if the client just wants validation we just check if object is present and valid
	if reqETag := req.Header.Get("If-None-Match"); reqETag != "" {
		cacheObj, cacheObjFound := fetchFromCache(req, []string{"statusCode", "metadata", "headers"})
		if cacheObjFound {
			reqETags := splitHeader(req.Header, "ETag", ",")
			if MapHasKey(reqETags, cacheObj.headers.Get("ETag")) {
				return sendResponse(req, cacheObj, http.StatusNotModified, w, stats)
			}
		}
	}
	if req.Header.Get("If-Modified-Since") != "" {
		cacheObj, cacheObjFound := fetchFromCache(req, []string{"statusCode", "metadata", "headers"})
		if cacheObjFound {
			modifiedSinceTime, err := HeaderValToTime(req.Header, "If-Modified-Since")
			if err == nil {
				lastModifiedTime, err := HeaderValToTime(cacheObj.headers, "Last-Modified")
				if err == nil && (lastModifiedTime.Equal(modifiedSinceTime) || lastModifiedTime.Before(modifiedSinceTime)) {
					return sendResponse(req, cacheObj, http.StatusNotModified, w, stats)
				}
			}
		}
	}

	// if it's a HEAD request or has certain response status codes we don't send the body  - RFCs 2616 7230
	// https://stackoverflow.com/questions/78182848/does-http-differentiate-between-an-empty-body-and-no-body
	if req.Method == "HEAD" {
		cacheObj, cacheObjFound := fetchFromCache(req, []string{"statusCode", "metadata", "headers"})
		if cacheObjFound {
			return sendResponse(req, cacheObj, cacheObj.statusCode, w, stats)
		}
	}

	// if we get here we need to fetch full object from cache
	cacheObj, cacheObjFound := fetchFromCache(req, nil)
	if !cacheObjFound {
		return false, stats
	}
	cacheObj.headers.Add("X-Cache", "HIT")
	return sendResponse(req, cacheObj, cacheObj.statusCode, w, stats)

}

func fetchFromCache(req *http.Request, fields []string) (cacheObj *cacheObjType, foundAndValid bool) {
	var err error
	cacheObj, err = cacheGet(req, fields)
	if err != nil {
		foundAndValid = false
		return
	}
	if cacheObj.IsStale() { // object is stale
		if !config.StandardViolations.EnableStandardViolations || !config.StandardViolations.ServeStale { // serve stale is not enabled - we obey standards
			if cacheObj.metadata.RevalidateDeadline.Before(time.Now()) { // we exceeded deadline until we could serve stale and revalidate in background according to standards
				foundAndValid = false
				return
			}
			// object is stale but it's within revalidation deadline
			go revalidateWorker(req.Clone(context.Background()), cacheObj.headers.Clone())
		} else { // serve stale is enabled - we serve and revalidate
			go revalidateWorker(req.Clone(context.Background()), cacheObj.headers.Clone())
		}
	}
	// if we get here we can serve the object and we also update ttl
	foundAndValid = true
	go updateTtlWorker(cacheObj.metadata, *req.Clone(context.Background()), cacheObj.headers)
	return
}

func sendResponse(req *http.Request, cacheObj *cacheObjType, toClientStatusCode int, w http.ResponseWriter, stats *stopWatches) (ok bool, s *stopWatches) {

	// update stats
	counters.Hits.Add(1)
	counters.HitBytes.Add(uint64(len(cacheObj.headers) + len(cacheObj.body)))

	// set log status (will be modified if necessary)
	logStatus := "HIT"

	// log on exit
	defer func() {
		cacheLog(req, cacheObj.statusCode, cacheObj.headers, logStatus, cacheObj.cacheKey, stats)
	}()

	// if not modified there's nothing there to send except status code
	if toClientStatusCode == http.StatusNotModified {
		logStatus = "NOTMODIF"
		w.WriteHeader(toClientStatusCode)
		return true, stats
	}

	// copy response headers
	for headerName, valSlice := range cacheObj.headers {
		for _, headerValue := range valSlice {
			w.Header().Add(headerName, headerValue)
		}
	}

	// if it's a HEAD request or has certain response status codes we don't send the body  - RFCs 2616 7230
	// https://stackoverflow.com/questions/78182848/does-http-differentiate-between-an-empty-body-and-no-body
	if req.Method == "HEAD" || int(toClientStatusCode/100) == 1 || toClientStatusCode == 204 || toClientStatusCode == 304 {
		// send headers
		w.WriteHeader(toClientStatusCode)
		return true, stats
	}

	// check if body needs reencoding (compression algo not supported by client)
	respEncoding := cacheObj.headers.Get("Content-Encoding")
	if respEncoding != "" {
		acceptEncoding := splitHeader(req.Header, "Accept-Encoding", ",")
		if !MapHasKey(acceptEncoding, respEncoding) {
			var encodedBuf bytes.Buffer
			encoding, reEncodeErr := reEncode(&encodedBuf, []byte(cacheObj.body), respEncoding, acceptEncoding)
			if reEncodeErr != nil {
				counters.EncodeErr.Add(1)
				log.Println("Could not reeencode cached response: ", reEncodeErr)
				return false, stats
			}
			cacheObj.body = encodedBuf.String()
			if encoding == "" {
				cacheObj.headers.Del("Content-Encoding")
			} else {
				cacheObj.headers.Set("Content-Encoding", encoding)
			}
			cacheObj.headers.Set("Content-Length", fmt.Sprint(len(cacheObj.body)))
		}
	}

	// send headers
	w.WriteHeader(toClientStatusCode)

	// send response body
	n, writeErr := w.Write([]byte(cacheObj.body))
	if writeErr != nil {
		counters.WriteErr.Add(1)
		log.Println(writeErr)
		return true, stats
	}
	if n != len(cacheObj.body) {
		counters.WriteErr.Add(1)
		log.Println("Written ", n, " bytes to client instead of ", len(cacheObj.body), "!!!")
	}

	return true, stats
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

	var body []byte
	var err error

	// create metadata
	metadata := metadata{}

	// update counters when function returns
	defer (func() {
		if logStatus == "MISS" {
			counters.Misses.Add(1)
			counters.MissBytes.Add(uint64(len(body)))
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

	// set updated time
	metadata.Updated = time.Now()

	// set other metadata times
	if metadata.setTimes(resp.Header) != nil {
		logStatus = "UC_STALE"
		return
	}

	// fetch response body
	lr := &io.LimitedReader{
		R: resp.Body,
		N: int64(sizeLimit),
	}
	body, err = io.ReadAll(lr)
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

	// send to cache
	metadata.Vary = varyHeadersAsStr(resp.Header)
	go setWorker(resp.StatusCode, metadata, req.Clone(context.Background()), resp.Header.Clone(), string(body))

	logStatus = "MISS"

}

func setWorker(statusCode int, metadata metadata, req *http.Request, respHeaders http.Header, body string) {
	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}
	if err := cacheSet(statusCode, metadata, req, respHeaders, body); err != nil {
		log.Println("Error setting cache object for ", req.RequestURI)
	}
	counters.Sets.Add(1)
}

func updateTtlWorker(cachedMetadata metadata, req http.Request, respHeaders http.Header) {
	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(updateWPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	cachedMetadata.setTimes(respHeaders)

	cacheUpdate(&req, cachedMetadata)

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
		metadata := metadata{
			Updated: time.Now(),
			Vary:    varyHeadersAsStr(resp.Header),
		}
		go updateTtlWorker(metadata, *req.Clone(context.Background()), resp.Header)
	} else {
		set(req, resp, &stopWatches{}, srcRevalidate)
	}
	resp.Body.Close()
	// remove request from currently revalidating list
	revalidateReqsMutex.Lock()
	delete(revalidateReqs, cacheKey)
	revalidateReqsMutex.Unlock()

}

func (metadata *metadata) setTimes(respHeaders http.Header) error {
	cacheControl := splitHeader(respHeaders, "cache-control", ",")

	// set stale time
	// object becomes stale when headers tell us (according to standards)
	maxAge, err := getMaxAge(cacheControl, respHeaders, false)
	if err != nil || maxAge <= 0 {
		return errors.New("cannot compute stale time")
	}
	metadata.Stale = NowPlusSeconds(maxAge)

	// set expire time
	// object can expire at stale time if we follow standards or at a later time if standard violations are enabled
	maxAge, err = getMaxAge(cacheControl, respHeaders, config.StandardViolations.EnableStandardViolations)
	if err != nil || maxAge <= 0 {
		return errors.New("cannot compute expire time")
	}
	metadata.Expires = NowPlusSeconds(maxAge)

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

	return nil
}

func getMaxAge(cacheControl map[string]string, respHeaders http.Header, withViolations bool) (int, error) {
	if MapHasKey(cacheControl, "immutable") {
		return validateMaxAge(config.Cache.MaxAge, respHeaders, withViolations)
	}
	// if standard violation is enabled use this algorithm
	if withViolations {
		maxAge := getCacheControlTtl(cacheControl, respHeaders, withViolations)
		// if OverrideCacheControl is enabled we use this algo
		if config.StandardViolations.OverrideCacheControl {
			maxAge = max(maxAge, min(getLastModifiedTtl(respHeaders), config.StandardViolations.OverrideCacheControlMaxAge))
			if maxAge > 0 {
				return validateMaxAge(maxAge, respHeaders, withViolations)
			}
			// no cache-control and no last-modified
			if age := getAge(respHeaders); age > 0 { // if age header is present we compute ttl based on that
				return validateMaxAge(age/config.Cache.AgeDivisor, respHeaders, withViolations)
			}
			// we resort to expire unless it is overriden
			if !config.StandardViolations.OverrideExpire {
				if maxAge = getExpiresTtl(respHeaders); maxAge > 0 {
					return validateMaxAge(maxAge, respHeaders, withViolations)
				}
			}
			// there are no headers present that we can't compute ttl on so we return the minimum default
			return validateMaxAge(config.StandardViolations.DefaultAge, respHeaders, withViolations)
		}
		// if OverrideCacheControl is disabled we use this algo
		if maxAge > 0 {
			return validateMaxAge(maxAge, respHeaders, withViolations)
		}
		if !config.StandardViolations.OverrideExpire {
			if maxAge = getExpiresTtl(respHeaders); maxAge > 0 {
				return validateMaxAge(maxAge, respHeaders, withViolations)
			}
		}
		return validateMaxAge(getLastModifiedTtl(respHeaders), respHeaders, withViolations)
	} else { // standard violations are not enabled
		// return from cache-control if possible
		if maxAge := getCacheControlTtl(cacheControl, respHeaders, withViolations); maxAge > 0 {
			return validateMaxAge(maxAge, respHeaders, withViolations)
		}
		if maxAge := getExpiresTtl(respHeaders); maxAge > 0 {
			return validateMaxAge(maxAge, respHeaders, withViolations)
		}
		// otherwise return from last-modified
		return validateMaxAge(getLastModifiedTtl(respHeaders), respHeaders, withViolations)
	}
}

func validateMaxAge(maxAge int, respHeaders http.Header, withViolations bool) (int, error) {
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
		return maxAge, fmt.Errorf("invalid maxAge value")
	}
	// cap maxAge to config setting
	maxAge = min(maxAge, config.Cache.MaxAge)
	return maxAge, nil
}

func getCacheControlTtl(cacheControl map[string]string, headers http.Header, withViolations bool) int {
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
	tDate, err := time.Parse(time.RFC1123, headers.Get("Date"))
	if err == nil {
		return int(time.Since(tDate).Seconds())
	}
	age, ageErr := strconv.Atoi(headers.Get("Age"))
	if ageErr == nil {
		return age
	}
	return 0
}
