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
	"time"

	"github.com/radudi1/prioworkers"
	"github.com/radudi1/stopwatch"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type cacheObj struct {
	Host       string
	Url        string
	StatusCode int
	Headers    http.Header
	Body       []byte
}

type cacheReqResp struct {
	cacheObj cacheObj
	req      http.Request
	maxAge   int
	stats    stopWatches
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

var setChan chan cacheReqResp
var updateChan chan cacheReqResp

func Get(w http.ResponseWriter, req *http.Request) (found bool, stats stopWatches) {

	if config.Redis.NumConn < 1 {
		return false, stopWatches{}
	}

	found = false
	statusCode := http.StatusInternalServerError
	var cacheObj cacheObj
	var cacheStatus string
	var cacheKey string
	defer (func() {
		if found && cacheStatus != "" {
			sendHeaders(w, statusCode, cacheObj.Headers, cacheStatus, cacheKey, req, stats)
		}
	})()

	stats.getSw = stopwatch.Start()
	defer stats.getSw.Stop()

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}

	cacheKey = getKey(req, "")

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
		log.Println(serErr)
		return
	}
	// if the client just wants validation we can already reply with valid because if it's cached we would have served it anyway as valid
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
			log.Println(serErr)
			return
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
	// check if body needs reencoding (compression algo not supported by client)
	respEncoding := cacheObj.Headers.Get("Content-Encoding")
	if respEncoding != "" {
		acceptEncoding := splitHeader(&req.Header, "Accept-Encoding", ",")
		if !MapHasKey(acceptEncoding, respEncoding) {
			var encodedBuf bytes.Buffer
			encoding, reEncodeErr := reEncode(&encodedBuf, &cacheObj.Body, respEncoding, acceptEncoding)
			if reEncodeErr != nil {
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
		log.Println(writeErr)
		return
	}
	if n != len(cacheObj.Body) {
		log.Println("Written ", n, " bytes to client instead of ", len(cacheObj.Body), "!!!")
	}
	return
}

func Set(req *http.Request, resp *http.Response, stats stopWatches) {

	if config.Redis.NumConn < 1 {
		return
	}

	var logStatus string
	defer func() { cacheLog(req, resp.StatusCode, resp.Header, logStatus, "", stats) }()

	stats.setSw = stopwatch.Start()
	defer stats.setSw.Stop()

	if config.Workers.PrioritiesEnabled {
		workerId := prioworkers.WorkStart(mainPrio)
		defer prioworkers.WorkEnd(workerId)
	}

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
	log.Println(resp.Header.Values("Cache-Control"))
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
	contenLengthStr := strings.TrimSpace(req.Header.Get("Content-length"))
	contentLength := int64(-1)
	if contenLengthStr != "" {
		n, err := strconv.ParseUint(contenLengthStr, 10, 63)
		if err == nil {
			contentLength = int64(n)
		}
	}
	if contentLength > int64(sizeLimit) {
		logStatus = "UC_TOOBIG"
		return
	}

	// Compute cache TTL from response
	maxAge := getMaxAge(cacheControl, &resp.Header)
	if maxAge < 1 {
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
	cacheObj := cacheObj{
		Host:       req.Host,
		Url:        req.RequestURI,
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       body,
	}

	setChan <- cacheReqResp{cacheObj: cacheObj, req: *req, maxAge: maxAge}

	logStatus = "MISS"

}

func setWorker(redisConn *redis.Client) {
	workerId := -1
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
				log.Println(serErr)
				cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SERERR", "", stats)
				continue
			}
			redisErr := cacheConn().Set(ctx, getKey(req, ""), string(cacheObjSer), time.Duration(maxAge)*time.Second).Err()
			if redisErr != nil {
				log.Println(redisErr)
				cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_CACHEERR", "", stats)
				continue
			}
		}

		cacheObjSer, serErr := msgpack.Marshal(cacheObj)
		if serErr != nil {
			log.Println(serErr)
			cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_SERERR", "", stats)
			continue
		}
		// store response with body
		redisErr := redisConn.Set(ctx, getKey(req, cacheObj.Headers.Get("Vary")), string(cacheObjSer), time.Duration(maxAge)*time.Second).Err()
		if redisErr != nil {
			log.Println(redisErr)
			cacheLog(req, cacheObj.StatusCode, cacheObj.Headers, "UC_CACHEERR", "", stats)
			continue
		}

	}
}

func updateTtlWorker(redisConn *redis.Client) {
	workerId := -1
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
			return
		}
		cacheControl := splitHeader(respHeaders, "Cache-Control", ",")
		maxAge := getMaxAge(cacheControl, respHeaders)
		if maxAge < 1 {
			return
		}
		cacheKey := getKey(req, "")
		redisErr := redisConn.Expire(ctx, cacheKey, time.Duration(maxAge)*time.Second).Err()
		if redisErr != nil {
			log.Println("Could not update TTL for key ", cacheKey, " to ", time.Duration(maxAge)*time.Second, " seconds")
		}
		if respHeaders.Get("Vary") != "" {
			cacheKey := getKey(req, respHeaders.Get("Vary"))
			redisErr := redisConn.Expire(ctx, cacheKey, time.Duration(maxAge)*time.Second).Err()
			if redisErr != nil {
				log.Println("Could not update TTL for key ", cacheKey, " to ", time.Duration(maxAge)*time.Second, " seconds")
			}
		}
	}
}

func getMaxAge(cacheControl map[string]string, respHeaders *http.Header) int {
	var maxAge int
	maxAge = 0
	if MapHasKey(cacheControl, "immutable") {
		maxAge = config.Cache.MaxAge
	} else if MapHasKey(cacheControl, "max-age") || MapHasKey(cacheControl, "s-maxage") {
		maxAge, _ = MapElemToI(cacheControl, "max-age")
		smaxAge, _ := MapElemToI(cacheControl, "s-maxage")
		maxAge = int(math.Max(float64(maxAge), float64(smaxAge)))
	} else {
		if !config.Cache.OverrideExpire {
			if expires := respHeaders.Get("Expires"); expires != "" {
				tExpires, err := time.Parse(time.RFC1123, expires)
				if err == nil {
					maxAge = int(tExpires.Sub(time.Now()).Seconds())
				}
			}
		}
		if maxAge == 0 && config.Cache.AgeDivisor != 0 {
			if lastModified := respHeaders.Get("Last-Modified"); lastModified != "" {
				tLastModified, err := time.Parse(time.RFC1123, lastModified)
				if err == nil {
					maxAge = int(float64(time.Since(tLastModified).Seconds()) / float64(config.Cache.AgeDivisor))
				}
			}
		}
	}
	age, ageErr := strconv.Atoi(respHeaders.Get("Age"))
	if ageErr == nil {
		if maxAge > age { // this is NOT according to standard !!! but some servers send responses which are extensively stale - if response is good for them it should be good for us
			maxAge = maxAge - age
		}
	}
	tDate, err := time.Parse(time.RFC1123, respHeaders.Get("Date"))
	if err == nil {
		maxAge = maxAge - int(time.Since(tDate).Seconds())
	}
	for _, v := range config.Cache.RestrictedMimePrefixes {
		if strings.HasPrefix(respHeaders.Get("Content-Type"), v) {
			maxAge = int(math.Min(float64(maxAge), float64(config.Cache.RestrictedMaxAge)))
			break
		}
	}
	maxAge = int(math.Min(float64(maxAge), float64(config.Cache.MaxAge)))
	return maxAge
}

func sendHeaders(w http.ResponseWriter, statusCode int, respHeaders http.Header, cacheStatus string, cacheKey string, req *http.Request, stats stopWatches) {
	for k, v := range respHeaders {
		w.Header().Set(k, strings.Join(v, " "))
	}
	w.WriteHeader(statusCode)
	if req != nil {
		cacheLog(req, statusCode, respHeaders, cacheStatus, cacheKey, stats)
	}
	updateChan <- cacheReqResp{cacheObj: cacheObj{Headers: respHeaders.Clone()}, req: *req}
}
