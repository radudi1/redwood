package responseCache

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/radudi1/prioworkers"
)

// worker priorities -- priorities are from 0 to 99 - 99 is the highest priority
const mainPrio = 90
const setWPrio = 50
const updateWPrio = 20

var logChan chan string

func Init() {

	loadConfig()

	if config.Redis.NumConn < 1 { // responseCache is actually disabled
		log.Println("!!! WARNING - responseCache is disabled! - set RedisNumConn to something greater than 0 in responseCache config in order to enable caching")
		return
	}

	// initialize logging worker
	logChan = make(chan string, config.Workers.LogBufferSize)
	if config.Workers.LogNumWorkers < 1 {
		config.Workers.LogNumWorkers = 1
	}
	for i := 0; i < config.Workers.LogNumWorkers; i++ {
		go cacheLogWorker()
	}

	redisInit()

	bumpInit()

	// init prioworkers
	prioworkers.Init(config.Workers.CacheSetNumWorkers + config.Workers.CacheUpdateNumWorkers)

	// spin up cache set workers
	setChan = make(chan cacheReqResp, config.Workers.CacheSetBufferSize)
	if config.Workers.CacheSetNumWorkers < 1 {
		config.Workers.CacheSetNumWorkers = 1
	}
	for i := 0; i < config.Workers.CacheSetNumWorkers; i++ {
		config.Redis.NumConn--
		go setWorker(redisConnArr[config.Redis.NumConn])
	}

	// spin up cache update workers
	updateChan = make(chan cacheReqResp, config.Workers.CacheUpdateBufferSize)
	if config.Workers.CacheUpdateNumWorkers < 1 {
		config.Workers.CacheUpdateNumWorkers = 1
	}
	for i := 0; i < config.Workers.CacheUpdateNumWorkers; i++ {
		config.Redis.NumConn--
		go updateTtlWorker(redisConnArr[config.Redis.NumConn])
	}

	// initialize other stuff
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR2)
	go signalHandler(signalChan)

}

func cacheLog(req *http.Request, statusCode int, respHeaders http.Header, cacheStatus string, cacheKey string, stats stopWatches) {
	logChan <- fmt.Sprintln(
		req.RemoteAddr,
		cacheStatus,
		statusCode,
		req.Method,
		respHeaders.Get("Content-Encoding"),
		respHeaders.Get("Content-Length"),
		strings.SplitN(respHeaders.Get("Content-Type"), ";", 2)[0],
		strings.ReplaceAll(respHeaders.Get("Cache-Control"), " ", ""),
		limitStr(req.URL.String(), 128),
		stats.getSw.GetRunningDuration().Milliseconds(),
		stats.setSw.GetRunningDuration().Milliseconds(),
		(stats.getSw.GetRunningDuration() + stats.setSw.GetRunningDuration()).Milliseconds(),
		stats.getSw.GetDurationSinceStart().Milliseconds(),
		cacheKey,
	)
}

func cacheLogWorker() {
	var logFile *os.File
	if config.Log.LogFile != "" {
		var err error
		logFile, err = os.OpenFile(config.Log.LogFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Println(err)
		} else {
			defer logFile.Close()
		}
	}
	for {
		// read strings from the channel here and write them to the buffer
		str := <-logChan
		if logFile != nil {
			logLine := fmt.Sprint(time.Now().Format("2006-01-02 15:04:05.000000"), " ", str)
			logFile.WriteString(logLine)
		}
	}
}

// os signal processing
func signalHandler(c chan os.Signal) {
	for {
		<-c
		log.Println("No-bump domains: ", noBumpDomains)
	}
}
