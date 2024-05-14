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
const revalidateWPrio = 40
const updateWPrio = 20

var logChan chan string
var revalidateLogChan chan string

func Init() {

	loadConfig()

	if config.Redis.NumConn < 1 { // responseCache is actually disabled
		log.Println("!!! WARNING - responseCache is disabled! - set RedisNumConn to something greater than 0 in responseCache config in order to enable caching")
		return
	}

	// initialize logging workers
	logChan = make(chan string, config.Log.LogBufferSize)
	go cacheLogWorker(config.Log.LogFile, logChan)
	revalidateLogChan = make(chan string, config.Log.LogBufferSize)
	go cacheLogWorker(config.Log.RevalidateLogFile, revalidateLogChan)

	redisInit()

	bumpInit()

	// init prioworkers
	if config.Workers.PrioritiesEnabled {
		prioworkers.Init(config.Workers.WorkerBufferSize, &prioworkers.PrioworkersOptions{
			MaxBlockedWorkers:        config.Workers.WorkerBufferSize,
			LowPrioSpinCnt:           max(config.Workers.CacheSetNumWorkers, config.Workers.CacheUpdateNumWorkers),
			EnforceSpinCntOnHighPrio: false,
		})
	}

	// spin up cache set workers
	setChan = make(chan cacheReqResp, config.Workers.WorkerBufferSize)
	if config.Workers.CacheSetNumWorkers < 1 {
		config.Workers.CacheSetNumWorkers = 1
	}
	for i := 0; i < config.Workers.CacheSetNumWorkers; i++ {
		config.Redis.NumConn--
		go setWorker(redisConnArr[config.Redis.NumConn])
	}

	// spin up cache update workers
	updateChan = make(chan cacheReqResp, config.Workers.WorkerBufferSize)
	if config.Workers.CacheUpdateNumWorkers < 1 {
		config.Workers.CacheUpdateNumWorkers = 1
	}
	for i := 0; i < config.Workers.CacheUpdateNumWorkers; i++ {
		config.Redis.NumConn--
		go updateTtlWorker(redisConnArr[config.Redis.NumConn])
	}

	// spin up revalidate workers
	if config.StandardViolations.EnableStandardViolations && config.StandardViolations.ServeStale {
		revalidateChan = make(chan http.Request, config.Workers.WorkerBufferSize)
		if config.StandardViolations.RevalidateNumWorkers < 1 {
			config.StandardViolations.RevalidateNumWorkers = 1
		}
		for i := 0; i < config.StandardViolations.RevalidateNumWorkers; i++ {
			go revalidateWorker()
		}
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

func cacheLogWorker(filename string, logChan chan string) {
	var logFile *os.File
	if filename == "" {
		log.Println("No filename given for ResponseCache log. Logging disabled")
	}
	var err error
	logFile, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Println(err)
		log.Println("ResponseCache logging to standard output")
	} else {
		defer logFile.Close()
	}
	for str := range logChan {
		// read strings from the channel here and write them to file or stdout
		logLine := fmt.Sprint(time.Now().Format("2006-01-02 15:04:05.000000"), " ", str)
		if logFile != nil {
			logFile.WriteString(logLine)
		} else {
			fmt.Print(logLine)
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
