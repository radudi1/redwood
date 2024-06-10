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

const (

	// worker priorities -- priorities are from 0 to 99 - 99 is the highest priority
	mainPrio        = 90
	setWPrio        = 50
	revalidateWPrio = 40
	updateWPrio     = 20

	// request sources
	srcClient     = 0
	srcRevalidate = 1
)

var logChan chan string
var revalidateLogChan chan string

func Init() {

	loadConfig()

	if !config.Cache.Enabled { // responseCache is actually disabled
		log.Println("!!! WARNING - responseCache is disabled! - set Enabled to true in the Cache section of responseCache.toml configuration file")
		return
	}

	// initialize logging workers
	logChan = make(chan string, config.Log.LogBufferSize)
	go cacheLogWorker(config.Log.LogFile, logChan)
	if config.StandardViolations.EnableStandardViolations && config.StandardViolations.ServeStale {
		revalidateLogChan = make(chan string, config.Log.LogBufferSize)
		go cacheLogWorker(config.Log.RevalidateLogFile, revalidateLogChan)
	}

	RedisInit()

	// create redis pipelines
	if config.Redis.MaxPipelineLen > 0 {
		if config.Redis.GetPipelineDeadlineUS > 0 {
			getPipe = NewRedisPipeline(redisContext, config.Redis.MaxPipelineLen, time.Duration(config.Redis.GetPipelineDeadlineUS*1000))
		}
		if config.Redis.SetPipelineDeadlineUS > 0 {
			setPipe = NewRedisPipeline(redisContext, config.Redis.MaxPipelineLen, time.Duration(config.Redis.SetPipelineDeadlineUS*1000))
		}
		if config.Redis.UpdatePipelineDeadlineUS > 0 {
			updatePipe = NewRedisPipeline(redisContext, config.Redis.MaxPipelineLen, time.Duration(config.Redis.UpdatePipelineDeadlineUS*1000))
		}
	}

	bumpInit()

	// init prioworkers
	if config.Workers.PrioritiesEnabled {
		prioworkers.Init(&prioworkers.PrioworkersOptions{
			MaxBlockedWorkers:        int64(config.Workers.WorkerBufferSize),
			LowPrioSpinCnt:           int64(max(config.Workers.CacheSetNumWorkers, config.Workers.CacheUpdateNumWorkers)),
			EnforceSpinCntOnHighPrio: false,
		})
	}

	// spin up cache set workers
	setChan = make(chan cacheReqResp, config.Workers.WorkerBufferSize)
	if config.Workers.CacheSetNumWorkers < 1 {
		config.Workers.CacheSetNumWorkers = 1
	}
	for i := 0; i < config.Workers.CacheSetNumWorkers; i++ {
		go setWorker()
	}

	// spin up cache update workers
	updateChan = make(chan cacheReqResp, config.Workers.WorkerBufferSize)
	if config.Workers.CacheUpdateNumWorkers < 1 {
		config.Workers.CacheUpdateNumWorkers = 1
	}
	for i := 0; i < config.Workers.CacheUpdateNumWorkers; i++ {
		go updateTtlWorker()
	}

	// spin up revalidate workers
	if config.Workers.RevalidateNumWorkers > 0 {
		revalidateChan = make(chan cacheReqResp, config.Workers.WorkerBufferSize)
		revalidateReqs = make(map[string]struct{})
		for i := 0; i < config.Workers.RevalidateNumWorkers; i++ {
			go revalidateWorker()
		}
	}

	// initialize other stuff
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR2)
	go signalHandler(signalChan)

}

func cacheLog(req *http.Request, statusCode int, respHeaders http.Header, cacheStatus string, cacheKey string, stats *stopWatches) {
	responsecacheLog(logChan, req, statusCode, respHeaders, cacheStatus, cacheKey, stats)
}

func revalidateLog(req *http.Request, statusCode int, respHeaders http.Header, cacheStatus string, cacheKey string, stats *stopWatches) {
	responsecacheLog(revalidateLogChan, req, statusCode, respHeaders, cacheStatus, cacheKey, stats)
}

func responsecacheLog(logChan chan string, req *http.Request, statusCode int, respHeaders http.Header, cacheStatus string, cacheKey string, stats *stopWatches) {
	logChan <- fmt.Sprintln(
		req.RemoteAddr,
		cacheStatus,
		statusCode,
		req.Method,
		respHeaders.Get("Content-Encoding"),
		respHeaders.Get("Content-Length"),
		strings.SplitN(respHeaders.Get("Content-Type"), ";", 2)[0],
		strings.ReplaceAll(respHeaders.Get("Vary"), " ", ""),
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
	for range c {
		// nobump domains
		fmt.Println("No-bump domains: ", noBumpDomains)
		// channel queues
		fmt.Println("Log queue length: ", len(logChan))
		fmt.Println("Revalidate log queue length: ", len(revalidateLogChan))
		fmt.Println("Set queue length: ", len(setChan))
		fmt.Println("Update queue length: ", len(updateChan))
		fmt.Println("Revalidate queue length: ", len(revalidateChan))
		// prioworkers state
		fmt.Printf("%+v\n", prioworkers.GetState())
		// pipeline stats
		if getPipe != nil {
			stats := getPipe.GetStats()
			fmt.Println("GET Pipe Stats:")
			fmt.Println("\tCmds per iteration: ", stats.AvgNumCmds, " avg, ", stats.MaxNumCmds, " max")
		}
		if setPipe != nil {
			stats := setPipe.GetStats()
			fmt.Println("SET Pipe Stats:")
			fmt.Println("\tCmds per iteration: ", stats.AvgNumCmds, " avg, ", stats.MaxNumCmds, " max")
		}
		if updatePipe != nil {
			stats := updatePipe.GetStats()
			fmt.Println("UPDATE Pipe Stats:")
			fmt.Println("\tCmds per iteration: ", stats.AvgNumCmds, " avg, ", stats.MaxNumCmds, " max")
		}
		// byte counters
		fmt.Printf("Hit MB: %d\n", counters.HitBytes.Load()/1024/1024)
		fmt.Printf("Miss MB: %d\n", counters.MissBytes.Load()/1024/1024)
		fmt.Printf("Uncacheable MB: %d\n", counters.UncacheableBytes.Load()/1024/1024)
		// request counters
		fmt.Printf("Hits: %d\n", counters.Hits.Load())
		fmt.Printf("Misses: %d\n", counters.Misses.Load())
		fmt.Printf("Uncacheable: %d\n", counters.Uncacheable.Load())
		fmt.Printf("Sets: %d\n", counters.Sets.Load())
		fmt.Printf("Updates: %d\n", counters.Updates.Load())
		fmt.Printf("Revalidations: %d\n", counters.Revalidations.Load())
		// error counters
		fmt.Printf("CacheErr: %d\n", counters.CacheErr.Load())
		fmt.Printf("SerErr: %d\n", counters.SerErr.Load())
		fmt.Printf("EncodeErr: %d\n", counters.EncodeErr.Load())
		fmt.Printf("ReadErr: %d\n", counters.ReadErr.Load())
		fmt.Printf("WriteErr: %d\n", counters.WriteErr.Load())
		// ratios
		if counters.Hits.Load()+counters.Misses.Load() == 0 { // prevent division by 0
			continue
		}
		fmt.Printf("Uncacheable Ratio: %d%%\n", counters.Uncacheable.Load()*100/(counters.Hits.Load()+counters.Misses.Load()+counters.Uncacheable.Load()))
		fmt.Printf("Uncacheable MB Ratio: %d%%\n", counters.UncacheableBytes.Load()*100/(counters.HitBytes.Load()+counters.MissBytes.Load()+counters.UncacheableBytes.Load()))
		fmt.Printf("Cacheable Hit Ratio: %d%%\n", counters.Hits.Load()*100/(counters.Hits.Load()+counters.Misses.Load()))
		fmt.Printf("Cacheable Hit MB Ratio: %d%%\n", counters.HitBytes.Load()*100/(counters.HitBytes.Load()+counters.MissBytes.Load()))
		fmt.Printf("Total Hit Ratio: %d%%\n", counters.Hits.Load()*100/(counters.Hits.Load()+counters.Misses.Load()+counters.Uncacheable.Load()))
		fmt.Printf("Total Hit MB Ratio: %d%%\n", counters.HitBytes.Load()*100/(counters.HitBytes.Load()+counters.MissBytes.Load()+counters.UncacheableBytes.Load()))
	}
}
