package responseCache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPipelineCmd struct {
	ctx      context.Context
	redisCmd string
	key      string
	val      string
	ttl      time.Duration
	valChan  chan interface{}
	errChan  chan error
}

type RedisPipelineStats struct {
	Iterations int64
	NumCmds    int64
	MaxNumCmds int64
	AvgNumCmds float64
}

type RedisPipelineCounters struct {
	iterations atomic.Int64
	numCmds    atomic.Int64
	maxNumCmds atomic.Int64
}

type RedisPipeline struct {
	context   context.Context
	maxLen    int
	deadline  time.Duration
	redisPipe redis.Pipeliner
	cmdChan   chan RedisPipelineCmd
	ticker    *time.Ticker
	counters  RedisPipelineCounters
}

var redisConn *redis.Client
var redisContext context.Context

func RedisInit() {
	// connect to redis
	redisConn = cacheOpen()
	if redisConn == nil {
		log.Panicln("!!! ERROR !!! COULD NOT CONNECT TO REDIS - responseCache IS DISABLED")
	}
	redisContext = context.Background()
}

func GetKey(req *http.Request, varyHeader string) string {
	keyStr := req.Host + " " + req.RequestURI + strings.Join(req.Header.Values("Cookie"), "") + strings.Join(req.Header.Values("X-API-Key"), "")
	if varyHeader != "" {
		varyArr := strings.Split(varyHeader, ",")
		for _, v := range varyArr {
			keyStr += " " + req.Header.Get(v)
		}
	}
	sum := md5.Sum([]byte(keyStr))
	return hex.EncodeToString(sum[:])
}

func CacheConn() *redis.Client {
	return redisConn
}

func NewRedisPipeline(ctx context.Context, maxPipeLen int, pipeDeadline time.Duration) *RedisPipeline {
	pipe := &RedisPipeline{
		context:   ctx,
		maxLen:    maxPipeLen,
		deadline:  pipeDeadline,
		redisPipe: redisConn.Pipeline(),
		cmdChan:   make(chan RedisPipelineCmd, maxPipeLen),
	}
	go pipe.redisPipelineGather()
	return pipe
}

func (pipe *RedisPipeline) Get(ctx context.Context, key string) (retval string, err error) {
	retVal, retErr := pipe.Cmd(ctx, "get", key, "", 0)
	return retVal.(string), retErr
}

func (pipe *RedisPipeline) Set(ctx context.Context, key string, val string, ttl time.Duration) (err error) {
	_, retErr := pipe.Cmd(ctx, "set", key, val, ttl)
	return retErr
}

func (pipe *RedisPipeline) Expire(ctx context.Context, key string, ttl time.Duration) (err error) {
	_, retErr := pipe.Cmd(ctx, "expire", key, "", ttl)
	return retErr
}

func (pipe *RedisPipeline) Cmd(ctx context.Context, cmd string, key string, val string, ttl time.Duration) (retval interface{}, err error) {
	pipeCmd := RedisPipelineCmd{
		ctx:      ctx,
		redisCmd: cmd,
		key:      key,
		val:      val,
		ttl:      ttl,
		valChan:  make(chan interface{}, 1),
		errChan:  make(chan error, 1),
	}
	pipe.cmdChan <- pipeCmd
	retval = <-pipeCmd.valChan
	close(pipeCmd.valChan)
	err = <-pipeCmd.errChan
	close(pipeCmd.errChan)
	return
}

func (pipe *RedisPipeline) GetStats() RedisPipelineStats {
	if pipe.counters.iterations.Load() == 0 {
		return RedisPipelineStats{}
	}
	stats := RedisPipelineStats{
		Iterations: pipe.counters.iterations.Load(),
		NumCmds:    pipe.counters.numCmds.Load(),
		MaxNumCmds: pipe.counters.maxNumCmds.Load(),
		AvgNumCmds: float64(pipe.counters.numCmds.Load()) / float64(pipe.counters.iterations.Load()),
	}
	return stats
}

func (pipe *RedisPipeline) redisPipelineGather() {
	pipe.counters.iterations.Add(1)
	cmdMap := make(map[interface{}]*RedisPipelineCmd)
	var cmdId interface{}
	pipeLen := 0
	pipe.ticker = time.NewTicker(pipe.deadline)
	for {
		select {
		case cmd := <-pipe.cmdChan:
			pipeLen++
			switch cmd.redisCmd {
			case "get":
				cmdId = pipe.redisPipe.Get(cmd.ctx, cmd.key)
			case "set":
				cmdId = pipe.redisPipe.Set(cmd.ctx, cmd.key, cmd.val, cmd.ttl)
			case "expire":
				cmdId = pipe.redisPipe.Expire(cmd.ctx, cmd.key, cmd.ttl)
			default:
				panic("Redis command " + cmd.redisCmd + " currently not supported by our redis pipeline")
			}
			// add the new command to map
			cmdMap[cmdId] = &cmd
			// if we reach maximum pipe length we execute pipe and exit - current pipe is finished
			if pipeLen >= pipe.maxLen {
				pipe.redisPipelineExec(cmdMap)
				return
			}
		case <-pipe.ticker.C: // deadline reached
			// if there's at least one command in pipe we execute the pipe and exit - current pipe is finished
			// if there are no commands in queue there's nothing to send to redis and we can continue with a new cycle
			if pipeLen > 0 {
				pipe.redisPipelineExec(cmdMap)
				return
			}
		}
	}
}

// redisPipelineExec sends current pipe to redis, creates a new pipe for future commands and does current pipe cleanup
// It must be called exactly once by redisPipelineGather right before it exits
func (pipe *RedisPipeline) redisPipelineExec(cmdMap map[interface{}]*RedisPipelineCmd) {
	// stop ticker
	pipe.ticker.Stop()
	// save current pipe, create a new pipe and launch a new gatherer as soon as possible
	curRedisPipe := pipe.redisPipe
	pipe.redisPipe = redisConn.Pipeline()
	go pipe.redisPipelineGather()
	// update counters
	pipe.counters.numCmds.Add(int64(len(cmdMap)))
	if int64(len(cmdMap)) > pipe.counters.maxNumCmds.Load() {
		pipe.counters.maxNumCmds.Store(int64(len(cmdMap)))
	}
	// send pipe to redis
	_, err := curRedisPipe.Exec(pipe.context)
	if err != nil {
		for _, cmd := range cmdMap {
			cmd.valChan <- ""
			cmd.errChan <- err
		}
		return
	}
	// return results
	for redisCmd, cmd := range cmdMap {
		if v, ok := interface{}(redisCmd).(*redis.StringCmd); ok {
			cmd.valChan <- v.Val()
			cmd.errChan <- v.Err()
		} else if v, ok := interface{}(redisCmd).(*redis.StatusCmd); ok {
			cmd.valChan <- v.Val()
			cmd.errChan <- v.Err()
		} else if v, ok := interface{}(redisCmd).(*redis.BoolCmd); ok {
			cmd.valChan <- v.Val()
			cmd.errChan <- v.Err()
		}
	}
}

func cacheOpen() *redis.Client {
	opts, err := redis.ParseURL(config.Redis.Url)
	if err != nil {
		log.Println("Invalid Redis URL. Caching disabled ", err)
		return nil
	}
	opts.DB = config.Redis.DBNum
	opts.MaxActiveConns = config.Redis.MaxNumConn
	conn := redis.NewClient(opts)
	return conn
}
