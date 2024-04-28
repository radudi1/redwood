package responseCache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

var redisConnArr []*redis.Client
var redisCurrCon atomic.Int64
var ctx = context.Background()

func redisInit() {
	// connect to redis
	// we make sure that we get enough connections for workers and leave at least one connection for main processing
	if config.Redis.NumConn < config.Workers.CacheSetNumWorkers+config.Workers.CacheUpdateNumWorkers+1 {
		config.Redis.NumConn = config.Workers.CacheSetNumWorkers + config.Workers.CacheUpdateNumWorkers + 1
	}
	redisConnArr = make([]*redis.Client, config.Redis.NumConn)
	for k, _ := range redisConnArr {
		conn := cacheOpen()
		if conn == nil {
			log.Panicln("!!! ERROR !!! COULD NOT CONNECT TO REDIS - responseCache IS DISABLED")
		}
		redisConnArr[k] = conn
	}
}

func getKey(req *http.Request, varyHeader string) string {
	keyStr := req.Host + " " + req.RequestURI + req.Header.Get("Cookie")
	if varyHeader != "" {
		varyArr := strings.Split(varyHeader, ",")
		for _, v := range varyArr {
			keyStr += " " + req.Header.Get(v)
		}
	}
	sum := md5.Sum([]byte(keyStr))
	return hex.EncodeToString(sum[:])
}

func cacheConn() *redis.Client {
	idx := redisCurrCon.Add(1)
	if idx >= int64(config.Redis.NumConn) {
		redisCurrCon.Store(0)
		idx = 0
	}
	return redisConnArr[idx]
}

func cacheOpen() *redis.Client {
	opts, err := redis.ParseURL(config.Redis.Url)
	if err != nil {
		log.Println("Invalid Redis URL. Caching disabled ", err)
		return nil
	}
	opts.DB = config.Redis.DBNum
	redisConn := redis.NewClient(opts)
	return redisConn
}
