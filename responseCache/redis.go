package responseCache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"
	"net/http"
	"strings"

	"github.com/redis/go-redis/v9"
)

var redisConn *redis.Client
var redisContext context.Context

func redisInit() {
	// connect to redis
	redisConn = cacheOpen()
	if redisConn == nil {
		log.Panicln("!!! ERROR !!! COULD NOT CONNECT TO REDIS - responseCache IS DISABLED")
	}
	redisContext = context.Background()
}

func getKey(req *http.Request, varyHeader string) string {
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

func cacheConn() *redis.Client {
	return redisConn
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
