package responseCache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

var redisConn rueidiscompat.Cmdable
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

func CacheConn() rueidiscompat.Cmdable {
	return redisConn
}

func cacheOpen() rueidiscompat.Cmdable {
	opts, err := rueidis.ParseURL(config.Redis.Url)
	if err != nil {
		log.Println("Invalid Redis URL. Caching disabled ", err)
		return nil
	}
	opts.SelectDB = config.Redis.DBNum
	opts.PipelineMultiplex = int(math.Log2(float64(config.Redis.MaxNumConn)))
	opts.DisableCache = true
	opts.MaxFlushDelay = time.Duration(config.Redis.PipelineDeadlineUS) * time.Microsecond
	opts.RingScaleEachConn = int(math.Log2(float64(config.Redis.MaxPipelineLen)))
	conn, err := rueidis.NewClient(opts)
	if err != nil {
		log.Println("Could not connect to Redis. Caching disabled ", err)
		return nil
	}
	compat := rueidiscompat.NewAdapter(conn)
	return compat
}
