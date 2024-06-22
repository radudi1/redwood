package responseCache

import (
	"context"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/zeebo/xxh3"
)

var redisConn rueidis.Client
var redisConnCompat rueidiscompat.Cmdable
var redisContext context.Context

func RedisInit() {
	// connect to redis
	redisConn, redisConnCompat = cacheOpen()
	if redisConn == nil {
		log.Panicln("!!! ERROR !!! COULD NOT CONNECT TO REDIS - responseCache IS DISABLED")
	}
	redisContext = context.Background()
}

func HashKey(key string) string {
	sum := xxh3.HashString128(key)
	hiStr := strconv.FormatUint(sum.Hi, 16)
	loStr := strconv.FormatUint(sum.Lo, 16)
	for i := 0; len(hiStr) < 16; i++ {
		hiStr = "0" + hiStr
	}
	for i := 0; len(loStr) < 16; i++ {
		loStr = "0" + loStr
	}
	return hiStr + loStr
}

func Serialize(data interface{}) (string, error) {
	serData, serErr := msgpack.Marshal(data)
	if serErr != nil {
		log.Println(serErr)
		return "", serErr
	}
	return string(serData), nil
}

func Unserialize(serializedData string, dstData interface{}) error {
	serErr := msgpack.Unmarshal([]byte(serializedData), dstData)
	if serErr != nil {
		log.Println(serErr)
		return serErr
	}
	return nil
}

func Hmget(key string, fields ...string) (map[string]string, error) {
	redisArr, redisErr := redisConn.Do(redisContext, redisConn.B().Hmget().Key(key).Field(fields...).Build()).AsStrSlice()
	if redisErr != nil {
		return nil, redisErr
	}
	m := make(map[string]string, len(fields))
	for i, k := range fields {
		m[k] = redisArr[i]
	}
	return m, nil
}

func CacheConn() rueidiscompat.Cmdable {
	return redisConnCompat
}

func cacheOpen() (redisConn rueidis.Client, redisConnCompat rueidiscompat.Cmdable) {
	opts, err := rueidis.ParseURL(config.Redis.Url)
	if err != nil {
		log.Println("Invalid Redis URL. Caching disabled ", err)
		return nil, nil
	}
	opts.SelectDB = config.Redis.DBNum
	opts.PipelineMultiplex = int(math.Log2(float64(config.Redis.MaxNumConn)))
	opts.DisableCache = true
	opts.MaxFlushDelay = time.Duration(config.Redis.PipelineDeadlineUS) * time.Microsecond
	opts.RingScaleEachConn = int(math.Log2(float64(config.Redis.MaxPipelineLen)))
	redisConn, err = rueidis.NewClient(opts)
	if err != nil {
		log.Println("Could not connect to Redis. Caching disabled ", err)
		return nil, nil
	}
	redisConnCompat = rueidiscompat.NewAdapter(redisConn)
	return
}
