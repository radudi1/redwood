package wrappers

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
)

type RedisConfig struct {
	Url                  string
	DBNum                int
	NumConnectRetries    int
	ConnectRetryInterval int
	MaxNumConn           int
	MaxPipelineLen       int
	PipelineDeadlineUS   int
}

type RedisWrapper struct {
	conn       rueidis.Client
	compatConn rueidiscompat.Cmdable
	Context    context.Context
}

func NewRedisWrapper(config RedisConfig) (*RedisWrapper, error) {
	wrapper := RedisWrapper{
		Context: context.Background(),
	}
	// connect to redis
	opts, err := rueidis.ParseURL(config.Url)
	if err != nil {
		log.Println("Invalid Redis URL. Caching disabled ", err)
		return nil, err
	}
	opts.SelectDB = config.DBNum
	opts.PipelineMultiplex = int(math.Log2(float64(config.MaxNumConn)))
	opts.DisableCache = true
	opts.MaxFlushDelay = time.Duration(config.PipelineDeadlineUS) * time.Microsecond
	opts.RingScaleEachConn = int(math.Log2(float64(config.MaxPipelineLen)))
	if config.NumConnectRetries < 0 {
		config.NumConnectRetries = math.MaxInt
	}
	wrapper.conn, err = rueidis.NewClient(opts)
	for i := 0; err != nil && i < config.NumConnectRetries; i++ {
		wrapper.conn, err = rueidis.NewClient(opts)
	}
	if err != nil {
		return nil, err
	}
	wrapper.compatConn = rueidiscompat.NewAdapter(wrapper.conn)
	return &wrapper, nil
}

func (wrapper *RedisWrapper) Hmget(key string, fields ...string) (map[string][]byte, error) {
	redisArr, redisErr := wrapper.conn.Do(wrapper.Context, wrapper.conn.B().Hmget().Key(key).Field(fields...).Build()).ToArray()
	if redisErr != nil {
		return nil, redisErr
	}
	m := make(map[string][]byte, len(fields))
	for i, k := range fields {
		if redisArr[i].Error() != nil {
			m[k] = nil
			continue
		}
		b, err := redisArr[i].AsBytes()
		if err != nil {
			return nil, err
		}
		m[k] = b
	}
	return m, nil
}

func (wrapper *RedisWrapper) GetConn() rueidis.Client {
	return wrapper.conn
}

func (wrapper *RedisWrapper) GetCompatConn() rueidiscompat.Cmdable {
	return wrapper.compatConn
}
