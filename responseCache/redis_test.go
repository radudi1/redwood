package responseCache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRedisPipeline_Cmd(t *testing.T) {
	config.Redis = &RedisConfig{
		Url:        "redis://keydb:6379",
		DBNum:      2,
		MaxNumConn: 4096,
	}
	type args struct {
		key string
		val string
		ttl time.Duration
	}
	type TestType struct {
		name       string
		ctx        context.Context
		cmd        string
		args       []args
		wantRetval interface{}
		wantErr    bool
	}
	var tests []TestType
	// create test data
	var data []args
	for i := 0; i < 100000; i++ {
		k := md5.Sum([]byte(fmt.Sprint(rand.Int())))
		v := md5.Sum([]byte(fmt.Sprint(rand.Int())))
		data = append(data, args{
			key: hex.EncodeToString(k[:]),
			val: hex.EncodeToString(v[:]),
			ttl: time.Duration(30 * time.Second),
		})
	}
	// set test
	tests = append(tests, TestType{
		name:    "set",
		ctx:     context.Background(),
		cmd:     "set",
		args:    data,
		wantErr: false,
	})
	// ttl update test
	tests = append(tests, TestType{
		name:    "expire",
		ctx:     context.Background(),
		cmd:     "expire",
		args:    data,
		wantErr: false,
	})
	// get test
	tests = append(tests, TestType{
		name:    "get",
		ctx:     context.Background(),
		cmd:     "get",
		args:    data,
		wantErr: false,
	})
	// init and run tests
	RedisInit()
	pipe := NewRedisPipeline(context.Background(), 1000, 100*time.Millisecond)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			for k, v := range tt.args {
				wg.Add(1)
				go func(k int, v args) {
					gotRetval, err := pipe.Cmd(tt.ctx, tt.cmd, v.key, v.val, v.ttl)
					if (err != nil) != tt.wantErr {
						t.Errorf("RedisPipeline.Cmd() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
					switch tt.cmd {
					case "set":
						if !reflect.DeepEqual(gotRetval, "OK") {
							t.Errorf("RedisPipeline.Cmd() = %v, want %v", gotRetval, "OK")
						}
					case "expire":
						if !reflect.DeepEqual(gotRetval, true) {
							t.Errorf("RedisPipeline.Cmd() = %v, want %v", gotRetval, true)
						}
					case "get":
						if !reflect.DeepEqual(gotRetval, v.val) {
							t.Errorf("RedisPipeline.Cmd() = %v, want %v", gotRetval, v.val)
						}
					}
					wg.Done()
				}(k, v)
			}
			wg.Wait()
		})
	}

}
