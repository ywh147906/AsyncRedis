package redis

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	goRedis "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const MaxCount = 100

var values = map[int]KV{}
var client = NewAsyncRedis(dial, 5, NewLogger())

type KV struct {
	Key string
	Val string
}

const (
	ADDR = "10.23.20.53:9223"
	PWD  = "pika336"
)

func init() {
	for i := 0; i <= MaxCount; i++ {
		values[i] = KV{Key: fmt.Sprintf("key:%d", i), Val: fmt.Sprintf("value:%d", i)}
	}
}

func dial() (redis.Conn, error) {
	return redis.Dial("tcp",
		ADDR,
		redis.DialDatabase(0),
		redis.DialPassword(PWD))
}

func NewLogger() *zap.Logger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.EncoderConfig.TimeKey = "time"
	logConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, _ := logConfig.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
		zap.Fields(zap.String("@service", "coin-im-comet")),
	)
	return logger
}

func TestAsyncDo(t *testing.T) {
	for i := 0; i < 100; i++ {
		kv := values[i]
		_, err := redis.String(client.AsyncDo("SET", kv.Key, kv.Val))
		PanicIfError(err)
	}

	for i := 0; i < 100; i++ {
		kv := values[i]
		v, err := redis.String(client.AsyncDo("GET", kv.Key))
		PanicIfError(err)
		if v != kv.Val {
			panic("value error")
		}
	}
}

func BenchmarkAsyncDo(b *testing.B) {
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			kv1 := values[rand.Intn(MaxCount)]
			_, err := redis.String(client.AsyncDo("SET", kv1.Key, kv1.Val))
			PanicIfError(err)
			kv2 := values[rand.Intn(MaxCount)]
			val, err := redis.String(client.AsyncDo("GET", kv2.Key))
			PanicIfError(err)
			if val != kv2.Val {
				panic("value error")
			}
		}
	})
}

func BenchmarkPoolDo(b *testing.B) {
	p := &redis.Pool{
		Dial:      dial,
		MaxActive: 50,
	}
	defer p.Close()
	b.RunParallel(func(b *testing.PB) {
		c := p.Get()
		defer c.Close()
		for b.Next() {
			kv1 := values[rand.Intn(MaxCount)]
			_, err := redis.String(c.Do("SET", kv1.Key, kv1.Val))
			PanicIfError(err)
			kv2 := values[rand.Intn(MaxCount)]
			val, err := redis.String(c.Do("GET", kv2.Key))
			PanicIfError(err)
			if val != kv2.Val {
				panic("value error")
			}
		}
	})
}

func BenchmarkGoRedis(b *testing.B) {
	rdb := goRedis.NewClient(&goRedis.Options{
		Addr:     ADDR,
		Password: PWD, // no password set
		DB:       0,   // use default DB
		PoolSize: 50,
	})
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			kv1 := values[rand.Intn(MaxCount)]
			_, err := rdb.Set(context.Background(), kv1.Key, kv1.Val, 0).Result()
			PanicIfError(err)
			kv2 := values[rand.Intn(MaxCount)]
			val, err := rdb.Get(context.Background(), kv2.Key).Result()
			PanicIfError(err)
			if val != kv2.Val {
				panic("value error")
			}
		}
	})
}
