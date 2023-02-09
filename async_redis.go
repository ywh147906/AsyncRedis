package redis

import (
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/ywh147906/AsyncRedis/log"
)

type AsyncRedis struct {
	client   *RoundRobinAsyncClient
	ping     sync.Once
	isOpened bool
}

func NewAsyncRedis(dial func() (redis.Conn, error), connectCnt int, logger log.Logger) *AsyncRedis {
	r := &AsyncRedis{}
	if connectCnt < 1 {
		connectCnt = 2
	}
	client, err := NewRoundRobinAsyncClient(dial, connectCnt, logger)
	PanicIfError(err)
	r.client = client
	r.isOpened = true
	r.doPing()
	return r
}

func (r *AsyncRedis) doPing() {
	r.ping.Do(func() {
		go func() {
			for {
				r.AsyncDo("PING")
				time.Sleep(10 * time.Second)
			}
		}()
	})
}

func (r *AsyncRedis) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return r.client.Do(commandName, args...)
}

func (r *AsyncRedis) AsyncDo(commandName string, args ...interface{}) (interface{}, error) {
	return r.client.AsyncDo(commandName, args...)
}

func (r *AsyncRedis) Close() {
	r.client.Close()
}
