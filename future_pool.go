package redis

import (
	"sync"

	"github.com/gomodule/redigo/redis"
)

var globalFuturePool futurePool

type futurePool struct {
	p sync.Pool
}

func (f *futurePool) Get() *Future {
	if fu, _ := f.p.Get().(*Future); fu != nil {
		fu.Reset()
		return fu
	}
	return &Future{
		wait: make(chan struct{}, 1),
	}
}

func (f *futurePool) Put(fu *Future) {
	fu.Reset()
	f.p.Put(fu)
}

var globalFutureConnPool futureConnPool

type futureConnPool struct {
	p sync.Pool
}

func (f *futureConnPool) Get(future *Future, conn redis.Conn) *FutureConn {
	if fu, _ := f.p.Get().(*FutureConn); fu != nil {
		fu.future = future
		fu.conn = conn
		return fu
	}
	return &FutureConn{future: future, conn: conn}
}

func (f *futureConnPool) Put(fu *FutureConn) {
	fu.future = nil
	fu.conn = nil
	f.p.Put(fu)
}
