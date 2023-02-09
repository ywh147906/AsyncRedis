package redis

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/ywh147906/AsyncRedis/log"
	"go.uber.org/zap"
)

const (
	CAPACITY = 256
)

var RedisTimeOutErr = errors.New("redis time out error")

var RequestQueueFull = errors.New("request queue full")

type Request struct {
	cmd    string
	args   []interface{}
	future *Future
}

type Future struct {
	r       interface{}
	err     error
	wait    chan struct{}
	timeout int64
}

func (f *Future) SetTimeout() {
	atomic.StoreInt64(&f.timeout, 1)
}

func (f *Future) IsTimeout() bool {
	return atomic.LoadInt64(&f.timeout) == 1
}

func (f *Future) Reset() {
	f.r = nil
	f.err = nil
	select {
	case <-f.wait:
	default:
	}
	atomic.StoreInt64(&f.timeout, 0)
}

func (f *Future) Close() {
	select {
	case f.wait <- struct{}{}:
	default:
	}
}

type FutureConn struct {
	future *Future
	conn   redis.Conn
}

type Client struct {
	requestQueue *EsQueue
	requestCnt   int32
	reqM         *sync.Mutex
	reqC         *sync.Cond

	futureQueue *EsQueue
	futureCnt   int32
	futureM     *sync.Mutex
	futureC     *sync.Cond

	mgr   *RoundRobinAsyncClient
	index int
	conn  redis.Conn
	mu    sync.RWMutex

	close bool

	logger log.Logger
}

func NewClient(mgr *RoundRobinAsyncClient, index int, conn redis.Conn, logger log.Logger) *Client {
	c := &Client{
		mgr:          mgr,
		index:        index,
		requestQueue: NewQueue(CAPACITY),
		futureQueue:  NewQueue(CAPACITY),
		conn:         conn,
		reqM:         &sync.Mutex{},
		futureM:      &sync.Mutex{},
		logger:       logger,
	}
	c.reqC = sync.NewCond(c.reqM)
	c.futureC = sync.NewCond(c.futureM)
	go c.processRequest()
	go c.processFuture()
	return c
}

func (cli *Client) ResetConn(conn redis.Conn) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	cli.conn = conn
}

func (cli *Client) getConn() redis.Conn {
	cli.mu.RLock()
	defer cli.mu.RUnlock()
	return cli.conn
}

//Do ...
func (cli *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	return cli.conn.Do(cmd, args)
}

//AsyncDo ...
func (cli *Client) AsyncDo(cmd string, args ...interface{}) (interface{}, error) {
	//future := &Future{
	//	wait: make(chan struct{}),
	//}
	future := globalFuturePool.Get()
	defer globalFuturePool.Put(future)

	ok := cli.pushRequest(&Request{cmd, args, future})
	if !ok {
		return nil, RequestQueueFull
	}

	timer := globalTimerPool.Get(2 * time.Second)
	defer globalTimerPool.Put(timer)

	select {
	case <-future.wait:
		return future.r, future.err
	case <-timer.C:
		future.SetTimeout()
		return nil, RedisTimeOutErr
	}
}

func (cli *Client) pushRequest(req *Request) bool {
	ok, _ := cli.requestQueue.Put(req)
	var retry = 20
	for retry > 0 && !ok {
		ok, _ = cli.requestQueue.Put(req)
		time.Sleep(10 * time.Millisecond)
		retry -= 1
	}
	if !ok {
		return false
	}
	cli.requestSchedule()
	return true
}

func (cli *Client) postFuture(data interface{}) bool {
	var retry = 20
	ok, _ := cli.futureQueue.Put(data)
	for retry > 0 && !ok {
		ok, _ = cli.futureQueue.Put(data)
		time.Sleep(10 * time.Millisecond)
		retry -= 1
	}
	if !ok {
		return false
	}
	cli.futureSchedule()
	return true
}

func (cli *Client) requestSchedule() {
	cli.reqC.Signal()
}

func (cli *Client) futureSchedule() {
	cli.futureC.Signal()
}

func (cli *Client) processRequest() {
	defer func() {
		if e := recover(); e != nil {
			cli.logger.Error("panic", zap.Any("panic info", e))
		}
	}()
	for !cli.close {
		cli.requestRun()
		cli.reqM.Lock()
		if cli.requestQueue.Quantity() <= 0 {
			cli.reqC.Wait()
		}
		cli.reqM.Unlock()
	}
}

func (cli *Client) processFuture() {
	defer func() {
		if e := recover(); e != nil {
			cli.logger.Error("panic", zap.Any("panic info", e))
		}
	}()
	for !cli.close {
		cli.futureRun()
		cli.futureM.Lock()
		if cli.futureQueue.Quantity() <= 0 {
			cli.futureC.Wait()
		}
		cli.futureM.Unlock()
	}
}

func (cli *Client) requestRun() {
	var err error
	var index = 0
	var conn = cli.getConn()
	var gets [CAPACITY]interface{}
	num, _ := cli.requestQueue.Gets(gets[:])
	if num <= 0 {
		return
	}
	for index = 0; index < int(num); index++ {
		req := gets[index].(*Request)
		if req.future.IsTimeout() {
			continue
		}
		err = conn.Send(req.cmd, req.args...)
		if err == nil {
			fc := globalFutureConnPool.Get(req.future, conn)
			flag := cli.postFuture(fc)
			if !flag {
				req.future.Close()
			}
		} else {
			fmt.Println("send request error", req.cmd, err)
			break
		}
	}
	if err != nil {
		for index < int(num) {
			req := gets[index].(*Request)
			req.future.err = err
			//close(req.future.wait)
			req.future.Close()
			index++
		}
	} else {
		err = conn.Flush()
	}

	if err != nil {
		cli.mgr.ResetClientConn(cli.index, cli)
	}
}

func (cli *Client) futureRun() {
	var gets [CAPACITY]interface{}
	if num, _ := cli.futureQueue.Gets(gets[:]); num > 0 {
		for index := 0; index < int(num); index++ {
			fConn := gets[index].(*FutureConn)
			resp, err := fConn.conn.Receive()
			if err == nil {
				fConn.future.r = resp
				//close(fConn.future.wait)
				fConn.future.Close()
			} else {
				fConn.future.err = err
				//close(fConn.future.wait)
				fConn.future.Close()
			}
			globalFutureConnPool.Put(fConn)
		}
	}
}

func (cli *Client) Close() {
	cli.close = true
	cli.conn.Close()
}
