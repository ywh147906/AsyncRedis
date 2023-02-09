package redis

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/ywh147906/AsyncRedis/log"
)

type RoundRobinAsyncClient struct {
	Dial       func() (redis.Conn, error)
	connectCnt int
	clients    []*Client
	index      uint64
}

func NewRoundRobinAsyncClient(newFn func() (redis.Conn, error), connectCnt int, logger log.Logger) (*RoundRobinAsyncClient, error) {
	ret := &RoundRobinAsyncClient{
		Dial:       newFn,
		connectCnt: connectCnt,
		clients:    make([]*Client, 0),
	}
	err := ret.init(logger)
	return ret, err
}

func (cli *RoundRobinAsyncClient) init(logger log.Logger) (err error) {
	for i := 0; i < cli.connectCnt; i++ {
		c, err := cli.Dial()
		PanicIfError(err)
		client := NewClient(cli, i, c, logger)
		cli.clients = append(cli.clients, client)
	}
	return
}

func (cli *RoundRobinAsyncClient) get() int {
	l := uint64(len(cli.clients))
	if l <= 0 {
		return -1
	}
	return int(atomic.AddUint64(&cli.index, 1) % l)
}

func (cli *RoundRobinAsyncClient) ResetClientConn(index int, client *Client) {
	for {
		c, err := cli.Dial()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		cli.clients[index].ResetConn(c)
		break
	}
}

// 异步
func (cli *RoundRobinAsyncClient) AsyncDo(cmd string, args ...interface{}) (interface{}, error) {
	if len(cli.clients) == 0 {
		return nil, errors.New("no clients")
	}
	index := cli.get()
	if index < 0 {
		return nil, errors.New("no clients")
	}
	return cli.clients[index].AsyncDo(cmd, args...)
}

// 同步
func (cli *RoundRobinAsyncClient) Do(cmd string, args ...interface{}) (interface{}, error) {
	if len(cli.clients) == 0 {
		return nil, errors.New("no clients")
	}
	index := cli.get()
	if index < 0 {
		return nil, errors.New("no clients")
	}
	return cli.clients[index].Do(cmd, args...)
}

func (cli *RoundRobinAsyncClient) GetConn() (redis.Conn, error) {
	index := cli.get()
	if index < 0 {
		return nil, errors.New("no clients")
	}
	return cli.clients[index].conn, nil
}

func (cli *RoundRobinAsyncClient) Close() {
	for _, client := range cli.clients {
		if client != nil {
			client.Close()
		}
	}
	return
}
