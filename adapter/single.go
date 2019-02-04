package redisadapter

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

type RedisClientImpl struct {
	mu      *sync.RWMutex
	mode    int
	host    string
	maxConn int
	batches map[string]*batch
	pool    *redis.Pool
}

type batch struct {
	totalCmd int
	conn     redis.Conn
}

func createPool(host string, maxConn int) *redis.Pool {
	return &redis.Pool{
		MaxActive:   maxConn,
		MaxIdle:     maxConn,
		IdleTimeout: 8 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Wait: true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialConnectTimeout(5*time.Second))
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			return c, err
		},
	}
}

func (c *RedisClientImpl) GetMaxConn() int {
	return c.maxConn
}

func (c *RedisClientImpl) NewBatch() string {
	name := uuid.New().String()
	defer c.mu.Unlock()
	c.mu.Lock()
	if _, ok := c.batches[name]; !ok {
		c.batches[name] = &batch{
			totalCmd: 0,
			conn:     c.pool.Get(),
		}
	}
	return name
}

func (c *RedisClientImpl) RunBatch(name string) (reply []interface{}, err error) {
	c.mu.RLock()
	if _, ok := c.batches[name]; !ok {
		return nil, errors.New("batch name not found")
	}
	if err = c.batches[name].conn.Flush(); err != nil {
		return nil, err
	}

	for i := 0; i < c.batches[name].totalCmd; i++ {
		var resp interface{}
		resp, err = c.batches[name].conn.Receive()
		if err != nil {
			return nil, err
		}
		reply = append(reply, resp)
	}
	c.mu.RUnlock()
	c.deleteBatch(name)

	return reply, err
}

func (c *RedisClientImpl) deleteBatch(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches[name].conn.Close()
	delete(c.batches, name)
}

func (c *RedisClientImpl) Send(name string, cmd string, args ...interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches[name].totalCmd++
	return c.batches[name].conn.Send(cmd, args...)
}
