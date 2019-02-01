package redisadapter

import (
	"fmt"
	"time"

	rc "github.com/chasex/redis-go-cluster"
	"github.com/gomodule/redigo/redis"
)

type RedisClientImpl struct {
	mode    int
	host    string
	maxConn int
	pool    *redis.Pool
}

func (r *RedisClientImpl) GetMode() int {
	return r.mode
}

func (c *RedisClientImpl) GetMaxConn() int {
	return c.maxConn
}

func (r *RedisClientImpl) CreatePool() error {
	r.pool = &redis.Pool{
		MaxActive:   r.maxConn,
		MaxIdle:     r.maxConn,
		IdleTimeout: 8 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Wait: true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", r.host, redis.DialConnectTimeout(5*time.Second))
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			return c, err
		},
	}
	_, err := r.pool.Get().Do("PING")
	return err
}

func (r *RedisClientImpl) GetConn() redis.Conn {
	return r.pool.Get()
}

func (c *RedisClientImpl) NewBatch() *rc.Batch {
	return &rc.Batch{}
}

func (c *RedisClientImpl) RunBatch(batch *rc.Batch) ([]interface{}, error) {
	return nil, nil
}
