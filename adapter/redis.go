package redisadapter

import (
	rc "github.com/chasex/redis-go-cluster"
	"github.com/gomodule/redigo/redis"
)

const (
	CLUSTER_MODE int = iota
	SINGLE_MODE
)

type RedisClient interface {
	GetMode() int
	GetMaxConn() int
	GetConn() redis.Conn
	NewBatch() *rc.Batch
	RunBatch(*rc.Batch) ([]interface{}, error)
}

func New(mode int, host string, maxConn int) (RedisClient, error) {
	switch mode {
	case CLUSTER_MODE:
		c := &RedisClusterClientImpl{mode: mode, host: host, maxConn: maxConn}
		err := c.CreateCluster()
		return c, err

	default:
		r := &RedisClientImpl{mode: mode, host: host, maxConn: maxConn}
		err := r.CreatePool()
		return r, err
	}
}
