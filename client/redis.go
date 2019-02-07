package redisadapter

import (
	"sync"

	rc "github.com/chasex/redis-go-cluster"
)

const (
	//CLUSTER_MODE 0
	CLUSTER_MODE int = iota
	// SINGLE_MODE 1
	SINGLE_MODE
)

type RedisClient interface {
	GetMaxConn() int
	NewBatch() string
	RunBatch(string) ([]interface{}, error)
	Send(string, string, ...interface{}) error
}

// New return new RedisClient
func New(mode int, host string, maxConn int) RedisClient {
	switch mode {
	case CLUSTER_MODE:
		c := &RedisClusterClientImpl{
			mu:      &sync.RWMutex{},
			mode:    mode,
			host:    host,
			maxConn: maxConn,
			batches: make(map[string]*rc.Batch),
			cluster: createCluster(host, maxConn),
		}
		return c

	default:
		r := &RedisClientImpl{
			mu:      &sync.RWMutex{},
			mode:    mode,
			host:    host,
			maxConn: maxConn,
			batches: make(map[string]*batch),
			pool:    createPool(host, maxConn),
		}
		return r
	}
}
