package redisadapter

import (
	"errors"
	"strings"
	"sync"
	"time"

	rc "github.com/chasex/redis-go-cluster"
	"github.com/google/uuid"
)

// RedisClusterClientImpl implementation of redis client for cluster mode
type RedisClusterClientImpl struct {
	mu      *sync.RWMutex
	mode    int
	host    string
	maxConn int
	batches map[string]*rc.Batch
	cluster *rc.Cluster
}

func createCluster(host string, maxConn int) *rc.Cluster {
	startNodes := strings.FieldsFunc(strings.Replace(host, " ", "", -1), func(r rune) bool {
		return r == ';' || r == ','
	})
	cluster, err := rc.NewCluster(&rc.Options{
		StartNodes:   startNodes,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		KeepAlive:    maxConn,
		AliveTime:    8 * time.Second,
	})
	if err != nil {
		panic("redis cluster panic!")
	}
	return cluster
}

// GetMaxConn Return Redis Cluster Max Connection Pool for each node
func (c *RedisClusterClientImpl) GetMaxConn() int {
	return c.maxConn
}

// NewBatch Init New Batch of Commands
func (c *RedisClusterClientImpl) NewBatch() string {
	name := uuid.New().String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.batches[name]; !ok {
		c.batches[name] = c.cluster.NewBatch()
	}
	return name
}

// RunBatch Run Commands in Single Batch
func (c *RedisClusterClientImpl) RunBatch(name string) ([]interface{}, error) {
	c.mu.RLock()
	if _, ok := c.batches[name]; !ok {
		return nil, errors.New("batch name not found")
	}
	batch := c.batches[name]
	c.mu.RUnlock()
	reply, err := c.cluster.RunBatch(batch)

	c.deleteBatch(name)
	return reply, err
}

// Send send command to batch
func (c *RedisClusterClientImpl) Send(name string, cmd string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.batches[name].Put(cmd, args...)
}

func (c *RedisClusterClientImpl) deleteBatch(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.batches, name)
}
