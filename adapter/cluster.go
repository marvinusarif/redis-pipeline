package redisadapter

import (
	"errors"
	"strings"
	"sync"
	"time"

	rc "github.com/chasex/redis-go-cluster"
	"github.com/google/uuid"
)

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

func (c *RedisClusterClientImpl) GetMaxConn() int {
	return c.maxConn
}

func (c *RedisClusterClientImpl) NewBatch() string {
	name := uuid.New().String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.batches[name]; !ok {
		c.batches[name] = c.cluster.NewBatch()
	}
	return name
}

func (c *RedisClusterClientImpl) RunBatch(name string) ([]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.batches[name]; !ok {
		return nil, errors.New("batch name not found")
	}
	return c.cluster.RunBatch(c.batches[name])
}

func (c *RedisClusterClientImpl) Send(name string, cmd string, args ...interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.batches[name].Put(cmd, args...)
}
