package redisadapter

import (
	"strings"
	"time"

	rc "github.com/chasex/redis-go-cluster"
	"github.com/gomodule/redigo/redis"
)

type RedisClusterClientImpl struct {
	mode    int
	host    string
	maxConn int
	cluster *rc.Cluster
}

func (c *RedisClusterClientImpl) GetMode() int {
	return c.mode
}

func (c *RedisClusterClientImpl) GetMaxConn() int {
	return c.maxConn
}

func (c *RedisClusterClientImpl) CreateCluster() error {
	var err error
	startNodes := strings.FieldsFunc(strings.Replace(c.host, " ", "", -1), func(r rune) bool {
		return r == ';' || r == ','
	})
	c.cluster, err = rc.NewCluster(&rc.Options{
		StartNodes:   startNodes,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		KeepAlive:    c.maxConn,
		AliveTime:    8 * time.Second,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *RedisClusterClientImpl) GetConn() redis.Conn {
	return nil
}

func (c *RedisClusterClientImpl) NewBatch() *rc.Batch {
	return c.cluster.NewBatch()
}

func (c *RedisClusterClientImpl) RunBatch(batch *rc.Batch) ([]interface{}, error) {
	return c.cluster.RunBatch(batch)
}
