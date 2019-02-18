package redisclient

import (
	"errors"

	"github.com/gomodule/redigo/redis"
)

var errTimeoutNotSupported = errors.New("redis: connection does not support ConnWithTimeout")

// RedisClientImpl implementation of Redis Single Node Client
type RedisClientImpl struct {
	mode    int
	host    []string
	maxConn int
	pool    *redis.Pool
}

type batch struct {
	totalCmd int
	conn     redis.Conn
}

// GetMode get mode of client
func (c *RedisClientImpl) GetMode() int {
	return c.mode
}

// GetHosts get hosts of client
func (c *RedisClientImpl) GetHosts() ([]string, []string) {
	return c.host, nil
}

// GetMaxConn Return Redis Single Node Max Connection Pool
func (c *RedisClientImpl) GetMaxConn() int {
	return c.maxConn
}

func (c *RedisClientImpl) GetConn(host string) (conn redis.Conn, isReadOnly bool, err error) {
	//only single host - input param is neglected
	return c.pool.Get(), false, nil
}

func (c *RedisClientImpl) GetConnWithTimeout(host string) (cwt redis.ConnWithTimeout, isReadOnly bool, err error) {
	cwt, ok := c.pool.Get().(redis.ConnWithTimeout)
	if !ok {
		return nil, false, errTimeoutNotSupported
	}
	return cwt, false, nil
}

func (c *RedisClientImpl) GetNodeIPByKey(key string, readOnly bool) (string, error) {
	return c.host[0], nil
}

func (c *RedisClientImpl) GetMasterFromSlaveIP(slaveIP string) (string, error) {
	return "", errors.New("not implemented")
}

func (c *RedisClientImpl) HandleError(err error) {
	return
}
