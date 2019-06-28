package redisclient

import (
	"github.com/gomodule/redigo/redis"
	rclib "github.com/redis-pipeline/cluster"
)

type RedisClusterV2ClientImpl struct {
	mode    int
	host    []string
	maxConn int
	cluster rclib.ClusterInterface
}

// GetHosts get hosts of client
func (c *RedisClusterV2ClientImpl) GetHosts() ([]string, []string) {
	masters := c.cluster.GetMasterIPs()
	slaves := c.cluster.GetSlaveIPs()
	return masters, slaves
}

func (c *RedisClusterV2ClientImpl) GetMaxConn() int {
	return c.maxConn
}

func (c *RedisClusterV2ClientImpl) GetConn(host string) (conn redis.Conn, isReadOnly bool, err error) {
	conn, isReadOnly, err = c.cluster.GetConnByAddr(host)
	if err != nil {
		return nil, isReadOnly, err
	}
	return conn, isReadOnly, nil
}

func (c *RedisClusterV2ClientImpl) GetConnWithTimeout(host string) (cwt redis.ConnWithTimeout, isReadOnly bool, err error) {
	conn, isReadOnly, err := c.cluster.GetConnByAddr(host)
	cwt, ok := conn.(redis.ConnWithTimeout)
	if !ok {
		return nil, isReadOnly, errTimeoutNotSupported
	}
	return cwt, isReadOnly, nil
}

func (c *RedisClusterV2ClientImpl) GetNodeIPByKey(key string, readOnly bool) (string, error) {
	slot := c.cluster.Slot(key)
	return c.cluster.GetNodeIPBySlot(slot, readOnly)
}

func (c *RedisClusterV2ClientImpl) GetMasterFromSlaveIP(slaveIP string) (string, error) {
	return c.cluster.GetMasterFromSlaveIP(slaveIP)
}

func (c *RedisClusterV2ClientImpl) HandleError(err error) {
	c.cluster.HandleError(err)
}
