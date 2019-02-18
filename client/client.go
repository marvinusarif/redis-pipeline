package redisclient

import (
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	rclib "github.com/redis-pipeline/cluster"
)

const (
	//CLUSTER_MODE 0
	CLUSTER_MODE int = iota
	// SINGLE_MODE 1
	SINGLE_MODE
)

// RedisClient ...
type RedisClient interface {
	GetHosts() ([]string, []string)
	GetMaxConn() int
	GetConn(host string) (redis.Conn, bool, error)
	GetConnWithTimeout(host string) (cwt redis.ConnWithTimeout, isReadOnly bool, err error)
	GetNodeIPByKey(key string, readOnly bool) (string, error)
	GetMasterFromSlaveIP(slaveIP string) (masterIP string, err error)
	HandleError(err error)
}

// New return new RedisClient
func New(mode int, host string, maxConn int, dialOption ...redis.DialOption) RedisClient {
	hosts := strings.FieldsFunc(strings.Replace(host, " ", "", -1), func(r rune) bool {
		return r == ';' || r == ','
	})
	switch mode {
	case CLUSTER_MODE:
		rc := &RedisClusterV2ClientImpl{
			mode:    mode,
			host:    hosts,
			maxConn: maxConn,
			cluster: rclib.New(hosts, maxConn, createPool)}
		return rc
	default:
		r := &RedisClientImpl{
			mode:    mode,
			host:    hosts,
			maxConn: maxConn,
			pool:    createPool(hosts[0], maxConn, dialOption...),
		}
		return r
	}
}

func createPool(host string, maxConn int, dialOption ...redis.DialOption) *redis.Pool {
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
			c, err := redis.Dial("tcp", host, dialOption...)
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			return c, err
		},
	}
}
