package redispipeline_test

import (
	"context"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/redis-pipeline"
	redisclient "github.com/redis-pipeline/client"
	mocks "github.com/redis-pipeline/mocks"
	"github.com/stretchr/testify/mock"
)

var (
	masterHost          []string
	slaveHost           []string
	maxCommandsPerBatch uint64
	maxIntervalMs       uint64
	maxConn             int
	client              *mocks.RedisClient
)

var _ = Describe("[Unit Test] Pipeline", func() {

	BeforeEach(func() {
		maxConn = 100
		maxIntervalMs = 0
		maxCommandsPerBatch = 100
		client = &mocks.RedisClient{}
	})

	Context("Redis Standalone", func() {
		BeforeEach(func() {
			masterHost = []string{"127.0.0.1:30008"}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
		})
		It("should return RedisPipelineImpl & error", func() {
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			pl.Close()
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
		})

		It("should set maxCommandPerBatch to Minimum", func() {
			maxCommandsPerBatch = 0
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			pl.Close()
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
		})
	})

	Context("Redis Standalone", func() {
		BeforeEach(func() {
			masterHost = []string{"127.0.0.1:30001"}
			slaveHost = []string{"127.0.0.1:30002"}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
		})
		It("should return RedisPipelineImpl & error", func() {
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
		})

		It("should set maxCommandPerBatch to Minimum", func() {
			maxCommandsPerBatch = 0
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
		})
	})
})

var _ = Describe("[Unit Test] Pipeline Session", func() {
	var client *mocks.RedisClient
	var conn *mocks.Conn

	BeforeEach(func() {
		masterHost = []string{"127.0.0.1:30008"}
		maxConn = 100
		maxIntervalMs = 10
		maxCommandsPerBatch = 100
		client = &mocks.RedisClient{}
		conn = &mocks.Conn{}

		client.On("GetMaxConn").Return(100)
		client.On("GetHosts").Return(masterHost, slaveHost)
		client.On("GetNodeIPByKey",
			mock.AnythingOfType("string"),
			mock.AnythingOfType("bool")).
			Return(masterHost[0], nil)
			//mock conn on master
		client.On("GetConn",
			mock.AnythingOfType("string")).
			Return(conn, false, nil)
		conn.On("Close").Return(nil)
		conn.On("Send",
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string")).
			Return(nil)
		conn.On("Flush").Return(nil)
		conn.On("Receive").Return("OK", nil)
	})

	Context("Redis", func() {
		It("should execute session", func() {
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				Execute()
			Expect(resp).ShouldNot(BeNil())
			Expect(resp).Should(HaveLen(1))
			Expect(err).Should(BeNil())
		})

		It("should force to flush", func() {
			maxCommandsPerBatch = 1
			expResp := []*CommandResponse{
				&CommandResponse{CommandId: 0, Value: "OK"},
				&CommandResponse{CommandId: 1, Value: "OK"},
			}

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)

			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).ShouldNot(BeNil())
			Expect(resp).Should(HaveLen(2))
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(BeNil())
		})

		It("should force to flush", func() {
			maxCommandsPerBatch = 1
			expResp := []*CommandResponse{
				&CommandResponse{CommandId: 0, Value: "OK"},
				&CommandResponse{CommandId: 1, Value: "OK"},
			}

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)

			resp, err := pl.NewSession(nil).
				PushCommandReadOnly("GET", "myKey").
				PushCommandReadOnly("GET", "myKey").
				Execute()
			Expect(resp).ShouldNot(BeNil())
			Expect(resp).Should(HaveLen(2))
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(BeNil())
		})

		It("should force to cancel", func() {
			maxCommandsPerBatch = 1
			expResp := []*CommandResponse{nil, nil}
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
			defer cancel()
			resp, err := pl.NewSession(ctx).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).ShouldNot(BeNil())
		})

		It("should force to cancel but not cancellable", func() {
			maxCommandsPerBatch = 1
			var expResp []*CommandResponse
			for i := 0; i < 20; i++ {
				expResp = append(expResp, &CommandResponse{CommandId: i, Value: "OK"})
			}
			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2)*time.Millisecond)
			defer cancel()
			sess := pl.NewSession(ctx)
			for i := 0; i < 20; i++ {
				sess = sess.PushCommand("SET", "myKey", "2")
			}
			resp, err := sess.Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(BeNil())
		})

		It("should error on get redis Get Conn", func() {
			client = &mocks.RedisClient{}
			conn = &mocks.Conn{}
			expError := errors.New("error on redis get conn")
			expResp := []*CommandResponse{nil, nil}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
			client.On("GetNodeIPByKey",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("bool")).
				Return(masterHost[0], nil)
			client.On("GetConn", mock.AnythingOfType("string")).
				Return(conn, false, expError)

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)

			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(Equal(expError))
		})

		It("should error on redis readOnly", func() {
			client = &mocks.RedisClient{}
			conn = &mocks.Conn{}
			expError := errors.New("connection is intended only for read-only commands")
			expResp := []*CommandResponse{nil, nil}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
			client.On("GetNodeIPByKey",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("bool")).
				Return(masterHost[0], nil)
			client.On("GetConn", mock.AnythingOfType("string")).
				Return(conn, true, nil)
			conn.On("Close").Return(nil)
			conn.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(nil)
			conn.On("Flush").Return(nil)
			conn.On("Receive").Return("OK", nil)

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(Equal(expError))
		})

		It("should error on conn send", func() {
			client = &mocks.RedisClient{}
			conn = &mocks.Conn{}
			expError := errors.New("errors on send")
			expResp := []*CommandResponse{nil, nil}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
			client.On("GetNodeIPByKey",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("bool")).
				Return(masterHost[0], nil)
			client.On("GetConn", mock.AnythingOfType("string")).
				Return(conn, false, nil)
			conn.On("Close").Return(nil)
			conn.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(expError)
			conn.On("Flush").Return(nil)
			conn.On("Receive").Return("OK", nil)

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(Equal(expError))
		})

		It("should error on conn flush", func() {
			client = &mocks.RedisClient{}
			conn = &mocks.Conn{}
			expError := errors.New("errors on flush")
			expResp := []*CommandResponse{nil, nil}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
			client.On("GetNodeIPByKey",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("bool")).
				Return(masterHost[0], nil)
			client.On("GetConn", mock.AnythingOfType("string")).
				Return(conn, false, nil)
			conn.On("Close").Return(nil)
			conn.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(nil)
			conn.On("Flush").Return(expError)
			conn.On("Receive").Return("OK", nil)

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(Equal(expError))
		})

		It("should error on conn receive", func() {
			client = &mocks.RedisClient{}
			conn = &mocks.Conn{}
			expError := errors.New("errors on receive")
			expResp := []*CommandResponse{nil, nil}
			client.On("GetMaxConn").Return(100)
			client.On("GetHosts").Return(masterHost, slaveHost)
			client.On("GetNodeIPByKey",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("bool")).
				Return(masterHost[0], nil)
			client.On("GetConn", mock.AnythingOfType("string")).
				Return(conn, false, nil)
			client.On("HandleError", mock.MatchedBy(func(err error) bool {
				return err == expError
			}))
			conn.On("Close").Return(nil)
			conn.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(nil)
			conn.On("Flush").Return(nil)
			conn.On("Receive").Return(nil, expError)

			pl := NewRedisPipeline(client, maxIntervalMs, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(Equal(expError))
		})
	})
})

var _ = Describe("Integration Test Pipeline", func() {
	// if os.Getenv("integration") == "true" {
	if true {
		Context("Redis Single", func() {
			var (
				host     string
				expected []struct {
					Command string
					Args    []interface{}
				}
				singleClient redisclient.RedisClient
				pl           RedisPipeline
			)
			BeforeEach(func() {
				host = "127.0.0.1:30008"
				maxConn = 100
				maxIntervalMs = 10
				maxCommandsPerBatch = 100
				singleClient = redisclient.New(redisclient.SINGLE_MODE,
					host,
					maxConn,
					redis.DialReadTimeout(1*time.Second),
					redis.DialWriteTimeout(1*time.Second),
					redis.DialConnectTimeout(5*time.Second))
				pl = NewRedisPipeline(singleClient, maxIntervalMs, maxCommandsPerBatch)
			})

			It("populate data to redis cluster", func() {
				expected = []struct {
					Command string
					Args    []interface{}
				}{
					{
						Command: "SET",
						Args:    []interface{}{"foo", "bar"},
					}, {
						Command: "SET",
						Args:    []interface{}{"fox", "lazy"},
					}, {
						Command: "SET",
						Args:    []interface{}{"minny", "manny"},
					}, {
						Command: "SET",
						Args:    []interface{}{"moo", "minny"},
					},
				}
				sess := pl.NewSession(nil)
				for _, exp := range expected {
					sess = sess.PushCommand(exp.Command, exp.Args...)
				}
				resp, err := sess.Execute()
				Expect(resp).ShouldNot(BeNil())
				Expect(err).Should(BeNil())
			})
			//
			It("get data from redis single node", func() {
				expected = []struct {
					Command string
					Args    []interface{}
				}{
					{
						Command: "GET",
						Args:    []interface{}{"foo", "bar"},
					}, {
						Command: "GET",
						Args:    []interface{}{"fox", "lazy"},
					}, {
						Command: "GET",
						Args:    []interface{}{"minny", "manny"},
					}, {
						Command: "GET",
						Args:    []interface{}{"moo", "minny"},
					},
				}
				sess := pl.NewSession(nil)
				for _, exp := range expected {
					sess = sess.PushCommand(exp.Command, exp.Args[0])
				}
				resps, err := sess.Execute()
				Expect(resps).ShouldNot(BeNil())
				Expect(err).Should(BeNil())

				for i, resp := range resps {
					reply := resp.Value.([]byte)
					Expect(string(reply)).Should(Equal(expected[i].Args[1]))
				}
			})
		})

		Context("Redis Cluster", func() {
			var (
				expected []struct {
					Command string
					Args    []interface{}
				}
				clusterClient redisclient.RedisClient
				pl            RedisPipeline
			)

			BeforeEach(func() {
				host := "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003;127.0.0.1:30004;127.0.0.1:30005;127.0.0.1:30006"
				maxConn = 100
				maxIntervalMs = 10
				maxCommandsPerBatch = 100
				clusterClient = redisclient.New(redisclient.CLUSTER_MODE,
					host,
					maxConn,
					redis.DialReadTimeout(1*time.Second),
					redis.DialWriteTimeout(1*time.Second),
					redis.DialConnectTimeout(5*time.Second))
				pl = NewRedisPipeline(clusterClient, maxIntervalMs, maxCommandsPerBatch)
			})

			It("populate data to redis cluster", func() {
				expected = []struct {
					Command string
					Args    []interface{}
				}{
					{
						Command: "SET",
						Args:    []interface{}{"foo", "bar"},
					}, {
						Command: "SET",
						Args:    []interface{}{"fox", "lazy"},
					}, {
						Command: "SET",
						Args:    []interface{}{"minny", "manny"},
					}, {
						Command: "SET",
						Args:    []interface{}{"moo", "minny"},
					},
				}
				sess := pl.NewSession(nil)
				for _, exp := range expected {
					sess = sess.PushCommand(exp.Command, exp.Args...)
				}
				resp, err := sess.Execute()
				Expect(resp).ShouldNot(BeNil())
				Expect(err).Should(BeNil())
			})
			It("get data from redis cluster", func() {
				expected = []struct {
					Command string
					Args    []interface{}
				}{
					{
						Command: "GET",
						Args:    []interface{}{"foo", "bar"},
					}, {
						Command: "GET",
						Args:    []interface{}{"fox", "lazy"},
					}, {
						Command: "GET",
						Args:    []interface{}{"minny", "manny"},
					}, {
						Command: "GET",
						Args:    []interface{}{"moo", "minny"},
					},
				}
				sess := pl.NewSession(nil)
				for _, exp := range expected {
					sess = sess.PushCommand(exp.Command, exp.Args[0])
				}
				resps, err := sess.Execute()
				Expect(resps).ShouldNot(BeNil())
				Expect(err).Should(BeNil())
				for i, resp := range resps {
					reply := resp.Value.([]byte)
					Expect(string(reply)).Should(Equal(expected[i].Args[1]))
				}
			})

			It("get data from redis cluster read only", func() {
				expected = []struct {
					Command string
					Args    []interface{}
				}{
					{
						Command: "GET",
						Args:    []interface{}{"foo", "bar"},
					}, {
						Command: "GET",
						Args:    []interface{}{"fox", "lazy"},
					}, {
						Command: "GET",
						Args:    []interface{}{"minny", "manny"},
					}, {
						Command: "GET",
						Args:    []interface{}{"moo", "minny"},
					},
				}
				sess := pl.NewSession(nil)
				for _, exp := range expected {
					sess = sess.PushCommandReadOnly(exp.Command, exp.Args[0])
				}
				resps, err := sess.Execute()
				Expect(resps).ShouldNot(BeNil())
				Expect(err).Should(BeNil())
				for i, resp := range resps {
					reply := resp.Value.([]byte)
					Expect(string(reply)).Should(Equal(expected[i].Args[1]))
				}
			})
		})
	}
})
