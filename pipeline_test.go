package redispipeline_test

import (
	"context"
	"errors"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/redis-pipeline"
	redisClient "github.com/redis-pipeline/client"
	mocks "github.com/redis-pipeline/mocks"
	"github.com/stretchr/testify/mock"
)

var (
	host                string
	maxCommandsPerBatch uint64
	maxConn             int
)

var _ = Describe("[Unit Test] Pipeline", func() {
	var client *mocks.RedisClient
	BeforeEach(func() {
		host = "127.0.0.1:30008"
		maxConn = 100
		maxCommandsPerBatch = 100
		client = &mocks.RedisClient{}
		client.On("GetMaxConn").Return(100)
	})

	Context("Redis Standalone/ Cluster", func() {
		It("should return RedisPipelineImpl & error", func() {
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
		})

		It("should set maxCommandPerBatch to Minimum", func() {
			maxCommandsPerBatch = 0
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
		})
	})
})

var _ = Describe("[Unit Test] Pipeline Session", func() {
	var client *mocks.RedisClient
	BeforeEach(func() {
		host = "127.0.0.1:30008"
		maxConn = 100
		maxCommandsPerBatch = 100
		client = &mocks.RedisClient{}
		client.On("GetMaxConn").Return(100)
	})

	Context("Redis", func() {
		It("should execute session", func() {
			client.On("NewBatch").
				Return("new-batch-mock")
			client.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(nil)
			client.On("RunBatch",
				mock.AnythingOfType("string")).
				Return([]interface{}{
					"OK",
				}, nil)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
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
				&CommandResponse{Value: "OK", Err: nil},
				&CommandResponse{Value: "OK", Err: nil},
			}
			client.On("NewBatch").
				Return("new-batch-mock")
			client.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(nil)
			client.On("RunBatch",
				mock.AnythingOfType("string")).
				Return([]interface{}{
					"OK", "OK",
				}, nil)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).ShouldNot(BeNil())
			Expect(resp).Should(HaveLen(2))
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(BeNil())
		})

		It("should force to cancel", func() {
			maxCommandsPerBatch = 1
			client.On("NewBatch").
				Return("new-batch-mock")
			client.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).Return(nil)
			client.On("RunBatch", mock.AnythingOfType("string")).Return([]interface{}{
				"OK", "OK",
			}, nil)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
			defer cancel()
			resp, err := pl.NewSession(ctx).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(BeNil())
			Expect(err).ShouldNot(BeNil())
		})

		It("should force to proceed session", func() {
			maxCommandsPerBatch = 1
			client.On("NewBatch").
				Return("new-batch-mock")
			client.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).Return(nil)
			client.On("RunBatch", mock.AnythingOfType("string")).Return([]interface{}{
				"OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK",
				"OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK",
				"OK", "OK", "OK", "OK", "OK",
			}, nil)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Millisecond)
			defer cancel()
			sess := pl.NewSession(ctx)
			for i := 0; i < 25; i++ {
				sess = sess.PushCommand("SET", "myKey", "1")
			}
			resp, err := sess.Execute()
			Expect(resp).ShouldNot(BeNil())
			Expect(err).Should(BeNil())
		})

		It("should error on redis send", func() {
			maxCommandsPerBatch = 1
			expError := errors.New("error on send")
			expResp := []*CommandResponse{
				&CommandResponse{Value: nil, Err: expError},
				&CommandResponse{Value: nil, Err: expError},
			}
			client.On("NewBatch").
				Return("new-batch-mock")
			client.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(expError)
			client.On("RunBatch", mock.AnythingOfType("string")).Return([]interface{}{
				"OK", "OK",
			}, nil)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(Equal(expResp))
			Expect(err).Should(Equal(expError))
		})

		It("should error on redis run batch", func() {
			maxCommandsPerBatch = 1
			expError := errors.New("error on running batch")
			client.On("NewBatch").
				Return("new-batch-mock")
			client.On("Send",
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string"),
				mock.AnythingOfType("string")).
				Return(nil)
			client.On("RunBatch", mock.AnythingOfType("string")).Return([]interface{}{}, expError)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			resp, err := pl.NewSession(nil).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "2").
				Execute()
			Expect(resp).Should(BeNil())
			Expect(err).Should(Equal(expError))
		})
	})
})

var _ = Describe("Integration Test Pipeline", func() {
	if os.Getenv("integration") == "true" {
		// if true {
		Context("Redis Single", func() {
			var (
				expected []struct {
					Command string
					Args    []interface{}
				}
				singleClient redisClient.RedisClient
				pl           RedisPipeline
			)
			BeforeEach(func() {
				host = "127.0.0.1:30008"
				singleClient = redisClient.New(redisClient.SINGLE_MODE, host, maxConn)
				pl = NewRedisPipeline(singleClient, maxCommandsPerBatch)
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
				clusterClient redisClient.RedisClient
				pl            RedisPipeline
			)

			BeforeEach(func() {
				host := "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003"
				clusterClient = redisClient.New(redisClient.CLUSTER_MODE, host, maxConn)
				pl = NewRedisPipeline(clusterClient, maxCommandsPerBatch)
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
		})
	}
})
