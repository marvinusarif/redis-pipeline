package redispipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/redis-pipeline"
	mocks "github.com/redis-pipeline/mocks"
	"github.com/stretchr/testify/mock"
)

var (
	host                string
	maxCommandsPerBatch uint64
	maxConn             int
)

var _ = Describe("Pipeline", func() {
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

var _ = Describe("Pipeline Session", func() {
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
		//
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
				"OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK",
				"OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK",
				"OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK",
			}, nil)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Millisecond)
			defer cancel()
			resp, err := pl.NewSession(ctx).
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				PushCommand("SET", "myKey", "1").
				Execute()
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
