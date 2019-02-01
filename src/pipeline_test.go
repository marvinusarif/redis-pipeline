package redispipeline_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	redis "github.com/redis-pipeline/adapter"
	. "github.com/redis-pipeline/src"
)

var (
	host                string
	maxCommandsPerBatch uint64
	maxConn             int
)

var _ = Describe("Pipeline", func() {
	Context("Redis Standalone", func() {
		BeforeEach(func() {
			host = "127.0.0.1:30008"
			maxConn = 100
			maxCommandsPerBatch = 100
		})

		It("should return RedisPipelineImpl & error", func() {
			client, err := redis.New(redis.SINGLE_MODE, host, maxConn)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
			Expect(err).To(BeNil())
		})

		It("should set maxCommandPerBatch to Minimum", func() {
			maxCommandsPerBatch = 0
			client, err := redis.New(redis.SINGLE_MODE, host, maxConn)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineImpl{}))
			Expect(err).To(BeNil())
		})
	})

	Context("Redis Cluster", func() {
		BeforeEach(func() {
			host = "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003"
			maxConn = 100
			maxCommandsPerBatch = 100
		})

		It("should return RedisPipelineClusterImpl & error", func() {
			client, err := redis.New(redis.CLUSTER_MODE, host, maxConn)
			pl := NewRedisPipeline(client, maxCommandsPerBatch)
			Expect(pl).Should(BeAssignableToTypeOf(&RedisPipelineClusterImpl{}))
			Expect(err).To(BeNil())
		})
	})
})

var _ = Describe("Session", func() {
	Describe("Pipeline Session", func() {
		Context("Redis Standalone", func() {
			BeforeEach(func() {
				host = "127.0.0.1:30008"
				maxConn = 100
				maxCommandsPerBatch = 100
			})
			It("should execute session", func() {
				client, err := redis.New(redis.SINGLE_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				pl.NewSession(nil).PushCommand("SET", "myKey", "1").Execute()
			})

			It("should force to flush", func() {
				maxCommandsPerBatch = 1
				client, err := redis.New(redis.SINGLE_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				pl.NewSession(nil).
					PushCommand("SET", "myKey", "1").
					PushCommand("SET", "myKey", "2").
					Execute()
			})

			It("should force to cancel", func() {
				maxCommandsPerBatch = 1
				client, err := redis.New(redis.SINGLE_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
				defer cancel()
				pl.NewSession(ctx).
					PushCommand("SET", "myKey", "1").
					PushCommand("SET", "myKey", "2").
					Execute()
			})
		})

		Context("Redis Cluster", func() {
			BeforeEach(func() {
				host = "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003"
				maxConn = 100
				maxCommandsPerBatch = 100
			})

			It("should execute session", func() {
				client, err := redis.New(redis.CLUSTER_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				pl.NewSession(nil).
					PushCommand("SET", "myKey", "1").
					Execute()
			})

			It("should force to flush", func() {
				maxCommandsPerBatch = 1
				client, err := redis.New(redis.CLUSTER_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				pl.NewSession(nil).
					PushCommand("SET", "myKey", "1").
					PushCommand("SET", "myKey", "2").
					Execute()
			})

			It("should force to cancel", func() {
				maxCommandsPerBatch = 1
				client, err := redis.New(redis.CLUSTER_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
				defer cancel()
				pl.NewSession(ctx).
					PushCommand("SET", "myKey", "1").
					PushCommand("SET", "myKey", "2").
					Execute()
			})

			It("should force to error", func() {
				maxCommandsPerBatch = 1
				client, err := redis.New(redis.CLUSTER_MODE, host, maxConn)
				Expect(err).To(BeNil())
				pl := NewRedisPipeline(client, maxCommandsPerBatch)
				pl.NewSession(nil).
					PushCommand("MSET", "myKey", "1").
					PushCommand("MSET", "myKey", "2").
					Execute()
			})
		})
	})
})
