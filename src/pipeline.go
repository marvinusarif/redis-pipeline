package redispipeline

import (
	"context"

	redis "github.com/redis-pipeline/adapter"
)

const (
	DEFAULT_MAX_COMMAND_PER_BATCH uint64 = 100
)

type RedisPipeline interface {
	NewSession(ctx context.Context) RedisPipelineSession
	createListener()
}

func NewRedisPipeline(client redis.RedisClient, maxCommandsPerBatch uint64) RedisPipeline {
	if maxCommandsPerBatch < 1 {
		maxCommandsPerBatch = DEFAULT_MAX_COMMAND_PER_BATCH
	}
	switch client.GetMode() {
	case redis.CLUSTER_MODE:
		return initRedisPipelineCluster(client, maxCommandsPerBatch)
	default: //SINGLE_MODE
		return initRedisPipeline(client, maxCommandsPerBatch)
	}
}

func (s *Session) reply(cmdresp []*CommandResponse, err error) {
	s.responseChan <- &SessionResponse{CommandsResponses: cmdresp, Err: err}
}

func (s *Status) startProcessIfAllowed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldProcess {
		s.cancellable = false
		return true
	}
	return false
}

func (s *Status) stopProcessIfAllowed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancellable {
		s.shouldProcess = false
		return true
	}
	return false
}
