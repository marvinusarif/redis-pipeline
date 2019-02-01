package redispipeline

import (
	"context"
	"sync"
)

const (
	DEFAULT_MAX_COMMAND_PER_BATCH uint64 = 100
	CLUSTER_MODE                  int    = iota
	SINGLE_MODE
)

type RedisPipeline interface {
	NewSession(ctx context.Context) RedisPipelineSession
	createListener()
}

var once sync.Once

func NewRedisPipeline(pipelineMode int, host string, maxConn int, maxCommandsPerBatch uint64) (RedisPipeline, error) {
	if maxCommandsPerBatch < 1 {
		maxCommandsPerBatch = DEFAULT_MAX_COMMAND_PER_BATCH
	}
	switch pipelineMode {
	case CLUSTER_MODE:
		return initRedisPipelineCluster(host, maxConn, maxCommandsPerBatch)
	default: //SINGLE_MODE
		return initRedisPipeline(host, maxConn, maxCommandsPerBatch)
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
