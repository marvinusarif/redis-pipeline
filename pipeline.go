package redispipeline

import (
	"context"
	"sync"
	"time"

	redis "github.com/redis-pipeline-v2/client"
)

const (
	defaultMaxCommandPerBatch uint64 = 100
)

// RedisPipeline interface
type RedisPipeline interface {
	NewSession(ctx context.Context) RedisPipelineSession
	createListener()
}

// NewRedisPipeline initiate RedisPipeline interface
func NewRedisPipeline(client redis.RedisClient, maxCommandsPerBatch uint64) RedisPipeline {
	if maxCommandsPerBatch < 1 {
		maxCommandsPerBatch = defaultMaxCommandPerBatch
	}
	return initRedisPipeline(client, maxCommandsPerBatch)
}

// RedisPipelineImpl Implementation of RedisPipeline interface
type RedisPipelineImpl struct {
	interval            time.Duration
	client              redis.RedisClient
	maxConn             int
	maxCommandsPerBatch uint64
	sessionChan         chan *Session
	flushChan           chan []*Session
}

func initRedisPipeline(client redis.RedisClient, maxCommandsPerBatch uint64) *RedisPipelineImpl {
	rb := &RedisPipelineImpl{
		client:              client,
		interval:            time.Duration(10) * time.Millisecond,
		maxConn:             client.GetMaxConn(),
		maxCommandsPerBatch: maxCommandsPerBatch,
		sessionChan:         make(chan *Session, client.GetMaxConn()*10),
		flushChan:           make(chan []*Session, client.GetMaxConn()),
	}
	go rb.createListener()
	rb.createFlushers()
	return rb
}

func (rb *RedisPipelineImpl) createListener() {
	var commandCounter uint64
	sessions := make([]*Session, 0)
	ticker := time.NewTicker(rb.interval)
	forcedToFlush := make(chan bool)

	for {
		select {
		case newSession := <-rb.sessionChan:
			sessions = append(sessions, newSession)
			commandCounter += uint64(len(newSession.commands))
			if commandCounter >= rb.maxCommandsPerBatch {
				go rb.forceToFlush(forcedToFlush)
			}

		case <-forcedToFlush:
			if commandCounter >= rb.maxCommandsPerBatch {
				go rb.sendToFlusher(sessions)
				sessions = make([]*Session, 0)
				commandCounter = 0
			}

		case <-ticker.C:
			if len(sessions) > 0 {
				go rb.sendToFlusher(sessions)
				sessions = make([]*Session, 0)
				commandCounter = 0
			}
		}
	}
}

// NewSession Of RedisPipeline
func (rb *RedisPipelineImpl) NewSession(ctx context.Context) RedisPipelineSession {
	if ctx == nil {
		ctx = context.Background()
	}
	return &RedisPipelineSessionImpl{
		pipelineHub: rb,
		ctx:         ctx,
		session: &Session{
			status: &Status{
				mu:            &sync.Mutex{},
				shouldProcess: true,
				cancellable:   true,
			},
			responseChan: make(chan *SessionResponse, 1),
			commands:     make([]*Command, 0),
		},
	}
}

func (rb *RedisPipelineImpl) createFlushers() {
	for i := 0; i < rb.maxConn; i++ {
		go rb.newFlusher(rb.flushChan)
	}
}

func (rb *RedisPipelineImpl) newFlusher(flushChan chan []*Session) {
	for sessions := range flushChan {
		rb.flush(sessions)
	}
}

func (rb *RedisPipelineImpl) flush(sessions []*Session) {
	sentSessions := make([]*Session, 0)
	batchName := rb.client.NewBatch()
	for _, session := range sessions {
		if session.status.startProcessIfAllowed() {
			var sessErr error
			var cmdresponses []*CommandResponse
			for _, cmd := range session.commands {
				err := rb.client.Send(batchName, cmd.commandName, cmd.args...)
				if err != nil {
					sessErr = err
				}
				cmdresponses = append(cmdresponses, &CommandResponse{nil, err})
			}
			if sessErr != nil {
				go session.reply(cmdresponses, sessErr)
			} else {
				sentSessions = append(sentSessions, session)
			}
		}
	}

	if len(sentSessions) > 0 {
		reply, err := rb.client.RunBatch(batchName)
		if err != nil {
			for _, session := range sentSessions {
				go session.reply(nil, err)
			}
		} else {
			var ctr int
			for _, session := range sentSessions {
				var cmdresponses []*CommandResponse
				for _ = range session.commands {
					var resp interface{}
					resp = reply[ctr]
					ctr++
					cmdresponses = append(cmdresponses, &CommandResponse{resp, err})
				}
				go session.reply(cmdresponses, nil)
			}
		}
	}
}

func (rb *RedisPipelineImpl) forceToFlush(forcedToFlush chan bool) {
	forcedToFlush <- true
}

func (rb *RedisPipelineImpl) sendToPipelineHub(session *Session) {
	rb.sessionChan <- session
}

func (rb *RedisPipelineImpl) sendToFlusher(sessions []*Session) {
	rb.flushChan <- sessions
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
