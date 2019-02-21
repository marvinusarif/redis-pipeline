package redispipeline

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis-pipeline/client"
)

const (
	defaultMaxCommandPerBatch uint64 = 100
	defaultMaxIntervalInMs    uint64 = 10
)

// RedisPipeline interface
type RedisPipeline interface {
	NewSession(ctx context.Context) RedisPipelineSession
	Close()
}

// NewRedisPipeline initiate RedisPipeline interface
func NewRedisPipeline(client redis.RedisClient, maxIntervalInMs uint64, maxCommandsPerBatch uint64) RedisPipeline {
	if maxCommandsPerBatch < 1 {
		maxCommandsPerBatch = defaultMaxCommandPerBatch
	}
	if maxIntervalInMs < 1 {
		maxIntervalInMs = defaultMaxIntervalInMs
	}
	return initRedisPipeline(client, maxIntervalInMs, maxCommandsPerBatch)
}

// RedisPipelineImpl Implementation of RedisPipeline interface
type RedisPipelineImpl struct {
	interval            time.Duration
	client              redis.RedisClient
	maxConn             int
	maxCommandsPerBatch uint64
	nodes               map[string]*Node
}

type Node struct {
	host        string
	sessionChan chan *Session
	flushChan   chan []*Session
	termSig     chan struct{}
}

func initRedisPipeline(client redis.RedisClient, maxIntervalInMs uint64, maxCommandsPerBatch uint64) *RedisPipelineImpl {
	rb := &RedisPipelineImpl{
		client:              client,
		interval:            time.Duration(maxIntervalInMs) * time.Millisecond,
		maxConn:             client.GetMaxConn(),
		maxCommandsPerBatch: maxCommandsPerBatch,
		nodes:               make(map[string]*Node),
	}

	flushChanSize := client.GetMaxConn()
	masters, slaves := client.GetHosts()
	for _, host := range masters {
		rb.nodes[host] = &Node{
			host:        host,
			sessionChan: make(chan *Session, client.GetMaxConn()*10),
			flushChan:   make(chan []*Session, flushChanSize),
			termSig:     make(chan struct{}, 1),
		}
		//will create session buffer as many as number of nodes
		go rb.createSessionBuffer(host)
		rb.createFlushers(host)
	}
	for _, host := range slaves {
		rb.nodes[host] = &Node{
			host:        host,
			sessionChan: make(chan *Session, client.GetMaxConn()*10),
			flushChan:   make(chan []*Session, flushChanSize),
			termSig:     make(chan struct{}, 1),
		}
		//will create session buffer as many as number of nodes
		go rb.createSessionBuffer(host)
		rb.createFlushers(host)
	}
	return rb
}

/*
 every session pushed to sessionBuffer should be filtered based on
 	command can be processed with respective Node in terms of slot
  read only command/ or not
*/
func (rb *RedisPipelineImpl) createSessionBuffer(host string) {
	var commandCounter uint64
	sessions := make([]*Session, 0)
	ticker := time.NewTicker(rb.interval)
	forcedToFlush := make(chan bool)
	//should check whether it should be locked or not
	//add refresh channel
	node := rb.nodes[host]
	for {
		select {
		case newSession := <-node.sessionChan:
			sessions = append(sessions, newSession)
			commandCounter += uint64(len(newSession.commands))
			if commandCounter >= rb.maxCommandsPerBatch {
				go rb.forceToFlush(forcedToFlush)
			}

		case <-forcedToFlush:
			if commandCounter >= rb.maxCommandsPerBatch {
				go rb.sendToFlusher(host, sessions)
				sessions = make([]*Session, 0)
				commandCounter = 0
			}

		case <-ticker.C:
			if len(sessions) > 0 {
				go rb.sendToFlusher(host, sessions)
				sessions = make([]*Session, 0)
				commandCounter = 0
			}

		case <-node.termSig:
			//close all flushers by closing receiving channel
			close(node.flushChan)
			return
		}
	}
}

func (rb *RedisPipelineImpl) Close() {
	for _, node := range rb.nodes {
		//terminate all session buffer
		node.termSig <- struct{}{}
	}
}

// NewSession Of RedisPipeline
func (rb *RedisPipelineImpl) NewSession(ctx context.Context) RedisPipelineSession {
	if ctx == nil {
		ctx = context.Background()
	}
	return &RedisPipelineSessionImpl{
		pipelineHub:         rb,
		ctx:                 ctx,
		session:             make(map[string]*Session),
		sessionResponseChan: nil,
	}
}

func (rb *RedisPipelineImpl) createFlushers(host string) {
	for i := 0; i < rb.maxConn; i++ {
		go rb.newFlusher(host, rb.nodes[host].flushChan)
	}
}

func (rb *RedisPipelineImpl) newFlusher(host string, flushChan chan []*Session) {
	for sessions := range flushChan {
		rb.flush(host, sessions)
	}
}

func (rb *RedisPipelineImpl) flush(host string, sessions []*Session) {
	sentSessions := make([]*Session, 0)
	conn, isConnReadOnly, err := rb.client.GetConn(host)
	if err != nil {
		//update cluster registry has been handled in cluster library
		for _, session := range sessions {
			go session.reply(nil, err)
		}
		return
	}
	if isConnReadOnly {
		err = conn.Send("READONLY")
		if err != nil {
			//update cluster registry has been handled in cluster library
			for _, session := range sessions {
				go session.reply(nil, err)
			}
			return
		}
	}
	defer conn.Close()
	for _, session := range sessions {
		//the session should match with the connection
		if session.readOnly == false && isConnReadOnly {
			go session.reply(nil, fmt.Errorf("connection is intended only for read-only commands"))
		} else {
			if session.status.startProcessIfAllowed() {
				var sessErr error
				for _, cmd := range session.commands {
					err := conn.Send(cmd.commandName, cmd.args...)
					if err != nil {
						sessErr = err
					}
				}
				if sessErr != nil {
					go session.reply(nil, sessErr)
				} else {
					sentSessions = append(sentSessions, session)
				}
			}
		}
	}

	if len(sentSessions) > 0 {
		err := conn.Flush()
		if err != nil {
			for _, session := range sentSessions {
				go session.reply(nil, err)
			}
			return
		} else {
			for _, session := range sentSessions {
				var sessErr error
				var cmdresponses []*CommandResponse
				for _, cmd := range session.commands {
					var resp interface{}
					resp, err := conn.Receive()
					if err != nil {
						sessErr = err
						//handling redirection error
						rb.client.HandleError(err)
					} else {
						cmdresponses = append(cmdresponses, &CommandResponse{
							CommandId: cmd.commandId,
							Value:     resp,
						})
					}
				}
				go session.reply(cmdresponses, sessErr)
			}
		}
	}
}

func (rb *RedisPipelineImpl) forceToFlush(forcedToFlush chan bool) {
	forcedToFlush <- true
}

func (rb *RedisPipelineImpl) sendToPipelineHub(host string, session *Session) {
	rb.nodes[host].sessionChan <- session
}

func (rb *RedisPipelineImpl) sendToFlusher(host string, sessions []*Session) {
	rb.nodes[host].flushChan <- sessions
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
