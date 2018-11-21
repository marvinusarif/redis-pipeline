package redispipeline

import (
	"context"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	DEFAULT_MAX_INTERVAL          int    = 1000
	DEFAULT_MAX_CONN              int    = 10
	DEFAULT_MAX_COMMAND_PER_BATCH uint64 = 100000
)

type RedisPipeline interface {
	NewSession(ctx context.Context) RedisPipelineSession
}

type RedisPipelineSession interface {
	PushCommand(command string, args ...interface{}) RedisPipelineSession
	Execute() ([]*CommandResponse, error)
}

type Command struct {
	commandName string
	args        []interface{}
}

type CommandResponse struct {
	Value interface{}
	Err   error
}
type SessionResponse struct {
	CommandsResponses []*CommandResponse
	Err               error
}
type RedisPipelineSessionImpl struct {
	pipelineHub *RedisPipelineImpl
	ctx         context.Context
	session     *Session
}
type Status struct {
	mu            *sync.Mutex
	shouldProcess bool
	cancellable   bool
}
type Session struct {
	status       *Status
	responseChan chan *SessionResponse
	commands     []*Command
}

type RedisPipelineImpl struct {
	interval            time.Duration
	pool                *redigo.Pool
	maxConn             int
	maxCommandsPerBatch uint64
	sessionChan         chan *Session
	flushChan           chan []*Session
}

var once sync.Once

func NewRedisPipeline(pool *redigo.Pool, maxConn int, maxInterval int, maxCommandsPerBatch uint64) RedisPipeline {
	var rb *RedisPipelineImpl

	once.Do(func() {
		if maxInterval < 1 {
			maxInterval = DEFAULT_MAX_INTERVAL
		}
		if maxConn < 1 {
			maxConn = DEFAULT_MAX_CONN
		}
		if maxCommandsPerBatch < 1 {
			maxCommandsPerBatch = DEFAULT_MAX_COMMAND_PER_BATCH
		}

		rb = &RedisPipelineImpl{
			interval:            time.Duration(maxInterval) * time.Millisecond,
			pool:                pool,
			maxConn:             maxConn,
			maxCommandsPerBatch: maxCommandsPerBatch,
			sessionChan:         make(chan *Session),
			flushChan:           make(chan []*Session, maxConn),
		}

		rb.createFlushers()

		go func() {
			var (
				commandCounter uint64
			)
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
						//passing slice not array
						// fmt.Println("max command forced", commandCounter)
						go rb.sendToFlusher(sessions)
						sessions = make([]*Session, 0)
						commandCounter = 0
					}

				case <-ticker.C:
					if len(sessions) > 0 {
						//passing slice not array
						// fmt.Println("max command timer", commandCounter)
						go rb.sendToFlusher(sessions)
						sessions = make([]*Session, 0)
						commandCounter = 0
					}
				}
			}
		}()
	})
	return rb
}

func (rb *RedisPipelineImpl) NewSession(ctx context.Context) RedisPipelineSession {
	if ctx == nil {
		ctx = context.Background()
	}
	session := &RedisPipelineSessionImpl{
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
	return session
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
	// fmt.Println("flush")
	sentSessions := make([]*Session, 0)
	conn := rb.pool.Get()
	defer conn.Close()

	for _, session := range sessions {
		if session.status.startProcessIfAllowed() {
			var sessErr error
			var cmdresponses []*CommandResponse
			for _, cmd := range session.commands {
				err := conn.Send(cmd.commandName, cmd.args...)
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
		if err := conn.Flush(); err != nil {
			for _, session := range sentSessions {
				go session.reply(nil, err)
			}
		} else {
			for _, session := range sentSessions {
				var sessErr error
				var cmdresponses []*CommandResponse
				for _ = range session.commands {
					resp, err := conn.Receive()
					if err != nil {
						sessErr = err
					}
					cmdresponses = append(cmdresponses, &CommandResponse{resp, err})
				}
				go session.reply(cmdresponses, sessErr)
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

func (ps *RedisPipelineSessionImpl) PushCommand(command string, args ...interface{}) RedisPipelineSession {
	cmd := &Command{
		commandName: command,
		args:        args,
	}
	ps.session.commands = append(ps.session.commands, cmd)
	return ps
}

func (ps *RedisPipelineSessionImpl) Execute() ([]*CommandResponse, error) {
	go ps.pipelineHub.sendToPipelineHub(ps.session)
	return ps.waitResponse()
}

func (ps *RedisPipelineSessionImpl) waitResponse() ([]*CommandResponse, error) {
	var (
		responses []*CommandResponse
		err       error
	)

	select {
	case sessionResponse := <-ps.session.responseChan:
		responses = sessionResponse.CommandsResponses
		err = sessionResponse.Err

	case <-ps.ctx.Done():
		//if the session is still cancellable then cancel it, if not then wait until get response
		if ps.session.status.cancelProcessIfAllowed() {
			err = ps.ctx.Err()
		}
	}
	return responses, err
}
func (s *Status) getShouldProcessStat() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shouldProcess
}

func (s *Status) setNotCancellable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancellable = false
}

func (s *Status) getCancellableStat() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancellable
}

func (s *Status) setNotShouldProcess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shouldProcess = false
}

func (s *Status) startProcessIfAllowed() bool {
	if s.getShouldProcessStat() {
		s.setNotCancellable()
		return true
	}
	return false
}

func (s *Status) cancelProcessIfAllowed() bool {
	if s.getCancellableStat() {
		s.setNotShouldProcess()
		return true
	}
	return false
}
