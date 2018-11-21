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
	done        chan *SessionResponse
}
type Status struct {
	mu            *sync.RWMutex
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
			sessionChan:         make(chan *Session, (10 * int(maxCommandsPerBatch) / maxConn / maxInterval)),
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
						go rb.sendToFlusher(sessions)
						sessions = make([]*Session, 0)
						commandCounter = 0
					}

				case <-ticker.C:
					if len(sessions) > 0 {
						//passing slice not array
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
	return &RedisPipelineSessionImpl{
		pipelineHub: rb,
		ctx:         ctx,
		done:        make(chan *SessionResponse, 1),
		session: &Session{
			status: &Status{
				mu:            &sync.RWMutex{},
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
	// fmt.Println("flush")
	sentSessions := make([]*Session, 0)
	conn := rb.pool.Get()
	defer conn.Close()

	for _, session := range sessions {
		if session.status.startProcessIfAllowed() {
			var sessErr error
			cmdresponses := make([]*CommandResponse, 0)

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
		err := conn.Flush()
		if err != nil {
			for _, session := range sentSessions {
				go session.reply(nil, err)
			}
			return
		}

		for _, session := range sentSessions {
			var sessErr error
			cmdresponses := make([]*CommandResponse, 0)

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
	go ps.waitResponse()
	select {
	case resp := <-ps.done:
		return resp.CommandsResponses, resp.Err
	}
}

func (ps *RedisPipelineSessionImpl) waitResponse() {
	var (
		cmdResponses []*CommandResponse
	)
	for {
		select {
		case sessionResponse := <-ps.session.responseChan:
			ps.done <- sessionResponse
			break

		case <-ps.ctx.Done():
			if ps.session.status.stopProcessIfAllowed() {
				ps.done <- &SessionResponse{cmdResponses, ps.ctx.Err()}
				break
			}
		}
	}
}

func (s *Status) startProcessIfAllowed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getShouldProcess() {
		s.setCancellable(false)
		return true
	}
	return false
}

func (s *Status) stopProcessIfAllowed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getCancellable() {
		s.setShouldProcess(false)
		return true
	}
	return false
}

func (s *Status) getShouldProcess() bool {
	return s.shouldProcess
}
func (s *Status) setShouldProcess(stat bool) {
	s.shouldProcess = stat
}

func (s *Status) setCancellable(stat bool) {
	s.cancellable = stat
}

func (s *Status) getCancellable() bool {
	return s.cancellable
}
