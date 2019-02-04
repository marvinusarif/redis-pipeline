package redispipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	DEFAULT_MAX_COMMAND_PER_BATCH uint64 = 100
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
	maxCommandsPerBatch uint64
	sessionChan         chan *Session
	flushChan           chan []*Session
}

var once sync.Once

func NewRedisPipeline(host string, maxConn int, maxCommandsPerBatch uint64) RedisPipeline {
	var rb *RedisPipelineImpl

	once.Do(func() {
		if maxCommandsPerBatch < 1 {
			maxCommandsPerBatch = DEFAULT_MAX_COMMAND_PER_BATCH
		}

		pool := &redigo.Pool{
			MaxActive:   maxConn,
			MaxIdle:     maxConn,
			IdleTimeout: 8 * time.Second,
			TestOnBorrow: func(c redigo.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			Wait: true,
			Dial: func() (redigo.Conn, error) {
				c, err := redigo.Dial("tcp", host, redigo.DialConnectTimeout(5*time.Second))
				if err != nil {
					fmt.Println(err)
					return nil, err
				}
				return c, err
			},
		}

		rb = &RedisPipelineImpl{
			//must be larger than client timeout
			interval:            time.Duration(10) * time.Millisecond,
			pool:                pool,
			maxCommandsPerBatch: maxCommandsPerBatch,
			sessionChan:         make(chan *Session, pool.MaxActive*10),
			flushChan:           make(chan []*Session, pool.MaxActive),
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
	for i := 0; i < rb.pool.MaxActive; i++ {
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

	// now := time.Now()
	for _, session := range sessions {
		if session.status.startProcessIfAllowed() == true {
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
		err := conn.Flush()
		if err != nil {
			for _, session := range sentSessions {
				go session.reply(nil, err)
			}
		} else {
			for _, session := range sentSessions {
				var sessErr error
				var cmdresponses []*CommandResponse
				// go session.reply(nil, nil)
				for _ = range session.commands {
					// now = time.Now()
					resp, err := conn.Receive()
					// log.Println("time receive :", time.Since(now))
					if err != nil {
						sessErr = err
					}
					// now = time.Now()
					cmdresponses = append(cmdresponses, &CommandResponse{resp, err})
					// log.Println("time append :", time.Since(now))

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
	for {
		select {
		case <-ps.ctx.Done():
			if ps.session.status.stopProcessIfAllowed() == true {
				err = ps.ctx.Err()
				return nil, err
			}

		case sessionResponse := <-ps.session.responseChan:
			responses = sessionResponse.CommandsResponses
			err = sessionResponse.Err
			return responses, err
		}
	}
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
