package redispipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	redis "github.com/gomodule/redigo/redis"
)

var rb *RedisPipelineImpl

type RedisPipelineImpl struct {
	interval            time.Duration
	pool                *redis.Pool
	maxCommandsPerBatch uint64
	sessionChan         chan *Session
	flushChan           chan []*Session
}

func initRedisPipeline(host string, maxConn int, maxCommandsPerBatch uint64) (*RedisPipelineImpl, error) {
	once.Do(func() {
		pool := &redis.Pool{
			MaxActive:   maxConn,
			MaxIdle:     maxConn,
			IdleTimeout: 8 * time.Second,
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			Wait: true,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", host, redis.DialConnectTimeout(5*time.Second))
				if err != nil {
					fmt.Println(err)
					return nil, err
				}
				return c, err
			},
		}

		rb = &RedisPipelineImpl{
			//must be larger than client timeout
			interval:            time.Duration(20) * time.Millisecond,
			pool:                pool,
			maxCommandsPerBatch: maxCommandsPerBatch,
			sessionChan:         make(chan *Session, pool.MaxActive*10),
			flushChan:           make(chan []*Session, pool.MaxActive),
		}
		go rb.createListener()
		rb.createFlushers()
	})
	return rb, nil
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

func (rb *RedisPipelineImpl) NewSession(ctx context.Context) RedisPipelineSession {
	if ctx == nil {
		ctx = context.Background()
	}
	return &RedisPipelineSessionImpl{
		pipelineHub:        rb,
		pipelineClusterHub: nil,
		ctx:                ctx,
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
