package redispipeline

import (
	"fmt"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	DEFAULT_INTERVAL  int    = 1000
	DEFAULT_MAX_CONN  int    = 10
	DEFAULT_MAX_BATCH uint64 = 100000
)

type RedisPipeline interface {
	NewSession() RedisPipelineSession
}

type RedisPipelineSession interface {
	PushCommand(command string, args ...interface{}) RedisPipelineSession
	Execute() []*Response
}

type Command struct {
	responseChan chan *Response
	commandName  string
	args         []interface{}
}

type Response struct {
	Value interface{}
	Err   error
}

type RedisPipelineSessionImpl struct {
	pipelineHub  *RedisPipelineImpl
	responseChan chan *Response
	commands     []*Command
}

type RedisPipelineImpl struct {
	interval   time.Duration
	pool       *redigo.Pool
	maxConn    int
	maxBatch   uint64
	redisParam chan []*Command
	flushChan  chan []*Command
}

var once sync.Once

func NewRedisPipeline(interval int, pool *redigo.Pool, maxConn int, maxBatch uint64) RedisPipeline {
	var rb *RedisPipelineImpl

	once.Do(func() {
		if interval < 1 {
			interval = DEFAULT_INTERVAL
		}
		if maxConn < 1 {
			maxConn = DEFAULT_MAX_CONN
		}
		if maxBatch < 1 {
			maxBatch = DEFAULT_MAX_BATCH
		}
		rb = &RedisPipelineImpl{
			interval:   time.Duration(interval) * time.Millisecond,
			pool:       pool,
			maxConn:    maxConn,
			maxBatch:   maxBatch,
			redisParam: make(chan []*Command, maxBatch),
			flushChan:  make(chan []*Command, maxConn),
		}

		for i := 0; i < rb.maxConn; i++ {
			go rb.newFlusher(rb.flushChan)
		}

		go func() {
			var (
				commandCounter uint64
				redisCommands  []*Command
			)
			ticker := time.NewTicker(rb.interval)
			forcedToFlush := make(chan bool)

			for {
				select {
				case newRedisCommand := <-rb.redisParam:
					redisCommands = append(redisCommands, newRedisCommand...)
					commandCounter += uint64(len(newRedisCommand))
					if commandCounter >= rb.maxBatch {
						go rb.forceToFlush(forcedToFlush)
					}

				case <-forcedToFlush:
					if len(redisCommands) >= int(rb.maxBatch) {
						//passing slice not array
						go rb.sendToFlusher(redisCommands[0:])
						redisCommands = make([]*Command, 0)
						commandCounter = 0
					}

				case <-ticker.C:
					if len(redisCommands) > 0 {
						//passing slice not array
						go rb.sendToFlusher(redisCommands[0:])
						redisCommands = make([]*Command, 0)
						commandCounter = 0
					}
				}
			}
		}()
	})
	return rb
}

func (rb *RedisPipelineImpl) NewSession() RedisPipelineSession {
	return &RedisPipelineSessionImpl{
		pipelineHub:  rb,
		responseChan: make(chan *Response),
		commands:     make([]*Command, 0),
	}
}

func (rb *RedisPipelineImpl) newFlusher(flushChan chan []*Command) {
	for redisCommands := range flushChan {
		rb.flush(redisCommands)
	}
}

func (rb *RedisPipelineImpl) flush(redisCommands []*Command) {
	fmt.Println("flush!!")
	var sentCommands []*Command
	conn := rb.pool.Get()
	defer conn.Close()

	for _, cmd := range redisCommands {
		if err := conn.Send(cmd.commandName, cmd.args...); err != nil {
			rb.reply(cmd, nil, err)
		} else {
			sentCommands = append(sentCommands, cmd)
		}
	}

	err := conn.Flush()
	if err != nil {
		for _, cmd := range sentCommands {
			rb.reply(cmd, nil, err)
		}
	}

	for _, cmd := range sentCommands {
		resp, err := conn.Receive()
		if err != nil {
			rb.reply(cmd, nil, err)
		} else {
			rb.reply(cmd, resp, nil)
		}
	}
}

func (rb *RedisPipelineImpl) forceToFlush(forcedToFlush chan bool) {
	forcedToFlush <- true
}

func (rb *RedisPipelineImpl) sendToPipelineHub(redisParams []*Command) {
	rb.redisParam <- redisParams
}

func (rb *RedisPipelineImpl) sendToFlusher(redisCommands []*Command) {
	rb.flushChan <- redisCommands
}

func (rb *RedisPipelineImpl) reply(cmd *Command, resp interface{}, err error) {
	cmd.responseChan <- &Response{resp, nil}
}

func (ps *RedisPipelineSessionImpl) PushCommand(command string, args ...interface{}) RedisPipelineSession {
	cmd := &Command{
		commandName: command,
		args:        args,
		//copy response channel from pipeline session to command response channel - it will share the same address
		responseChan: ps.responseChan,
	}
	ps.commands = append(ps.commands, cmd)
	return ps
}

func (ps *RedisPipelineSessionImpl) Execute() []*Response {
	go ps.pipelineHub.sendToPipelineHub(ps.commands)
	return ps.waitResponse()
}

func (ps *RedisPipelineSessionImpl) waitResponse() []*Response {
	var responses []*Response
	for i := 0; i < len(ps.commands); i++ {
		responses = append(responses, <-ps.responseChan)
	}
	close(ps.responseChan)
	return responses
}
