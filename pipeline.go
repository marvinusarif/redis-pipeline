package util

import (
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
	PushCommand(command string, args ...interface{}) (interface{}, error)
}

type Command struct {
	commandName string
	args        []interface{}
	response    chan *Response
}

type Response struct {
	value interface{}
	err   error
}

type RedisPipelineImpl struct {
	interval   time.Duration
	pool       *redigo.Pool
	maxConn    int
	maxBatch   uint64
	redisParam chan *Command
	flushChan  chan []*Command
}

var once sync.Once

func InitRedisPipeline(interval int, pool *redigo.Pool, maxConn int, maxBatch uint64) RedisPipeline {
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
			redisParam: make(chan *Command, maxBatch),
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
					redisCommands = append(redisCommands, newRedisCommand)
					commandCounter++
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

func (rb *RedisPipelineImpl) PushCommand(command string, args ...interface{}) (interface{}, error) {
	redisParam := &Command{
		commandName: command,
		args:        args,
		response:    make(chan *Response, 1),
	}
	go rb.sendToBuffer(redisParam)
	//wait until get response
	redisResponse := <-redisParam.response
	return redisResponse.value, redisResponse.err
}

func (rb *RedisPipelineImpl) newFlusher(flushChan chan []*Command) {
	for redisCommands := range flushChan {
		rb.flush(redisCommands)
	}
}

func (rb *RedisPipelineImpl) flush(redisCommands []*Command) {
	var sentCommands []*Command
	conn := rb.pool.Get()
	defer conn.Close()

	for _, cmd := range redisCommands {
		if err := conn.Send(cmd.commandName, cmd.args...); err != nil {
			go rb.reply(cmd, nil, err)
		} else {
			sentCommands = append(sentCommands, cmd)
		}
	}

	err := conn.Flush()
	if err != nil {
		for _, cmd := range sentCommands {
			go rb.reply(cmd, nil, err)
		}
	}

	for _, cmd := range sentCommands {
		resp, err := conn.Receive()
		if err != nil {
			go rb.reply(cmd, nil, err)
		} else {
			go rb.reply(cmd, resp, nil)
		}
	}
}

func (rb *RedisPipelineImpl) forceToFlush(forcedToFlush chan bool) {
	forcedToFlush <- true
}

func (rb *RedisPipelineImpl) sendToBuffer(redisParam *Command) {
	rb.redisParam <- redisParam
}

func (rb *RedisPipelineImpl) sendToFlusher(redisCommands []*Command) {
	rb.flushChan <- redisCommands
}

func (rb *RedisPipelineImpl) reply(cmd *Command, resp interface{}, err error) {
	cmd.response <- &Response{resp, nil}
}
