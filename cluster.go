package redispipeline

import (
	"context"
	"strings"
	"sync"
	"time"

	rc "github.com/chasex/redis-go-cluster"
)

var rbc *RedisPipelineClusterImpl

type RedisPipelineClusterImpl struct {
	interval            time.Duration
	cluster             *rc.Cluster
	maxCommandsPerBatch uint64
	sessionChan         chan *Session
	flushChan           chan []*Session
	maxActive           int
}

type RedisPipelineClusterSessionImpl struct {
	pipelineHub *RedisPipelineClusterImpl
	ctx         context.Context
	session     *Session
}

func initRedisPipelineCluster(hosts string, maxConn int, maxCommandsPerBatch uint64) (*RedisPipelineClusterImpl, error) {
	var (
		err     error
		cluster *rc.Cluster
	)

	once.Do(func() {
		startNodes := strings.FieldsFunc(strings.Replace(hosts, " ", "", -1), func(r rune) bool {
			return r == ';' || r == ','
		})
		cluster, err = rc.NewCluster(&rc.Options{
			StartNodes:   startNodes,
			ConnTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			KeepAlive:    maxConn,
			AliveTime:    8 * time.Second,
		})
		rbc = &RedisPipelineClusterImpl{
			cluster:             cluster,
			interval:            time.Duration(10) * time.Millisecond,
			maxCommandsPerBatch: maxCommandsPerBatch,
			sessionChan:         make(chan *Session, maxConn*10),
			flushChan:           make(chan []*Session, maxConn),
			maxActive:           maxConn,
		}
		go rbc.createListener()
		rbc.createFlushers()
	})

	if err != nil {
		return nil, err
	}

	return rbc, nil
}

func (rbc *RedisPipelineClusterImpl) createListener() {
	var commandCounter uint64
	sessions := make([]*Session, 0)
	ticker := time.NewTicker(rbc.interval)
	forcedToFlush := make(chan bool)

	for {
		select {
		case newSession := <-rbc.sessionChan:
			sessions = append(sessions, newSession)
			commandCounter += uint64(len(newSession.commands))
			if commandCounter >= rbc.maxCommandsPerBatch {
				go rbc.forceToFlush(forcedToFlush)
			}

		case <-forcedToFlush:
			if commandCounter >= rbc.maxCommandsPerBatch {
				go rbc.sendToFlusher(sessions)
				sessions = make([]*Session, 0)
				commandCounter = 0
			}

		case <-ticker.C:
			if len(sessions) > 0 {
				go rbc.sendToFlusher(sessions)
				sessions = make([]*Session, 0)
				commandCounter = 0
			}
		}
	}
}

func (rbc *RedisPipelineClusterImpl) NewSession(ctx context.Context) RedisPipelineSession {
	if ctx == nil {
		ctx = context.Background()
	}
	return &RedisPipelineSessionImpl{
		pipelineHub:        nil,
		pipelineClusterHub: rbc,
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

func (rbc *RedisPipelineClusterImpl) createFlushers() {
	for i := 0; i < rbc.maxActive; i++ {
		go rbc.newFlusher(rbc.flushChan)
	}
}

func (rbc *RedisPipelineClusterImpl) newFlusher(flushChan chan []*Session) {
	for sessions := range flushChan {
		rbc.flush(sessions)
	}
}

func (rbc *RedisPipelineClusterImpl) flush(sessions []*Session) {
	sentSessions := make([]*Session, 0)
	batch := rbc.cluster.NewBatch()
	for _, session := range sessions {
		if session.status.startProcessIfAllowed() == true {
			var sessErr error
			var cmdresponses []*CommandResponse
			for _, cmd := range session.commands {
				err := batch.Put(cmd.commandName, cmd.args...)
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
		reply, err := rbc.cluster.RunBatch(batch)
		if err != nil {
			for _, session := range sentSessions {
				go session.reply(nil, err)
			}
		} else {
			for _, session := range sentSessions {
				var sessErr error
				var cmdresponses []*CommandResponse
				for _ = range session.commands {
					var resp interface{}
					resp, reply = reply[0], reply[1:]
					cmdresponses = append(cmdresponses, &CommandResponse{resp, err})
				}
				go session.reply(cmdresponses, sessErr)
			}
		}
	}
}

func (rbc *RedisPipelineClusterImpl) forceToFlush(forcedToFlush chan bool) {
	forcedToFlush <- true
}

func (rbc *RedisPipelineClusterImpl) sendToPipelineHub(session *Session) {
	rbc.sessionChan <- session
}

func (rbc *RedisPipelineClusterImpl) sendToFlusher(sessions []*Session) {
	rbc.flushChan <- sessions
}
