package redispipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// RedisPipelineSession ...
type RedisPipelineSession interface {
	PushCommand(command string, args ...interface{}) RedisPipelineSession
	PushCommandReadOnly(command string, args ...interface{}) RedisPipelineSession
	Execute() ([]*CommandResponse, error)
}

// RedisPipelineSessionImpl ...
type RedisPipelineSessionImpl struct {
	pipelineHub         *RedisPipelineImpl
	ctx                 context.Context
	session             map[string]*Session
	sessionResponseChan chan *SessionResponse
	totalCmd            int
}

// Command ...
type Command struct {
	commandId   int
	commandName string
	args        []interface{}
}

// CommandResponse ...
type CommandResponse struct {
	CommandId int
	Value     interface{}
}

// SessionResponse ...
type SessionResponse struct {
	CommandsResponses []*CommandResponse
	Err               error
}

// Status ...
type Status struct {
	mu            *sync.Mutex
	shouldProcess bool
	cancellable   bool
}

// Session ...
type Session struct {
	readOnly     bool
	status       *Status
	responseChan chan *SessionResponse
	commands     []*Command
}

// PushCommand push command to session
func (ps *RedisPipelineSessionImpl) PushCommand(command string, args ...interface{}) RedisPipelineSession {
	return ps.pushCommand(false, command, args...)
}

func (ps *RedisPipelineSessionImpl) PushCommandReadOnly(command string, args ...interface{}) RedisPipelineSession {
	return ps.pushCommand(true, command, args...)
}

func (ps *RedisPipelineSessionImpl) pushCommand(readOnly bool, command string, args ...interface{}) RedisPipelineSession {
	var nodeIP string
	//give master nodes or slave node based on the nodes registry in cluster library
	nodeIP, _ = ps.pipelineHub.client.GetNodeIPByKey(args[0].(string), readOnly)
	if _, ok := ps.session[nodeIP]; !ok {
		ps.session[nodeIP] = &Session{
			readOnly: readOnly,
			status: &Status{
				mu:            &sync.Mutex{},
				shouldProcess: true,
				cancellable:   true,
			},
			//responseChannel will be set on execute since it's related to the number of nodeIP
			responseChan: nil,
			commands:     make([]*Command, 0),
		}
	}

	cmd := &Command{
		commandId:   ps.totalCmd,
		commandName: command,
		args:        args,
	}
	ps.session[nodeIP].commands = append(ps.session[nodeIP].commands, cmd)
	ps.totalCmd++
	return ps
}

// Execute all commands within session
func (ps *RedisPipelineSessionImpl) Execute() ([]*CommandResponse, error) {
	ps.sessionResponseChan = make(chan *SessionResponse, len(ps.session))

	if len(ps.session) < 1 {
		return nil, errors.New("redis pipeline session is empty")
	}

	for nodeIP, sess := range ps.session {
		//set up response channel to same with other session
		sess.responseChan = ps.sessionResponseChan
		if _, ok := ps.pipelineHub.nodes[nodeIP]; !ok {
			if sess.status.stopProcessIfAllowed() {
				go sess.reply(nil, fmt.Errorf("node %s is down", nodeIP))
			}
		} else {
			//session should be distributed to responsible node
			go ps.pipelineHub.sendToPipelineHub(nodeIP, sess)
		}
	}

	return ps.waitResponse()
}

func (ps *RedisPipelineSessionImpl) waitResponse() (responses []*CommandResponse, err error) {
	var sessionCtr int
	responses = make([]*CommandResponse, ps.totalCmd)
	for {
		select {
		case <-ps.ctx.Done():
			for nodeIP := range ps.session {
				if ps.session[nodeIP].status.stopProcessIfAllowed() {
					sessionCtr++
					err = ps.ctx.Err()
				}
				if err != nil && sessionCtr == len(ps.session) {
					return responses, err
				}
			}

		case sessionResponse := <-ps.sessionResponseChan:
			sessionCtr++
			if err == nil {
				err = sessionResponse.Err
			}
			for _, resp := range sessionResponse.CommandsResponses {
				responses[resp.CommandId] = resp
			}
			if sessionCtr == len(ps.session) {
				return responses, err
			}
		}
	}
}
