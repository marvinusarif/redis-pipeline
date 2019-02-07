package redispipeline

import (
	"context"
	"sync"
)

// RedisPipelineSession ...
type RedisPipelineSession interface {
	PushCommand(command string, args ...interface{}) RedisPipelineSession
	Execute() ([]*CommandResponse, error)
}

// RedisPipelineSessionImpl ...
type RedisPipelineSessionImpl struct {
	pipelineHub *RedisPipelineImpl
	ctx         context.Context
	session     *Session
}

// Command ...
type Command struct {
	commandName string
	args        []interface{}
}

// CommandResponse ...
type CommandResponse struct {
	Value interface{}
	Err   error
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
	status       *Status
	responseChan chan *SessionResponse
	commands     []*Command
}

// PushCommand push command to session
func (ps *RedisPipelineSessionImpl) PushCommand(command string, args ...interface{}) RedisPipelineSession {
	cmd := &Command{
		commandName: command,
		args:        args,
	}
	ps.session.commands = append(ps.session.commands, cmd)
	return ps
}

// Execute all commands within session
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
			if ps.session.status.stopProcessIfAllowed() {
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
