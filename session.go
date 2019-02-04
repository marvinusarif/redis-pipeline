package redispipeline

import (
	"context"
	"sync"
)

type RedisPipelineSession interface {
	PushCommand(command string, args ...interface{}) RedisPipelineSession
	Execute() ([]*CommandResponse, error)
}

type RedisPipelineSessionImpl struct {
	pipelineHub *RedisPipelineImpl
	ctx         context.Context
	session     *Session
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
