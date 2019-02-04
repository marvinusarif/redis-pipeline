// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"
import redispipeline "github.com/redis-pipeline"

// RedisPipelineSession is an autogenerated mock type for the RedisPipelineSession type
type RedisPipelineSession struct {
	mock.Mock
}

// Execute provides a mock function with given fields:
func (_m *RedisPipelineSession) Execute() ([]*redispipeline.CommandResponse, error) {
	ret := _m.Called()

	var r0 []*redispipeline.CommandResponse
	if rf, ok := ret.Get(0).(func() []*redispipeline.CommandResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*redispipeline.CommandResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PushCommand provides a mock function with given fields: command, args
func (_m *RedisPipelineSession) PushCommand(command string, args ...interface{}) redispipeline.RedisPipelineSession {
	var _ca []interface{}
	_ca = append(_ca, command)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	var r0 redispipeline.RedisPipelineSession
	if rf, ok := ret.Get(0).(func(string, ...interface{}) redispipeline.RedisPipelineSession); ok {
		r0 = rf(command, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(redispipeline.RedisPipelineSession)
		}
	}

	return r0
}
