// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// RedisClient is an autogenerated mock type for the RedisClient type
type RedisClient struct {
	mock.Mock
}

// GetMaxConn provides a mock function with given fields:
func (_m *RedisClient) GetMaxConn() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// NewBatch provides a mock function with given fields:
func (_m *RedisClient) NewBatch() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// RunBatch provides a mock function with given fields: _a0
func (_m *RedisClient) RunBatch(_a0 string) ([]interface{}, error) {
	ret := _m.Called(_a0)

	var r0 []interface{}
	if rf, ok := ret.Get(0).(func(string) []interface{}); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]interface{})
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Send provides a mock function with given fields: _a0, _a1, _a2
func (_m *RedisClient) Send(_a0 string, _a1 string, _a2 ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _a2...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, ...interface{}) error); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}