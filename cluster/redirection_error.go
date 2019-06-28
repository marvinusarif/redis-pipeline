package cluster

import (
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
)

type RedirError struct {
	// Type indicates if the redirection is a MOVED or an ASK.
	Type string
	// NewSlot is the slot number of the redirection.
	NewSlot int
	// Addr is the node address to redirect to.
	Addr string

	raw string
}

// Error returns the error message of a RedirError. This is the
// message as received from redis.
func (e *RedirError) Error() string {
	return e.raw
}

func isRedisErr(err error, typ string) bool {
	re, ok := err.(redis.Error)
	if !ok {
		return false
	}
	parts := strings.Fields(re.Error())
	return len(parts) > 0 && parts[0] == typ
}

// ParseRedir parses err into a RedirError. If err is
// not a MOVED or ASK error or if it is nil, it returns nil.
func ParseRedir(err error) *RedirError {
	re, ok := err.(redis.Error)
	if !ok {
		return nil
	}
	parts := strings.Fields(re.Error())
	if len(parts) != 3 || (parts[0] != "MOVED" && parts[0] != "ASK") {
		return nil
	}
	slot, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil
	}
	return &RedirError{
		Type:    parts[0],
		NewSlot: slot,
		Addr:    parts[2],
		raw:     re.Error(),
	}
}
