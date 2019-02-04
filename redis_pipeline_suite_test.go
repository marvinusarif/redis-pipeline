package redispipeline_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRedisPipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RedisPipeline Suite")
}
