package test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRabbitEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RabbitEvents Suite")
}
