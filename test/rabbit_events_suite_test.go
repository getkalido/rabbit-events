package test

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

func TestRabbitEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RabbitEvents Suite")
}

type TestError struct {
	err error
}

func NewTestError(err error) error {
	return &TestError{err}
}

func (e *TestError) Error() string {
	return e.err.Error()
}

func (e *TestError) Temporary() bool {
	return true
}

// deleteQueue relies on the presence of Ginkgo an does not work
// outside of tests.
func deleteQueue(host string, username string, password string, queueName string) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", username, password, host))
	ExpectWithOffset(1, err).To(BeNil())
	defer func() {
		defer GinkgoRecover()
		err := conn.Close()
		Expect(err).To(BeNil())
	}()
	channel, err := conn.Channel()
	ExpectWithOffset(1, err).To(BeNil())
	_, err = channel.QueueDelete(queueName, false, false, false)
	ExpectWithOffset(1, err).To(BeNil())
}
