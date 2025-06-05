package test

import (
	"fmt"

	rabbitevents "github.com/getkalido/rabbit-events"
	. "github.com/onsi/ginkgo/v2"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

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

// Namer provides consistent names for objects in each test.
type Namer struct {
	suffix string
}

func NewNamer(suffix string) *Namer {
	return &Namer{suffix: suffix}
}

func (n *Namer) Exchange() string {
	return "test-exchange-" + n.suffix
}

func (n *Namer) Topic() string {
	return "test.queue-" + n.suffix
}

func (n *Namer) Queue() string {
	return "requeue-test-" + n.suffix
}

func RabbitDo(
	cfg rabbitevents.RabbitConfig,
	fn func(conn *amqp.Channel),
) error {
	GinkgoHelper()
	conn, err := amqp.Dial(
		fmt.Sprintf(
			"amqp://%s:%s@%s/",
			cfg.GetUserName(),
			cfg.GetPassword(),
			cfg.GetHost(),
		),
	)
	if err != nil {
		return errors.Wrap(err, "rabbit: RabbitDo: Dial Failed")
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return errors.Wrap(err, "rabbit: RabbitDo: Channel Failed")
	}

	fn(ch)
	return nil
}
