package rabbitevents

import (
	"errors"

	"github.com/streadway/amqp"
)

type EventProcessingError struct {
	err error
}

var ErrNilContext = errors.New("received nil context")

var ErrExchangeClosed = errors.New("exchange has been closed")

func NewEventProcessingError(err error) error {
	return &EventProcessingError{err}
}

func (e *EventProcessingError) Error() string {
	return e.err.Error()
}

func (e *EventProcessingError) Temporary() bool {
	return true
}

func IsTemporaryError(err error) bool {
	//If the eror was created by status.Errorf, then use that code.
	if err, ok := err.(interface {
		Temporary() bool
	}); ok {
		return err.Temporary()
	}
	nestedErr := errors.Unwrap(err)
	if nestedErr != nil {
		return IsTemporaryError(nestedErr)
	}
	return false
}

type MessageError struct {
	err     error
	message amqp.Delivery
}

func NewMessageError(message amqp.Delivery, err error) *MessageError {
	return &MessageError{err: err, message: message}
}

func (me *MessageError) Error() string {
	return me.err.Error()
}

func (me *MessageError) Unwrap() error {
	return me.err
}

func (me *MessageError) GetMessage() amqp.Delivery {
	return me.message
}
