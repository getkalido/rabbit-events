package rabbitevents

import "errors"

var ErrNilContext = errors.New("received nil context")

type EventProcessingError struct {
	s string
}

func (e *EventProcessingError) Error() string {
	return e.s
}

func NewEventProcessingError(text string) error {
	return &EventProcessingError{text}
}

func IsEventProcessingError(err error) (bool, error) {
	innerError := errors.Unwrap(err)
	var eval *EventProcessingError
	is := errors.Is(err, eval)
	return is, innerError
}
