package rabbitevents

import "errors"

var ErrNilContext = errors.New("received nil context")

type EventProcessingError struct {
	s string
}

func (e *EventProcessingError) Error() string {
	return e.s
}

func (e *EventProcessingError) Is(tgt error) bool {
	_, ok := tgt.(*EventProcessingError)
	return ok
}

func NewEventProcessingError(text string) error {
	return &EventProcessingError{text}
}

func IsEventProcessingError(err error) bool {
	var eval *EventProcessingError
	return errors.Is(err, eval)
}
