package rabbitevents

import "errors"

type EventProcessingError struct {
	err error
}

var ErrNilContext = errors.New("received nil context")

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
