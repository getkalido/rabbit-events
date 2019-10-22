package rabbitevents

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
)

type ActionType string

const (
	Create ActionType = "create"
	Update ActionType = "update"
	Delete ActionType = "delete"
)

type Event struct {
	Path   string
	Action ActionType
	Source EventSource
	ID     int64
	State  interface{}
}

type EventSource struct {
	Context    context.Context
	Originator string
}

type EventEmitter func(action ActionType, context context.Context, id int64, state interface{}) error

type Unsubscribe func()

type EventConsumer interface {
	Subscribe(ids []int64, handler func(*Event)) Unsubscribe
}

type TargetedEventConsumer func(handler func(*Event)) (func(), error)

type RabbitEventHandler interface {
	Emit(path string) EventEmitter
	Consume(path string, typer ...func(*interface{})) (EventConsumer, error)
}

type ImplRabbitEventHandler struct {
	rabbitEx     RabbitExchange
	exchangeName string
	stop         func()
}

func NewRabbitEventHandler(rabbitEx RabbitExchange, exchangeName string) RabbitEventHandler {
	return &ImplRabbitEventHandler{
		rabbitEx:     rabbitEx,
		exchangeName: exchangeName,
	}
}

func (rem *ImplRabbitEventHandler) Emit(path string) EventEmitter {
	return func(action ActionType, context context.Context, id int64, state interface{}) error {
		event := Event{
			Path:   path,
			Action: action,
			ID:     id,
			Source: EventSource{
				Context: context,
			},
			State: state,
		}

		callers := make([]uintptr, 30)
		numCallers := runtime.Callers(1, callers)
		if numCallers > 1 {
			frames := runtime.CallersFrames(callers)
			for {
				first, more := frames.Next()

				event.Source.Originator += first.File + ":" + fmt.Sprint(first.Line) + " " + first.Function + "\n"

				if !more {
					break
				}
			}
		}

		data, err := json.Marshal(event)

		if err != nil {
			return err
		}

		return rem.rabbitEx.SendTo(rem.exchangeName, ExchangeTypeTopic, true, false, path)(data)
	}
}

func (rem *ImplRabbitEventHandler) Consume(path string, typer ...func(*interface{})) (EventConsumer, error) {
	receive, stop, err := rem.rabbitEx.ReceiveFrom(rem.exchangeName, ExchangeTypeTopic, true, false, path, "")
	if err != nil {
		return nil, err
	}
	rem.stop = stop
	eo := &eventObserver{}
	go func() { _ = receive(eo.Change) }()

	return eo, nil
}

func Emit(rabbitIni *RabbitIni, path string, action ActionType, context context.Context, id int64, state interface{}) error {

	r := NewRabbitExchange(rabbitIni)

	defer func() { _ = r.Close() }()

	rem := NewRabbitEventHandler(r, rabbitIni.GetEventChannel())

	return rem.Emit(path)(action, context, id, state)
}

type eventObserver struct {
	lock sync.RWMutex
	//EventId -> listenerID -> listener
	listeners      map[int64]map[int64]func(*Event)
	nextListenerID int64
}

func (eo *eventObserver) Change(data []byte) error {
	e := &Event{}
	err := json.Unmarshal(data, e)
	if err != nil {
		return err
	}
	eo.lock.RLock()
	defer eo.lock.RUnlock()

	if eo.listeners == nil {
		return nil
	}

	for _, listener := range eo.listeners[e.ID] {
		//If the listeners do a lot of work ,kicking these off in goroutines might be worth
		listener(e)
	}
	return nil
}

func (eo *eventObserver) Subscribe(ids []int64, handler func(*Event)) Unsubscribe {
	eo.lock.Lock()
	defer eo.lock.Unlock()
	listenerID := eo.nextListenerID
	eo.nextListenerID++

	if eo.listeners == nil {
		eo.listeners = make(map[int64]map[int64]func(*Event))
	}

	for _, id := range ids {
		if _, ok := eo.listeners[id]; !ok {
			eo.listeners[id] = make(map[int64]func(*Event))
		}
		eo.listeners[id][listenerID] = handler
	}
	return func() {
		eo.lock.Lock()
		defer eo.lock.Unlock()
		for _, id := range ids {
			delete(eo.listeners[id], listenerID)
		}
	}

}
