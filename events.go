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
	Old    interface{}
	State  interface{}
}

type EventSource struct {
	Context    map[string][]string
	Originator string
}

type EventEmitter func(ctx context.Context, action ActionType, context map[string][]string, id int64, old, state interface{}) error

type Unsubscribe func()

type EventConsumer interface {
	Subscribe(ids []int64, handler func(*Event)) Unsubscribe
	SubscribeUnfiltered(handler func(*Event)) Unsubscribe
}

type TargetedEventConsumer func(handler func(*Event)) (func(), error)

type RabbitEventHandler interface {
	Emit(path string) EventEmitter
	Consume(path string, typer ...func() interface{}) (EventConsumer, error)
}

type ImplRabbitEventHandler struct {
	rabbitEx      RabbitExchange
	exchangeName  string
	prefetchCount int
	stop          func()
}

const rabbitPrefetchForSingleQueue = 100

func NewRabbitEventHandler(rabbitEx RabbitExchange, exchangeName string, prefetchCount int) RabbitEventHandler {
	if prefetchCount == 0 {
		prefetchCount = rabbitPrefetchForSingleQueue + runtime.NumCPU()*2
	}
	return &ImplRabbitEventHandler{
		rabbitEx:      rabbitEx,
		exchangeName:  exchangeName,
		prefetchCount: prefetchCount,
	}
}

func (rem *ImplRabbitEventHandler) Emit(path string) EventEmitter {
	messageSender := rem.rabbitEx.SendTo(rem.exchangeName, ExchangeTypeTopic, true, false, path)
	return func(ctx context.Context, action ActionType, context map[string][]string, id int64, old, state interface{}) error {
		if ctx == nil {
			return ErrNilContext
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		event := Event{
			Path:   path,
			Action: action,
			ID:     id,
			Source: EventSource{
				Context: context,
			},
			Old:   old,
			State: state,
		}

		callers := make([]uintptr, 30)
		numCallers := runtime.Callers(2, callers)
		if numCallers > 1 {
			frames := runtime.CallersFrames(callers)
			first, _ := frames.Next()
			event.Source.Originator += first.File + ":" + fmt.Sprint(first.Line) + " " + first.Function + "\n"
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		data, err := json.Marshal(event)

		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		return messageSender(ctx, data)
	}
}

func (rem *ImplRabbitEventHandler) Consume(path string, typer ...func() interface{}) (EventConsumer, error) {
	receive, stop, err := rem.rabbitEx.Receive(ExchangeSettings{
		Name:         rem.exchangeName,
		ExchangeType: ExchangeTypeTopic,
		Durable:      true,
		AutoDelete:   false,
		Exclusive:    false,
		NoWait:       false,
		Args:         nil,
	}, QueueSettings{
		Name:       "",
		RoutingKey: path,
		AutoDelete: true,
		Exclusive:  true,
		Prefetch:   rem.prefetchCount,
	})
	if err != nil {
		return nil, err
	}
	rem.stop = stop
	eo := &eventObserver{}
	for _, tr := range typer {
		eo.typer = tr
	}
	go func() { _ = receive(eo.Change) }()

	return eo, nil
}

func Emit(ctx context.Context, rabbitIni *RabbitIni, path string, action ActionType, context map[string][]string, id int64, old, state interface{}) error {
	if ctx == nil {
		return ErrNilContext
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	r := NewRabbitExchange(rabbitIni)

	defer func() { _ = r.Close() }()

	rem := NewRabbitEventHandler(r, rabbitIni.GetEventChannel(), 0)

	return rem.Emit(path)(ctx, action, context, id, old, state)
}

type eventObserver struct {
	lock sync.RWMutex
	//EventId -> listenerID -> listener
	listeners            map[int64]map[int64]func(*Event)
	nextListenerID       int64
	typer                func() interface{}
	nextGlobalListenerID int64
	globalListeners      map[int64]func(*Event) //This is a default listener for all events
}

func (eo *eventObserver) Change(ctx context.Context, data []byte) error {
	if ctx == nil {
		return ErrNilContext
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if data == nil {
		return nil
	}

	e := &Event{}

	if eo.typer != nil {
		e.State = eo.typer()
	}

	err := json.Unmarshal(data, e)
	if err != nil {
		return err
	}
	eo.lock.RLock()
	defer eo.lock.RUnlock()

	for _, listener := range eo.listeners[e.ID] {
		//If the listeners do a lot of work ,kicking these off in goroutines might be worth
		listener(e)
	}

	for _, globalListener := range eo.globalListeners {
		globalListener(e)
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

func (eo *eventObserver) SubscribeUnfiltered(handler func(*Event)) Unsubscribe {
	eo.lock.Lock()
	defer eo.lock.Unlock()

	listenerID := eo.nextGlobalListenerID
	eo.nextGlobalListenerID++

	eo.globalListeners = make(map[int64]func(*Event))

	eo.globalListeners[listenerID] = handler
	return func() {
		eo.lock.Lock()
		defer eo.lock.Unlock()
		delete(eo.globalListeners, listenerID)
	}
}
