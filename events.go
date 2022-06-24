package rabbitevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
)

type ActionType string

const (
	Create               ActionType = "create"
	Update               ActionType = "update"
	Delete               ActionType = "delete"
	EventPathHeaderKey              = "path"
	EventActionHeaderKey            = "action"
	HeaderExchangeSuffix            = "headers"
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
type MultiEventEmitter func(ctx context.Context, action ActionType, context map[string][]string, old, state interface{}, paths map[string]int64) error

type Unsubscribe func()
type UnsubscribeMultiple func() error

type EventConsumer interface {
	Subscribe(ids []int64, handler func(*Event)) Unsubscribe
	SubscribeUnfiltered(handler func(*Event)) Unsubscribe
}

type MultiEventConsumer interface {
	Subscribe(paths map[string][]int64, handler func(*Event)) (UnsubscribeMultiple, error)
}

type TargetedEventConsumer func(handler func(*Event)) (func(), error)

type RabbitEventHandler interface {
	Emit(path string) EventEmitter
	EmitMultiple(paths []string) MultiEventEmitter
	Consume(path string, typer ...func() interface{}) (EventConsumer, error)
}

type ImplRabbitEventHandler struct {
	rabbitEx          RabbitExchange
	exchangeName      string
	topicExchangeName string
	prefetchCount     int
}

const rabbitPrefetchForSingleQueue = 100

func NewRabbitEventHandler(rabbitEx RabbitExchange, exchangeName string, prefetchCount int) RabbitEventHandler {
	if prefetchCount == 0 {
		prefetchCount = rabbitPrefetchForSingleQueue + runtime.NumCPU()*2
	}
	return &ImplRabbitEventHandler{
		rabbitEx:          rabbitEx,
		topicExchangeName: exchangeName,
		exchangeName:      fmt.Sprintf("%s-%s", exchangeName, HeaderExchangeSuffix),
		prefetchCount:     prefetchCount,
	}
}

func (rem *ImplRabbitEventHandler) sendEvent(
	messageSender MessageHandleFunc,
	ctx context.Context,
	action ActionType,
	context map[string][]string,
	old, state interface{},
	paths map[string]int64,
) error {
	messageHeaders := map[string]interface{}{
		// Must be explicitly made one of the types supported by AMQP, since
		// the library doesn't provide an interface to convert values to
		// supported types.
		EventActionHeaderKey: string(action),
	}
	lastPath := ""
	lastID := int64(0)
	for k, v := range paths {
		messageHeaders[k] = v
		// Needing this for wildecard matches, if we want to capture events
		// irrespective of ID
		messageHeaders[fmt.Sprintf("%s-%s", EventPathHeaderKey, k)] = true
		lastPath = k
		lastID = v
	}
	if ctx == nil {
		return ErrNilContext
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	event := Event{
		Action: action,
		Source: EventSource{
			Context: context,
		},
		ID:    lastID,
		Path:  lastPath,
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

	return messageSender(ctx, data, messageHeaders)
}

func (rem *ImplRabbitEventHandler) Emit(path string) EventEmitter {
	messageSender := rem.rabbitEx.SendTo(rem.exchangeName, ExchangeTypeHeaders, true, false, "")
	topicMessageSender := rem.rabbitEx.SendTo(rem.topicExchangeName, ExchangeTypeTopic, true, false, path)
	return func(ctx context.Context, action ActionType, context map[string][]string, id int64, old, state interface{}) error {
		err := rem.sendEvent(messageSender, ctx, action, context, old, state, map[string]int64{path: id})
		if err != nil {
			return err
		}
		return rem.sendEvent(topicMessageSender, ctx, action, context, old, state, map[string]int64{path: id})
	}
}

func (rem *ImplRabbitEventHandler) EmitMultiple(paths []string) MultiEventEmitter {
	messageSender := rem.rabbitEx.SendTo(rem.exchangeName, ExchangeTypeHeaders, true, false, "")
	topicMessageSenders := make(map[string]MessageHandleFunc)
	for _, path := range paths {
		topicMessageSenders[path] = rem.rabbitEx.SendTo(
			rem.topicExchangeName, ExchangeTypeTopic, true, false, path)
	}
	return func(ctx context.Context, action ActionType, context map[string][]string, old, state interface{}, paths map[string]int64) error {
		err := rem.sendEvent(messageSender, ctx, action, context, old, state, paths)
		if err != nil {
			return err
		}
		for path, id := range paths {
			sender, ok := topicMessageSenders[path]
			if !ok {
				continue
			}
			err = rem.sendEvent(sender, ctx, action, context, old, state, map[string]int64{path: id})
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func (rem *ImplRabbitEventHandler) Consume(path string, typer ...func() interface{}) (EventConsumer, error) {
	receive, stop, bind, unbind, err := rem.rabbitEx.ReceiveMultiple(
		ExchangeSettings{
			Name:         rem.exchangeName,
			ExchangeType: ExchangeTypeHeaders,
			Durable:      true,
			AutoDelete:   false,
			Exclusive:    false,
			NoWait:       false,
			Args:         nil,
		},
		QueueSettings{
			Name:       "",
			RoutingKey: "",
			AutoDelete: true,
			Exclusive:  false,
			Prefetch:   rem.prefetchCount,
			BindArgs: map[string]interface{}{
				"x-match": "any",
				fmt.Sprintf("%s-%s", EventPathHeaderKey, path): true,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	eo := &eventObserver{
		defaultPath: path,
		receive:     receive,
		stop:        stop,
		bind:        bind,
		unbind:      unbind,
	}
	for _, tr := range typer {
		eo.typer = tr
	}

	return eo, nil
}

func (rem *ImplRabbitEventHandler) ConsumeMultiple(typer ...func() interface{}) (MultiEventConsumer, error) {

	receive, stop, bind, unbind, err := rem.rabbitEx.ReceiveMultiple(
		ExchangeSettings{
			Name:         rem.exchangeName,
			ExchangeType: ExchangeTypeHeaders,
			Durable:      true,
			AutoDelete:   false,
			Exclusive:    false,
			NoWait:       false,
			Args:         nil,
		},
		QueueSettings{
			Name:       "",
			RoutingKey: "",
			AutoDelete: true,
			Exclusive:  true,
			Prefetch:   rem.prefetchCount,
		},
	)
	if err != nil {
		return nil, err
	}
	eo := &multiEventObserver{
		receive: receive,
		stop:    stop,
		bind:    bind,
		unbind:  unbind,
	}
	for _, tr := range typer {
		eo.typer = tr
	}

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
	typer         func() interface{}
	defaultPath   string
	receive       func(MessageHandleFunc) error
	stop          func()
	bind          BindFunc
	unbind        BindFunc
	listeners     map[string]interface{}
	listenersLock sync.Mutex
}

func (eo *eventObserver) Change(ctx context.Context, data []byte, headers map[string]interface{}, handler func(*Event)) error {
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
		e.Old = eo.typer()
	}

	err := json.Unmarshal(data, e)
	if err != nil {
		return err
	}
	handler(e)

	return nil
}

func (eo *eventObserver) Subscribe(ids []int64, handler func(*Event)) Unsubscribe {
	func() {
		eo.listenersLock.Lock()
		defer eo.listenersLock.Unlock()
		if eo.listeners == nil {
			eo.listeners = make(map[string]interface{})
		}
	}()

	listeners := make(map[string]map[string]interface{}, 0)
	// if no IDs were provided, listen for everything emitted on path
	if len(ids) == 0 {
		listenerKey := fmt.Sprintf("%s-%s", EventPathHeaderKey, eo.defaultPath)
		headers := map[string]interface{}{
			"x-match":   "any",
			listenerKey: true,
		}
		listeners[listenerKey] = headers
	}

	// Bind for every ID we need to listen for
	for _, id := range ids {
		listenerKey := fmt.Sprintf("%s.%d", eo.defaultPath, id)
		headers := map[string]interface{}{
			"x-match":      "any",
			eo.defaultPath: id,
		}
		listeners[listenerKey] = headers
	}

	for listenerKey, headers := range listeners {
		bind := func() error {
			err := eo.bind("", headers)
			if err != nil {
				return err
			}
			eo.listenersLock.Lock()
			defer eo.listenersLock.Unlock()
			eo.listeners[listenerKey] = true
			return nil
		}
		err := bind()
		if err != nil {
			panic(err)
		}
	}

	go func() {
		err := eo.receive(func(ctx context.Context, data []byte, headers map[string]interface{}) error {
			return eo.Change(ctx, data, headers, handler)
		})
		if err != nil {
			log.Printf("rabbit:eventObserver:receive Failed. Reason: %+v", err)
		}
	}()

	unsubscribe := func() {
		eo.listenersLock.Lock()
		defer eo.listenersLock.Unlock()
		for k, headers := range listeners {
			err := eo.unbind("", headers)
			if err != nil {
				panic(err)
			}
			_, ok := eo.listeners[k]
			if ok {
				delete(eo.listeners, k)
			}
		}
	}

	return unsubscribe
}

func (eo *eventObserver) SubscribeUnfiltered(handler func(*Event)) Unsubscribe {
	return eo.Subscribe(nil, handler)
}

type multiEventObserver struct {
	typer         func() interface{}
	receive       func(MessageHandleFunc) error
	stop          func()
	bind          BindFunc
	unbind        BindFunc
	listeners     map[string]interface{}
	listenersLock sync.Mutex
}

func (eo *multiEventObserver) Change(ctx context.Context, data []byte, headers map[string]interface{}, handler func(*Event)) error {
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
		e.Old = eo.typer()
	}

	err := json.Unmarshal(data, e)
	if err != nil {
		return err
	}
	handler(e)

	return nil
}

func (eo *multiEventObserver) Subscribe(
	paths map[string][]int64,
	handler func(*Event),
) (UnsubscribeMultiple, error) {
	func() {
		eo.listenersLock.Lock()
		defer eo.listenersLock.Unlock()
		if eo.listeners == nil {
			eo.listeners = make(map[string]interface{})
		}
	}()

	// Building the headers we need to bind
	listeners := make(map[string]map[string]interface{}, 0)
	for path, ids := range paths {
		// if no IDs are specified for a path, listen for all events on that path
		if len(ids) == 0 {
			listenerKey := fmt.Sprintf("%s-%s", EventPathHeaderKey, path)
			headers := map[string]interface{}{
				"x-match":   "any",
				listenerKey: true,
			}
			listeners[listenerKey] = headers
			continue
		}
		// Bind for every id we want to listen for on path.
		for _, id := range ids {
			headers := map[string]interface{}{
				"x-match": "any",
				path:      id,
			}
			listenerKey := fmt.Sprintf("%s.%d", path, id)
			listeners[listenerKey] = headers
		}
	}

	for listenerKey, headers := range listeners {
		bind := func() error {
			err := eo.bind("", headers)
			if err != nil {
				return err
			}
			eo.listenersLock.Lock()
			defer eo.listenersLock.Unlock()
			eo.listeners[listenerKey] = true
			return nil
		}
		err := bind()
		if err != nil {
			return nil, err
		}
	}

	go func() {
		err := eo.receive(func(ctx context.Context, data []byte, headers map[string]interface{}) error {
			return eo.Change(ctx, data, headers, handler)
		})
		if err != nil {
			log.Printf("rabbit:multiEventObserver:receive Failed. Reason: %+v", err)
		}
	}()

	unsubscribe := func() error {
		eo.listenersLock.Lock()
		defer eo.listenersLock.Unlock()
		for k, headers := range listeners {
			err := eo.unbind("", headers)
			if err != nil {
				return err
			}
			_, ok := eo.listeners[k]
			if ok {
				delete(eo.listeners, k)
			}
		}
		return nil
	}

	return unsubscribe, nil
}
