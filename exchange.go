package rabbitevents

import (
	"context"
	"fmt"
	"log"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const (
	maxRequeueNo            = 3
	requeueHeaderKey        = "requeue-no"
	retryQueueNameSuffix    = "retry"
	retryExchangeNameSuffix = "retry"
	retryExchangeType       = "direct"
)

type RabbitExchange interface {
	SendTo(name, exchangeType string, durable, autoDelete bool, key string) MessageHandleFunc

	ReceiveFrom(name, exchangeType string, durable, autoDelete bool, key string, clientName string) (func(MessageHandleFunc) error, func(), error)
	Receive(exchange ExchangeSettings, queue QueueSettings) (func(MessageHandleFunc) error, func(), error)

	BulkReceiveFrom(name, exchangeType string, durable, autoDelete bool, key string, clientName string, prefetchCount int, maxWait time.Duration) (func(BulkMessageHandleFunc) error, func(), error)
	BulkReceive(exchange ExchangeSettings, queue QueueSettings, maxWait time.Duration) (func(BulkMessageHandleFunc) error, func(), error)

	Close() error
}

type RabbitExchangeImpl struct {
	rabbitConnectionMutex          sync.RWMutex
	rabbitConnectionConnectTimeout chan int
	rabbitIni                      RabbitConfig
	rabbitConnection               *amqp.Connection
}

func NewRabbitExchange(rabbitIni RabbitConfig) *RabbitExchangeImpl {
	return &RabbitExchangeImpl{
		rabbitIni:                      rabbitIni,
		rabbitConnectionConnectTimeout: make(chan int, 1),
	}
}

func (re *RabbitExchangeImpl) Close() error {
	conn := re.readEventConnection()
	if conn != nil && !conn.IsClosed() {
		return conn.Close()
	}
	return nil
}

func (re *RabbitExchangeImpl) SendTo(name, exchangeType string, durable, autoDelete bool, key string) MessageHandleFunc {

	var conn *amqp.Connection
	var ch *amqp.Channel
	var version int64 = 0

	var connM sync.RWMutex
	var chM sync.RWMutex

	getConnection := func() (*amqp.Connection, error) {
		currentConn := func() *amqp.Connection {
			connM.RLock()
			defer connM.RUnlock()
			if conn != nil && !conn.IsClosed() {
				return conn
			}
			return nil
		}()

		if currentConn != nil {
			return currentConn, nil
		}

		return func() (*amqp.Connection, error) {
			connM.Lock()
			defer connM.Unlock()
			if conn == nil || conn.IsClosed() {
				var err error
				conn, err = re.newEventConnection(conn, re.rabbitIni)
				if err != nil {
					return nil, errors.Wrapf(err, "rabbit:Failed to reconnect to rabbit  %+v", err)
				}
				ch = nil

			}
			return conn, nil
		}()
	}

	getChannel := func() (*amqp.Channel, int64, error) {
		currentChannel, currentVersion := func() (*amqp.Channel, int64) {
			chM.RLock()
			defer chM.RUnlock()
			return ch, version
		}()
		if currentChannel != nil {
			return currentChannel, currentVersion, nil
		}

		return func() (*amqp.Channel, int64, error) {
			conn, err := getConnection()
			if err != nil {
				return nil, 0, err
			}

			chM.Lock()
			defer chM.Unlock()

			if ch == nil {
				var err error
				ch, err = conn.Channel()
				if err != nil {
					return nil, 0, err
				}
				version++
				err = ch.ExchangeDeclare(
					name,         // name
					exchangeType, // type
					durable,      // durable
					autoDelete,   // delete when unused
					false,        // exclusive
					false,        // no-wait
					nil,          // args
				)

				if err != nil {
					return nil, 0, err
				}
			}
			return ch, version, nil
		}()
	}

	replaceChannel := func(oldVersion int64) {
		chM.Lock()
		defer chM.Unlock()
		if oldVersion == version {
			if ch != nil {
				ch.Close()
				ch = nil
			}
		}

	}

	return func(ctx context.Context, message []byte) error {
		if ctx == nil {
			return ErrNilContext
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		var firstError error
		var try int
		for try = 1; try <= 3; try++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			ch, chVersion, err := getChannel()
			if err != nil {
				if firstError == nil {
					firstError = errors.Wrapf(err, "getChannel Failed after try %v", try)
				}
				continue
			}

			dm := amqp.Transient
			if durable {
				dm = amqp.Persistent
			}

			err = ch.Publish(
				name,  // exchange
				key,   // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					Headers:      map[string]interface{}{requeueHeaderKey: 0},
					ContentType:  "text/plain",
					Body:         message,
					DeliveryMode: dm,
				})
			if err != nil {
				if firstError == nil {
					firstError = errors.Wrapf(err, "ch.Publish Failed after try %v", try)
				}
				replaceChannel(chVersion)
				continue
			}

			return nil

		}

		return errors.Wrapf(firstError, "rabbit:Failed to send message after %v tries", try)
	}
}

func (re *RabbitExchangeImpl) ReceiveFrom(name, exchangeType string, durable, autoDelete bool, key string, clientName string) (func(MessageHandleFunc) error, func(), error) {
	return re.Receive(ExchangeSettings{
		Name:         name,
		ExchangeType: exchangeType,
		Durable:      durable,
		AutoDelete:   autoDelete,
		Exclusive:    false,
		NoWait:       false,
		Args:         nil,
	}, QueueSettings{
		Name:       clientName,
		RoutingKey: key,
		AutoDelete: true,
		Exclusive:  true,
	})
}

func (re *RabbitExchangeImpl) BulkReceiveFrom(
	name, exchangeType string,
	durable, autoDelete bool,
	key string,
	clientName string,
	prefetchCount int,
	maxWait time.Duration,
) (func(BulkMessageHandleFunc) error, func(), error) {
	return re.BulkReceive(ExchangeSettings{
		Name:         name,
		ExchangeType: exchangeType,
		Durable:      durable,
		AutoDelete:   autoDelete,
		Exclusive:    false,
		NoWait:       false,
		Args:         nil,
	}, QueueSettings{
		Name:       clientName,
		RoutingKey: key,
		AutoDelete: true,
		Exclusive:  true,
		Prefetch:   prefetchCount,
	}, maxWait)
}

type ExchangeSettings struct {
	Name         string
	ExchangeType string
	Durable      bool
	AutoDelete   bool
	Exclusive    bool
	NoWait       bool
	Args         map[string]interface{}
}

type QueueSettings struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
	BindArgs   map[string]interface{}
	RoutingKey string
	Prefetch   int
}

func (re *RabbitExchangeImpl) getChannel(
	exchange ExchangeSettings,
	queue QueueSettings,
	retryExchangeName string,
) (*amqp.Channel, <-chan amqp.Delivery, chan *amqp.Error, func(), error) {
	conn, err := re.getEventConnection()

	if err != nil {
		return nil, nil, nil, func() {}, err
	}

	if conn.IsClosed() {
		conn, err = re.newEventConnection(conn, re.rabbitIni)
		if err != nil {
			return nil, nil, nil, func() {}, err
		}
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, nil, func() {}, err
	}

	closeChan := func() {
		err = ch.Close()
		if err != nil {
			log.Printf("rabbit:ProcessMessage:Consume Close Channel Failed. Reason: %+v", err)
		}
	}

	if queue.Prefetch == 0 {
		queue.Prefetch = runtime.NumCPU() * 2
	}
	err = ch.Qos(queue.Prefetch, 0, false)
	if err != nil {
		log.Printf("rabbit:ProcessMessage:Consume Qos. Reason: %+v", err)
	}

	err = ch.ExchangeDeclare(
		exchange.Name,         // name
		exchange.ExchangeType, // type
		exchange.Durable,      // durable
		exchange.AutoDelete,   // delete when unused
		exchange.Exclusive,    // exclusive
		exchange.NoWait,       // no-wait
		exchange.Args,         // args
	)

	if err != nil {
		return nil, nil, nil, closeChan, err
	}

	err = ch.ExchangeDeclare(
		retryExchangeName, // name
		retryExchangeType, // type
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // args
	)

	if err != nil {
		return nil, nil, nil, closeChan, err
	}

	q, err := ch.QueueDeclare(
		queue.Name,       // name
		queue.Durable,    // durable
		queue.AutoDelete, // delete when unused
		queue.Exclusive,  // exclusive
		queue.NoWait,     // no-wait
		queue.Args,       // args
	)

	if err != nil {
		log.Printf("rabbit:ProcessMessage:QueueDeclare Failed. Reason: %+v", err)
		return nil, nil, nil, closeChan, err
	}

	err = ch.QueueBind(
		q.Name,           // queue name
		queue.RoutingKey, // routing key
		exchange.Name,    // exchange name
		queue.NoWait,     // no-wait
		queue.BindArgs,   //args
	)

	if err != nil {
		log.Printf("rabbit:ProcessMessage:QueueBind Failed. Reason: %+v", err)
		return nil, nil, nil, closeChan, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	if err != nil {
		log.Printf("rabbit:ProcessMessage:Consume Failed. Reason: %+v", err)

		return nil, nil, nil, closeChan, err
	}

	//We buffer the errors, so that when we are not processing the error message (like while shutting down) the channel can still close.
	errChan := make(chan *amqp.Error, 1)
	ch.NotifyClose(errChan)

	return ch, msgs, errChan, closeChan, nil
}

func (re *RabbitExchangeImpl) Receive(exchange ExchangeSettings, queue QueueSettings) (func(MessageHandleFunc) error, func(), error) {
	retryExchangeName := fmt.Sprintf("%s-%s", exchange.Name, retryExchangeNameSuffix)

	channel, msgs, errChan, closer, err := re.getChannel(exchange, queue, retryExchangeName)
	if err != nil {
		return nil, nil, err
	}

	stop := make(chan struct{})
	return func(handler MessageHandleFunc) error {
			defer closer()
			for {
				// If the `msgs` channel is no longer valid, then we need to open a new one
				// If that attempt fails, the channel will remain invalid, so we will try again, until we succeed
				if msgs == nil {
					channel, msgs, errChan, closer, err = re.getChannel(exchange, queue, retryExchangeName)
					if err != nil {
						log.Printf("Cannot get a new channel, after message chanel got closed %+v\n", err)
						time.Sleep(time.Second)
					}
				}
				// If `msgs` is still empty, we need to retry, so we loop
				if msgs == nil {
					continue
				}

				select {
				case m, ok := <-msgs:
					//if the channel is closed, we want to stop receiving on this one, and we need to open a new one
					if !ok {
						msgs = nil
						continue
					}
					go func(m amqp.Delivery) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						err := handler(ctx, m.Body)
						if err != nil {
							log.Printf("Error handling rabbit message Exchange: %s Queue: %s Body: [%s] %+v\n", exchange.Name, queue.Name, m.Body, err)
							isProcessingError := IsTemporaryError(err)
							err = m.Nack(false, false)
							if err != nil {
								log.Printf("Error Nack rabbit message %+v\n", err)
							}
							if isProcessingError {
								requeuesObj, ok := m.Headers[requeueHeaderKey]
								if !ok {
									return
								}
								noRequeues, ok := requeuesObj.(int32)
								if ok && noRequeues < maxRequeueNo {
									noRequeues++
									err = requeueMessage(
										channel,
										queue,
										exchange.Name,
										retryExchangeName,
										m.Body,
										int(noRequeues),
									)
									if err != nil {
										log.Printf("Error requeueing message %+v\n", err)
									}
								}
							}
							return
						}
						err = m.Ack(false)
						if err != nil {
							log.Printf("Error Ack rabbit message %+v\n", err)
							return
						}
					}(m)

				case <-stop:
					return nil

				case e := <-errChan:
					// Something went wrong
					if e == nil {
						continue
					}
					log.Printf("rabbit:ProcessMessages Rabbit Failed: %d - %s", e.Code, e.Reason)

					if e.Code == amqp.ConnectionForced || e.Code == amqp.FrameError {
						// for now only care about total connection loss
						closer()
						for {
							time.Sleep(time.Millisecond * 250)
							channel, msgs, errChan, closer, err = re.getChannel(exchange, queue, retryExchangeName)
							if err != nil {
								log.Printf("Error connecting to rabbit %+v\n", err)
							} else {
								break
							}
						}
					}

				}
			}
		},
		func() {
			close(stop)
		}, nil
}

func (re *RabbitExchangeImpl) BulkReceive(exchange ExchangeSettings, queue QueueSettings, maxWait time.Duration) (func(BulkMessageHandleFunc) error, func(), error) {
	retryExchangeName := fmt.Sprintf("%s-%s", exchange.Name, retryExchangeNameSuffix)

	channel, msgs, errChan, closer, err := re.getChannel(exchange, queue, retryExchangeName)
	if err != nil {
		return nil, nil, err
	}

	stop := make(chan struct{})
	return func(handler BulkMessageHandleFunc) error {
			defer closer()
			batch := make([]amqp.Delivery, 0, queue.Prefetch)
			// TODO: Inject the NewTimer function into the service to enable us to mock it during testing.
			fillWaitTimer := time.NewTimer(maxWait)
			handleMessages := func(messages []amqp.Delivery) {
				if len(messages) == 0 {
					return
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				errors := handler(ctx, messages)
				erroredMessages := make(map[string]struct{})
				if len(errors) > 0 {
					for _, msgError := range errors {
						m := msgError.message
						err := msgError.err
						erroredMessages[m.MessageId] = struct{}{}
						log.Printf("Error handling rabbit message Exchange: %s Queue: %s Body: [%s] %+v\n", exchange.Name, queue.Name, m.Body, err)
						isProcessingError := IsTemporaryError(err)
						err = m.Nack(false, false)
						if err != nil {
							log.Printf("Error Nack rabbit message %+v\n", err)
						}
						if isProcessingError {
							requeuesObj, ok := m.Headers[requeueHeaderKey]
							if !ok {
								continue
							}
							noRequeues, ok := requeuesObj.(int32)
							if ok && noRequeues < maxRequeueNo {
								noRequeues++
								err = requeueMessage(
									channel,
									queue,
									exchange.Name,
									retryExchangeName,
									m.Body,
									int(noRequeues),
								)
								if err != nil {
									log.Printf("Error requeueing message %+v\n", err)
								}
							}
						}
					}
					for _, m := range messages {
						if _, ok := erroredMessages[m.MessageId]; !ok {
							m.Ack(false)
						}
					}
				} else {
					// If none of the messages resulted in errors, ack multiple (the whole prefetched batch)
					// From rabbit docs: When multiple is true, this delivery and all prior unacknowledged deliveries
					// on the same channel will be acknowledged. This is useful for batch processing
					// of deliveries.
					err = messages[len(messages)-1].Ack(true)
					if err != nil {
						log.Printf("Error Ack rabbit message %+v\n", err)
						return
					}
				}
			}
			for {
				// If the `msgs` channel is no longer valid, then we need to open a new one
				// If that attempt fails, the channel will remain invalid, so we will try again, until we succeed
				if msgs == nil {
					channel, msgs, errChan, closer, err = re.getChannel(exchange, queue, retryExchangeName)
					if err != nil {
						log.Printf("Cannot get a new channel, after message chanel got closed %+v\n", err)
						time.Sleep(time.Second)
					}
				}
				// If `msgs` is still empty, we need to retry, so we loop
				if msgs == nil {
					continue
				}
				select {
				case m, ok := <-msgs:
					// if the channel is closed, we want to stop receiving on this one, and we need to open a new one
					if !ok {
						msgs = nil
						continue
					}
					batch = append(batch, m)
					// TODO: Disociate batch size from the prefetch count so that we can prefetch new messages
					// while processing the batch. This will require a rework of the current recieve pattern.
					if len(batch) == queue.Prefetch {
						batchCopy := batch
						go handleMessages(batchCopy)
						batch = make([]amqp.Delivery, 0, queue.Prefetch)
						fillWaitTimer.Reset(maxWait)
					}
				case <-fillWaitTimer.C:
					if len(batch) > 0 {
						batchCopy := batch
						go handleMessages(batchCopy)
						batch = make([]amqp.Delivery, 0, queue.Prefetch)
					}
					fillWaitTimer.Reset(maxWait)
				case <-stop:
					return nil

				case e := <-errChan:
					// Something went wrong
					if e == nil {
						continue
					}
					log.Printf("rabbit:ProcessMessages Rabbit Failed: %d - %s", e.Code, e.Reason)

					if e.Code == amqp.ConnectionForced || e.Code == amqp.FrameError {
						// for now only care about total connection loss
						closer()
						for {
							time.Sleep(time.Millisecond * 250)
							channel, msgs, errChan, closer, err = re.getChannel(exchange, queue, retryExchangeName)
							if err != nil {
								log.Printf("Error connecting to rabbit %+v\n", err)
							} else {
								break
							}
						}
					}

				}
			}
		},
		func() {
			close(stop)
		}, nil
}

func requeueMessage(
	channel *amqp.Channel,
	queue QueueSettings,
	exchangeName string,
	retryExchangeName string,
	body []byte,
	noRequeues int,
) error {
	expiration := int(math.Pow(10.0, float64(noRequeues)) * 1000)
	expirationStr := fmt.Sprintf("%v", expiration)
	retryQueueName := getRequeueQueueName(queue.Name, expirationStr)
	requeueQueue, err := channel.QueueDeclare(
		retryQueueName,   // name
		queue.Durable,    // durable
		queue.AutoDelete, // delete when unused
		false,            // exclusive
		queue.NoWait,     // no-wait
		map[string]interface{}{
			"x-dead-letter-exchange":    exchangeName,
			"x-dead-letter-routing-key": queue.RoutingKey,
			"x-message-ttl":             expiration,
		}, // args
	)

	if err != nil {
		log.Printf("rabbit:requeueMessage:QueueDeclare Failed. Reason: %+v", err)
		return err
	}

	err = channel.QueueBind(
		requeueQueue.Name, // queue name
		requeueQueue.Name, // routing key
		retryExchangeName, // exchange name
		queue.NoWait,      // no-wait
		queue.Args,        // args
	)

	if err != nil {
		log.Printf("rabbit:requeueMessage:QueueBind Failed. Reason: %+v", err)
		return err
	}

	err = channel.Publish(
		retryExchangeName,
		retryQueueName, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			Headers:      map[string]interface{}{requeueHeaderKey: noRequeues},
			ContentType:  "text/plain",
			Body:         body,
			DeliveryMode: amqp.Transient,
		})

	return err
}

func getRequeueQueueName(queueName string, expiry string) string {
	return fmt.Sprintf("%s-%s-%s", queueName, retryQueueNameSuffix, expiry)
}

/*
 getEventConnection get the connection, creating if not exists
*/
func (re *RabbitExchangeImpl) getEventConnection() (*amqp.Connection, error) {
	ec := re.readEventConnection()
	if ec != nil {
		return ec, nil
	}
	return re.newEventConnection(ec, re.rabbitIni)

}

func (re *RabbitExchangeImpl) readEventConnection() *amqp.Connection {
	re.rabbitConnectionMutex.RLock()
	defer re.rabbitConnectionMutex.RUnlock()
	return re.rabbitConnection
}

/*
 newEventConnection creates new connection to rabbit and sets re.rabbitConnection

 It uses rabbitConnectionConnectTimeout as a semaphore with timeout to prevent many go routines waiting to try to connect.
 It still needs to lock rabbitConnectionMutex that is used for faster read access
*/
func (re *RabbitExchangeImpl) newEventConnection(old *amqp.Connection, rabbitIni RabbitConfig) (*amqp.Connection, error) {

	timer := time.NewTimer(rabbitIni.GetConnectTimeout())
	defer timer.Stop()
	select {
	case re.rabbitConnectionConnectTimeout <- 0:
		{
			defer func() { <-re.rabbitConnectionConnectTimeout }()

			current := re.readEventConnection()
			if current != old {
				return current, nil
			}

			re.rabbitConnectionMutex.Lock()
			defer re.rabbitConnectionMutex.Unlock()

			conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", rabbitIni.GetUserName(), rabbitIni.GetPassword(), rabbitIni.GetHost()))

			if err != nil {
				return nil, err
			}

			go func() {
				for blocked := range conn.NotifyBlocked(make(chan amqp.Blocking)) {
					if blocked.Active {
						log.Printf("rabbit:eventConnection server is blocked because %s", blocked.Reason)
					}
				}
			}()

			re.rabbitConnection = conn
			return re.rabbitConnection, nil
		}
	case <-timer.C:
		{
			return nil, errors.New("Timout waiting to start connection")
		}
	}
}

func Fanout(listen func(MessageHandleFunc) error) (func(MessageHandleFunc) func(), error) {
	listeners := make(map[int64]MessageHandleFunc)
	lock := sync.RWMutex{}
	var counter int64 = 0

	err := listen(func(ctx context.Context, message []byte) error {
		if ctx == nil {
			return ErrNilContext
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lock.RLock()
		defer lock.RUnlock()
		for _, listener := range listeners {
			err := listener(ctx, message)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return func(listenInstance MessageHandleFunc) func() {
		lock.Lock()
		defer lock.Unlock()
		counter++
		listenNumber := counter

		listeners[listenNumber] = listenInstance
		return func() {
			lock.Lock()
			defer lock.Unlock()
			delete(listeners, listenNumber)
		}
	}, nil
}
