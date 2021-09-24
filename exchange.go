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
	return func(ctx context.Context, message []byte) error {
		if ctx == nil {
			return ErrNilContext
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn, err := re.getEventConnection()
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		var firstError error
		var ch *amqp.Channel
		var try int
		for try = 1; try <= 3; try++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if conn.IsClosed() {
				conn, err = re.newEventConnection(conn, re.rabbitIni)
				if err != nil {
					return errors.Wrapf(err, "rabbit:Failed to reconnect to rabbit after %v tries, first failing to %+v", try, err)
				}
			}

			ch, err = conn.Channel()
			if err != nil {
				if firstError == nil {
					firstError = errors.Wrapf(err, "conn.Channel Failed after try %v", try)
				}
				continue
			}

			defer func() {
				if ch != nil {
					err := ch.Close()
					if err != nil {
						log.Printf("rabbit:Emit Close Channel Failed %+v", err)
					}
				}
			}()

			if ctx.Err() != nil {
				return ctx.Err()
			}

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
				if firstError == nil {
					firstError = errors.Wrapf(err, "ch.ExchangeDeclare Failed after try %v", try)
				}
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
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

func (re *RabbitExchangeImpl) Receive(exchange ExchangeSettings, queue QueueSettings) (func(MessageHandleFunc) error, func(), error) {
	getChannel := func() (*amqp.Channel, <-chan amqp.Delivery, chan *amqp.Error, func(), error) {
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

		retryExchangeName := fmt.Sprintf("%s-%s", exchange.Name, retryExchangeNameSuffix)
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

		for i := 0; i < maxRequeueNo; i++ {
			expiration := int(math.Pow(10.0, float64(i+1)) * 1000)
			expirationStr := fmt.Sprintf("%v", expiration)
			requeueQueue, err := ch.QueueDeclare(
				getRequeueQueueName(queue.Name, expirationStr), // name
				queue.Durable,    // durable
				queue.AutoDelete, // delete when unused
				false,            // exclusive
				queue.NoWait,     // no-wait
				map[string]interface{}{
					"x-dead-letter-exchange":    exchange.Name,
					"x-dead-letter-routing-key": queue.RoutingKey,
					"x-message-ttl":             expiration,
				}, // args
			)

			if err != nil {
				log.Printf("rabbit:ProcessMessage:QueueDeclare Failed. Reason: %+v", err)
				return nil, nil, nil, closeChan, err
			}

			err = ch.QueueBind(
				requeueQueue.Name, // queue name
				requeueQueue.Name, // routing key
				retryExchangeName, // exchange name
				queue.NoWait,      // no-wait
				queue.BindArgs,    //args
			)

			if err != nil {
				log.Printf("rabbit:ProcessMessage:QueueBind Failed. Reason: %+v", err)
				return nil, nil, nil, closeChan, err
			}
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

	channel, msgs, errChan, closer, err := getChannel()
	if err != nil {
		return nil, nil, err
	}

	stop := make(chan interface{})
	return func(handler MessageHandleFunc) error {
			defer closer()
			for {

				select {
				case m := <-msgs:
					go func(m amqp.Delivery) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						err := handler(ctx, m.Body)
						if err != nil {
							log.Printf("Error handling rabbit message Exchange: %s Queue: %s Body: [%s] %+v\n", exchange.Name, queue.Name, m.Body, err)
							isProcessingError := IsEventProcessingError(err)
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
									expiration := fmt.Sprintf("%v", int(math.Pow(10.0, float64(noRequeues))*1000))
									err = channel.Publish(
										fmt.Sprintf("%s-%s", exchange.Name, retryExchangeNameSuffix), // exchange
										getRequeueQueueName(queue.Name, expiration),                  // routing key
										false, // mandatory
										false, // immediate
										amqp.Publishing{
											Headers:      map[string]interface{}{requeueHeaderKey: noRequeues},
											ContentType:  "text/plain",
											Body:         m.Body,
											DeliveryMode: amqp.Transient,
										})
									if err != nil {
										log.Printf("Error Requeueing message %+v\n", err)
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
							channel, msgs, errChan, closer, err = getChannel()
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
