package rabbitevents

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
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
	if conn != nil {

		return conn.Close()
	}
	return nil
}

func (re *RabbitExchangeImpl) SendTo(name, exchangeType string, durable, autoDelete bool, key string) MessageHandleFunc {
	return func(message []byte) error {
		conn, err := re.getEventConnection()
		if err != nil {
			return err
		}

		var firstError error
		var ch *amqp.Channel
		var try int
		for try = 0; try < 3; try++ {
			ch, err = conn.Channel()
			if err != nil {
				if firstError == nil {
					firstError = errors.Wrapf(err, "conn.Channel Failed after try %v", try+1)
				}
				conn, err = re.newEventConnection(conn, re.rabbitIni)
				if err != nil {
					return errors.Wrapf(err, "rabbit:Failed to reconnect to rabbit after %v tries, first failing to %+v", try+1, err)
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
					firstError = errors.Wrapf(err, "ch.ExchangeDeclare Failed after try %v", try+1)
				}
				conn, err = re.newEventConnection(conn, re.rabbitIni)
				if err != nil {
					return errors.Wrapf(err, "rabbit:Failed to reconnect to rabbit after %v tries, first failing to %+v", try+1, err)
				}
				continue
			}

			err = ch.Publish(
				name,  // exchange
				key,   // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         message,
					DeliveryMode: amqp.Transient,
				})

			if err != nil {
				if firstError == nil {
					firstError = errors.Wrapf(err, "ch.Publish Failed after try %v", try+1)
				}
				conn, err = re.newEventConnection(conn, re.rabbitIni)
				if err != nil {
					return errors.Wrapf(err, "rabbit:Failed to reconnect to rabbit after %v tries, first failing to %+v", try+1, err)
				}
				continue
			}

			return nil

		}
		return errors.Wrapf(firstError, "rabbit:Failed to send message after %v tries", try+1)
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
}

func (re *RabbitExchangeImpl) Receive(exchange ExchangeSettings, queue QueueSettings) (func(MessageHandleFunc) error, func(), error) {
	getChannel := func() (<-chan amqp.Delivery, chan *amqp.Error, func(), error) {
		conn, err := re.getEventConnection()

		if err != nil {
			return nil, nil, func() {}, err
		}

		if conn.IsClosed() {
			conn, err = re.newEventConnection(conn, re.rabbitIni)
			if err != nil {
				return nil, nil, func() {}, err
			}
		}

		ch, err := conn.Channel()

		if err != nil {
			return nil, nil, func() {}, err
		}

		err = ch.Qos(runtime.NumCPU(), 0, false)
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
			return nil, nil, func() {
				err = ch.Close()
				if err != nil {
					log.Printf("rabbit:ProcessMessage:Consume Close Channel Failed. Reason: %+v", err)
				}
			}, err
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
			return nil, nil, func() {
				err = ch.Close()
				if err != nil {
					log.Printf("rabbit:ProcessMessage:Consume Close Channel Failed. Reason: %+v", err)
				}
			}, err
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
			return nil, nil, func() {
				err = ch.Close()
				if err != nil {
					log.Printf("rabbit:ProcessMessage:Consume Close Channel Failed. Reason: %+v", err)
				}
			}, err
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

			return nil, nil, func() {
				err = ch.Close()
				if err != nil {
					log.Printf("rabbit:ProcessMessage:Consume Close Channel Failed. Reason: %+v", err)
				}
			}, err
		}

		//We buffer the errors, so that when we are not processing the error message (like while shutting down) the channel can still close.
		errChan := make(chan *amqp.Error, 1)
		ch.NotifyClose(errChan)

		return msgs, errChan, func() {
			err = ch.Close()
			if err != nil {
				log.Printf("rabbit:ProcessMessage:Consume Close Channel Failed. Reason: %+v", err)
			}
		}, nil
	}

	msgs, errChan, closer, err := getChannel()
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
						err = handler(m.Body)
						if err != nil {
							log.Printf("Error handling rabbit message Exchange: %s Queue: %s Body: [%s] %+v\n", exchange.Name, queue.Name, m.Body, err)
							err = m.Nack(false, false)
							if err != nil {
								log.Printf("Error Nack rabbit message %+v\n", err)
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

					if e.Code == 320 || e.Code == 501 {
						// for now only care about total connection loss
						closer()
						for {
							time.Sleep(time.Millisecond * 250)
							msgs, errChan, closer, err = getChannel()
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

	err := listen(func(message []byte) error {
		lock.RLock()
		defer lock.RUnlock()
		for _, listener := range listeners {
			err := listener(message)
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
