package rabbitevents

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	ExchangeTypeFanout = "fanout"
	ExchangeTypeTopic  = "topic"
)

type MessageHandler interface {
	HandleMessage([]byte) error
}

type MessageHandleFunc func([]byte) error

type RabbitConfig interface {
	GetUserName() string
	GetPassword() string
	GetHost() string
	GetConnectTimeout() time.Duration
}

// ProcessDirectMessage Processes messages from the `exchange` queue. All calls bind to the same queue, and messages are load balanced over them.
// An error from the MessageHandler.HandleMessage Will caue messages not to be acked (and retried)
func ProcessDirectMessage(rabbitIni RabbitConfig, exchange, routingKey string, handler MessageHandler) {
	for {
		time.Sleep(time.Millisecond * 250)

		conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", rabbitIni.GetUserName(), rabbitIni.GetPassword(), rabbitIni.GetHost()))

		if err != nil {
			log.Printf("rabbit:ProcessDirectMessage:Dial Failed. Reason: %+v", err)
			continue
		}

		ch, err := conn.Channel()

		if err != nil {
			log.Printf("rabbit:ProcessDirectMessage:Channel Failed. Reason: %+v", err)
			err = conn.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:Channel Close Connection Failed. Reason: %+v", err)
			}
			continue
		}

		err = ch.ExchangeDeclare(
			exchange, // name
			"direct", // type
			true,     // durable
			false,    // delete when unused
			false,    // exclusive
			false,    // no-wait
			nil,      // args
		)

		if err != nil {
			log.Printf("rabbit:ProcessDirectMessage:ExchangeDeclare Failed. Reason: %+v", err)
			err = ch.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:ExchangeDeclare Close Channel Failed. Reason: %+v", err)
			}
			err = conn.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:ExchangeDeclare Close Connection Failed. Reason: %+v", err)
			}
			continue
		}

		q, err := ch.QueueDeclare(
			fmt.Sprintf("%s:%s", exchange, routingKey), // queue name should be unique to the routingkey
			false, // durable
			false, // delete when unused
			false, // not exclusive
			false, // no-wait
			nil,   // args
		)

		if err != nil {
			log.Printf("rabbit:ProcessDirectMessage:QueueDeclare Failed. Reason: %+v", err)
			err = ch.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:QueueDeclare Close Channel Failed. Reason: %+v", err)
			}
			err = conn.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:QueueDeclare Close Connection Failed. Reason: %+v", err)
			}
			continue
		}

		err = ch.QueueBind(
			q.Name,     // queue name
			routingKey, // routing key
			exchange,   // exchange name
			false,      // no-wait
			nil,        //args
		)

		if err != nil {
			log.Printf("rabbit:ProcessDirectMessage:QueueBind Failed. Reason: %+v", err)
			err = ch.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:QueueBind Close Channel Failed. Reason: %+v", err)
			}
			err = conn.Close()
			if err != nil {
				log.Printf("rabbit:ProcessDirectMessage:QueueBind Close Connection Failed. Reason: %+v", err)
			}
			continue
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

		forever := make(chan bool)

		go func() {
			defer close(forever)
			for d := range msgs {
				msg := d.Body
				err = handler.HandleMessage(msg)
				if err != nil {
					log.Printf("%+v", err)
					continue
				}

				err = d.Ack(false)
				if err != nil {
					log.Printf("%+v", err)
					return
				}

			}
		}()

		<-forever

		err = ch.Close()
		if err != nil {
			log.Printf("rabbit:ProcessDirectMessage:ch.Close Failed. Reason: %+v", err)

		}
	}
}

// SendChangeNotificationMessages Sends a bunch of messages on the change notification rabbit channel, without dialing everytime like a noob
func SendChangeNotificationMessages(messages []string, channel string, rabbitIni RabbitConfig) {

	exchange := NewRabbitExchange(rabbitIni)

	defer func() { _ = exchange.Close() }()

	messageHandler := PackerString(exchange.SendTo(channel, ExchangeTypeFanout, false, true, ""))

	err := messageHandler(messages)
	if err != nil {
		log.Printf("Error sending rabbit messages %+v\n", err)
	}
}

type RabbitBatcher struct {
	once               sync.Once
	stop               chan struct{}
	q                  chan string
	QuiescenceTime     time.Duration
	MaxDelay           time.Duration
	MaxMessagesInBatch int
	BatchSender        func([]string, string, RabbitConfig)
	Config             RabbitConfig
	Channel            string
}

var DefaultQuiescenceTime time.Duration = time.Millisecond * 5
var DefaultMaxDelay time.Duration = time.Millisecond * 50
var DefualtMaxMessagesInBatch = 1000

func (rb *RabbitBatcher) setup() {
	rb.q = make(chan string, 1)
	rb.stop = make(chan struct{})
	if rb.MaxDelay == 0 {
		rb.MaxDelay = DefaultMaxDelay

	}
	if rb.QuiescenceTime == 0 {
		rb.QuiescenceTime = DefaultQuiescenceTime
	}
	if rb.BatchSender == nil {
		rb.BatchSender = SendChangeNotificationMessages
	}
	if rb.MaxMessagesInBatch == 0 {
		rb.MaxMessagesInBatch = DefualtMaxMessagesInBatch
	}
}

func (rb *RabbitBatcher) Process() {
	rb.once.Do(rb.setup)
	var quiescence *time.Timer
	var timeout *time.Timer
	var timeoutC <-chan time.Time
	var quiescenceC <-chan time.Time

	messages := make([]string, 0)
	for {
		select {
		case message := <-rb.q:
			{
				messages = append(messages, message)
				if len(messages) == 1 {
					timeout = time.NewTimer(rb.MaxDelay)
					quiescence = time.NewTimer(rb.QuiescenceTime)
					timeoutC = timeout.C
					quiescenceC = quiescence.C
				} else {
					quiescence.Reset(rb.QuiescenceTime)
				}
				if len(messages) >= rb.MaxMessagesInBatch {
					rb.BatchSender(messages, rb.Channel, rb.Config)
					messages = messages[:0]
					timeoutC = nil
					quiescenceC = nil
				}
			}
		case <-timeoutC:
			{
				rb.BatchSender(messages, rb.Channel, rb.Config)
				messages = messages[:0]
				timeoutC = nil
				quiescenceC = nil
			}
		case <-quiescenceC:
			{
				rb.BatchSender(messages, rb.Channel, rb.Config)
				messages = messages[:0]
				timeoutC = nil
				quiescenceC = nil
			}
		case <-rb.stop:
			{
				if len(messages) > 0 {
					rb.BatchSender(messages, rb.Channel, rb.Config)
				}
				return
			}
		}
	}
}

func (rb *RabbitBatcher) QueueMessage(message string) {
	rb.once.Do(rb.setup)
	rb.q <- message
}

func (rb *RabbitBatcher) Stop() {
	close(rb.stop)
}

var defaultBatcher = &RabbitBatcher{}

func init() {
	go defaultBatcher.Process()
}

func UnPacker(handler MessageHandleFunc) MessageHandleFunc {
	return func(packed []byte) error {
		messages := make([]string, 0)
		err := json.Unmarshal(packed, &messages)
		if err != nil {
			return err
		}
		for _, val := range messages {
			err = handler([]byte(val))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func PackerString(handler MessageHandleFunc) func(messages []string) error {
	return func(messages []string) error {
		data, err := json.Marshal(messages)
		if err != nil {
			return err
		}
		err = handler(data)
		if err != nil {
			return err
		}
		return nil
	}
}
