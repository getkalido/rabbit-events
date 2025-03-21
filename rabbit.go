package rabbitevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	slogformatter "github.com/samber/slog-formatter"
)

const (
	ExchangeTypeFanout = "fanout"
	ExchangeTypeTopic  = "topic"
)

type MessageHandler interface {
	HandleMessage([]byte) error
}

type MessageHandleFunc func(context.Context, []byte) error
type BulkMessageHandleFunc func(context.Context, []amqp.Delivery) []*MessageError

type RabbitConfig interface {
	GetUserName() string
	GetPassword() string
	GetHost() string
	GetConnectTimeout() time.Duration
}

var defaultLogger atomic.Pointer[slog.Logger]

func DefaultLogger() *slog.Logger {
	return defaultLogger.Load()
}

func SetDefaultLogger(logger *slog.Logger) {
	defaultLogger.Store(logger)
}

func init() {
	defaultLogger.Store(
		slog.New(
			slogformatter.NewFormatterHandler(
				slogformatter.ErrorFormatter("reason"),
			)(
				slog.NewJSONHandler(
					os.Stderr,
					&slog.HandlerOptions{
						Level: slog.LevelDebug,
					},
				),
			),
		),
	)
}

// ProcessDirectMessage Processes messages from the `exchange` queue. All calls bind to the same queue, and messages are load balanced over them.
// An error from the MessageHandler.HandleMessage Will caue messages not to be acked (and retried)
func ProcessDirectMessage(rabbitIni RabbitConfig, exchange, routingKey string, handler MessageHandler) {
	for {
		time.Sleep(time.Millisecond * 250)

		conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", rabbitIni.GetUserName(), rabbitIni.GetPassword(), rabbitIni.GetHost()))

		if err != nil {
			DefaultLogger().Error("rabbit:ProcessDirectMessage:Dial Failed", slog.Any("reason", err))
			continue
		}

		ch, err := conn.Channel()

		if err != nil {
			DefaultLogger().Error("rabbit:ProcessDirectMessage:Channel Failed", slog.Any("reason", err))
			err = conn.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:Channel Close Connection Failed", slog.Any("reason", err))
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
			DefaultLogger().Error("rabbit:ProcessDirectMessage:ExchangeDeclare Failed", slog.Any("reason", err))
			err = ch.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:ExchangeDeclare Close Channel Failed", slog.Any("reason", err))
			}
			err = conn.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:ExchangeDeclare Close Connection Failed", slog.Any("reason", err))
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
			DefaultLogger().Error("rabbit:ProcessDirectMessage:QueueDeclare Failed", slog.Any("reason", err))
			err = ch.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:QueueDeclare Close Channel Failed", slog.Any("reason", err))
			}
			err = conn.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:QueueDeclare Close Connection Failed", slog.Any("reason", err))
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
			DefaultLogger().Error("rabbit:ProcessDirectMessage:QueueBind Failed", slog.Any("reason", err))
			err = ch.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:QueueBind Close Channel Failed", slog.Any("reason", err))
			}
			err = conn.Close()
			if err != nil {
				DefaultLogger().Error("rabbit:ProcessDirectMessage:QueueBind Close Connection Failed", slog.Any("reason", err))
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
					DefaultLogger().Error("rabbit:ProcessDirectMessage:handler.HandleMessage Failed", slog.Any("reason", err))
					continue
				}

				err = d.Ack(false)
				if err != nil {
					DefaultLogger().Error("rabbit:ProcessDirectMessage:d.Ack Failed", slog.Any("reason", err))
					return
				}
			}
		}()

		<-forever

		err = ch.Close()
		if err != nil {
			DefaultLogger().Error("rabbit:ProcessDirectMessage:ch.Close Failed", slog.Any("reason", err))
		}
	}
}

// SendChangeNotificationMessages Sends a bunch of messages on the change notification rabbit channel, without dialing everytime like a noob
func SendChangeNotificationMessages(ctx context.Context, messages []string, channel string, rabbitIni RabbitConfig) {

	exchange := NewRabbitExchange(rabbitIni)

	defer func() { _ = exchange.Close() }()

	messageHandler := PackerString(exchange.SendTo(channel, ExchangeTypeFanout, false, true, ""))

	err := messageHandler(ctx, messages)
	if err != nil {
		DefaultLogger().Error("Error sending rabbit messages", slog.Any("reason", err))
	}
}

type RabbitBatcher struct {
	once               sync.Once
	stop               chan struct{}
	q                  chan string
	QuiescenceTime     time.Duration
	MaxDelay           time.Duration
	MaxMessagesInBatch int
	BatchSender        func(context.Context, []string, string, RabbitConfig)
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
					rb.BatchSender(context.Background(), messages, rb.Channel, rb.Config)
					messages = messages[:0]
					timeoutC = nil
					quiescenceC = nil
				}
			}
		case <-timeoutC:
			{
				rb.BatchSender(context.Background(), messages, rb.Channel, rb.Config)
				messages = messages[:0]
				timeoutC = nil
				quiescenceC = nil
			}
		case <-quiescenceC:
			{
				rb.BatchSender(context.Background(), messages, rb.Channel, rb.Config)
				messages = messages[:0]
				timeoutC = nil
				quiescenceC = nil
			}
		case <-rb.stop:
			{
				if len(messages) > 0 {
					rb.BatchSender(context.Background(), messages, rb.Channel, rb.Config)
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
	return func(ctx context.Context, packed []byte) error {
		if ctx == nil {
			return ErrNilContext
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		messages := make([]string, 0)
		err := json.Unmarshal(packed, &messages)
		if err != nil {
			return err
		}
		for _, val := range messages {
			err = handler(ctx, []byte(val))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func PackerString(handler MessageHandleFunc) func(ctx context.Context, messages []string) error {
	return func(ctx context.Context, messages []string) error {
		if ctx == nil {
			return ErrNilContext
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		data, err := json.Marshal(messages)
		if err != nil {
			return err
		}
		err = handler(ctx, data)
		if err != nil {
			return err
		}
		return nil
	}
}
