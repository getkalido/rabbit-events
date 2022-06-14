package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"

	. "github.com/getkalido/rabbit-events"
)

type TestError struct {
	err error
}

func NewTestError(err error) error {
	return &TestError{err}
}

func (e *TestError) Error() string {
	return e.err.Error()
}

func (e *TestError) Temporary() bool {
	return true
}

// deleteQueue relies on the presence of Ginkgo an does not work
// outside of tests.
func deleteQueue(host string, username string, password string, queueName string) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", username, password, host))
	ExpectWithOffset(1, err).To(BeNil())
	defer func() {
		defer GinkgoRecover()
		err := conn.Close()
		Expect(err).To(BeNil())
	}()
	channel, err := conn.Channel()
	ExpectWithOffset(1, err).To(BeNil())
	_, err = channel.QueueDelete(queueName, false, false, false)
	ExpectWithOffset(1, err).To(BeNil())
}

var _ = Describe("RabbitEvents", func() {

	Describe("RabbitBatcher", func() {
		It("Should send messages after quiescence", func() {
			messagesSent := make([]string, 0)
			var l sync.Mutex
			mockSender := func(ctx context.Context, m []string, x string, ri RabbitConfig, _ map[string]interface{}) {
				l.Lock()
				defer l.Unlock()
				messagesSent = append(messagesSent, m...)
			}
			rb := &RabbitBatcher{QuiescenceTime: time.Nanosecond, MaxDelay: time.Hour, BatchSender: mockSender, Config: &RabbitIni{}}
			go func() {
				defer GinkgoRecover()
				rb.Process()
			}()
			rb.QueueMessage("A")

			time.Sleep(time.Millisecond * 40)

			l.Lock()
			defer l.Unlock()
			Expect(messagesSent).To(HaveLen(1))

			rb.Stop()
		})

		It("Should send messages before max timeout", func() {
			callCount := 0
			var l sync.Mutex

			mockSender := func(ctx context.Context, m []string, x string, ri RabbitConfig, _ map[string]interface{}) {
				l.Lock()
				defer l.Unlock()
				callCount++
			}
			rb := &RabbitBatcher{
				QuiescenceTime: time.Millisecond * 4,
				MaxDelay:       time.Millisecond * 5,
				BatchSender:    mockSender,
				Config:         &RabbitIni{},
			}
			go func() {
				defer GinkgoRecover()
				rb.Process()
			}()

			for k := 0; k < 15; k++ {
				rb.QueueMessage("A")
				time.Sleep(time.Millisecond * 1)
			}

			rb.Stop()

			l.Lock()
			defer l.Unlock()
			Expect(callCount > 1).To(Equal(true))
		})

		It("Should send all messages ", func() {
			messagesSent := make([]string, 0)
			var l sync.Mutex

			mockSender := func(ctx context.Context, m []string, x string, ri RabbitConfig, _ map[string]interface{}) {
				l.Lock()
				defer l.Unlock()
				messagesSent = append(messagesSent, m...)
			}
			rb := &RabbitBatcher{
				QuiescenceTime: time.Millisecond * 4,
				MaxDelay:       time.Millisecond * 5,
				BatchSender:    mockSender,
			}
			go func() {
				defer GinkgoRecover()
				rb.Process()
			}()

			var wg sync.WaitGroup
			for k := 0; k < 15; k++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rb.QueueMessage("A")
				}()
			}
			wg.Wait()

			time.Sleep(6 * time.Millisecond)

			rb.Stop()

			l.Lock()
			defer l.Unlock()
			Expect(messagesSent).To(HaveLen(15))
		})

	})

	Describe("Event Publisher", func() {
		It("Should cancel publishing midway if context is timed out", func() {
			exchange := NewRabbitExchange(&RabbitIni{})
			eventHandler := NewRabbitEventHandler(exchange, "", 0)
			emitter := eventHandler.Emit("test")
			ctx, cancel := context.WithTimeout(context.Background(), -time.Nanosecond)
			defer cancel()
			err := emitter(ctx, Create, nil, 54, nil, nil)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("Should cancel publishing midway if context is cancelled", func() {
			exchange := NewRabbitExchange(&RabbitIni{})
			eventHandler := NewRabbitEventHandler(exchange, "", 0)
			emitter := eventHandler.Emit("test")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := emitter(ctx, Create, nil, 54, nil, nil)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(context.Canceled))
		})

		It("Should not cancel publishing midway if context is not cancelled", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			exchange := NewMockRabbitExchange(ctrl)
			exchangeRunner := func(ctx context.Context, data []byte, _ map[string]interface{}) error {
				Expect(ctx.Err()).To(BeNil())
				return nil
			}
			exchange.EXPECT().SendTo("test-exchange-headers", ExchangeTypeHeaders, true, false, "").Return(exchangeRunner)
			exchange.EXPECT().SendTo("test-exchange", ExchangeTypeTopic, true, false, "test").Return(exchangeRunner)
			eventHandler := NewRabbitEventHandler(exchange, "test-exchange", 0)
			emitter := eventHandler.Emit("test")
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			err := emitter(ctx, Create, nil, 54, nil, nil)
			Expect(err).To(BeNil())
		})
	})

	Describe("Exchange ReceiveFrom", func() {
		var (
			closeHandlerOnce sync.Once
			exchange         *RabbitExchangeImpl
			ctrl             *gomock.Controller
			mockConfig       *MockRabbitConfig
		)

		BeforeEach(func() {
			closeHandlerOnce = sync.Once{}

			ctrl = gomock.NewController(GinkgoT())
			mockConfig = NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672").Times(2)
			mockConfig.EXPECT().GetUserName().Return("guest").Times(2)
			mockConfig.EXPECT().GetPassword().Return("guest").Times(2)
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange = NewRabbitExchange(mockConfig)
		})

		AfterEach(func() {
			// The queue must be deleted after the tests, in order to avoid
			// a concurrency issue where the queue exists when the next test
			// checks for it, but is deleted before the next test binds
			// to the queue.
			deleteQueue("localhost:5672", "guest", "guest", "requeue-test")
		})

		It("Should requeue the message once within a 10 seconds window if a processing error is encountered", func() {
			defer ctrl.Finish()

			handler, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
			)
			Expect(err).To(BeNil())
			defer closeHandlerOnce.Do(closeHandler)
			replies := make(chan struct{})
			doOnce := make(chan struct{}, 1)
			go func() {
				defer GinkgoRecover()
				err := handler(func(ctx context.Context, message []byte, _ map[string]interface{}) (err error) {
					replies <- struct{}{}
					doOnce <- struct{}{}
					return NewEventProcessingError(errors.New("Whaaaaaa"))
				})
				Expect(err).To(BeNil())
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			err = sendFn(context.Background(), []byte{}, map[string]interface{}{})
			Expect(err).To(BeNil())
			noReplies := 0
			timer := time.NewTimer(13 * time.Second)
		recvLoop:
			for {
				select {
				case <-replies:
					noReplies++
					if noReplies == 2 {
						break recvLoop
					}
				case <-timer.C:
					break recvLoop
				}
			}
			Expect(noReplies).To(Equal(2))
		})

		It("Should requeue the message once within a 10 seconds window if another type of temporary error is enountered", func() {
			defer ctrl.Finish()

			handler, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
			)
			Expect(err).To(BeNil())
			defer closeHandlerOnce.Do(closeHandler)
			replies := make(chan struct{})
			doOnce := make(chan struct{}, 1)
			go func() {
				defer GinkgoRecover()
				err := handler(func(ctx context.Context, message []byte, _ map[string]interface{}) (err error) {
					replies <- struct{}{}
					doOnce <- struct{}{}
					return NewTestError(errors.New("This should result in a requeueing as well"))
				})
				Expect(err).To(BeNil())
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			err = sendFn(context.Background(), []byte{}, map[string]interface{}{})
			Expect(err).To(BeNil())
			noReplies := 0
			timer := time.NewTimer(13 * time.Second)
		recvLoop:
			for {
				select {
				case <-replies:
					noReplies++
					if noReplies == 2 {
						break recvLoop
					}
				case <-timer.C:
					break recvLoop
				}
			}
			Expect(noReplies).To(Equal(2))
		})

		It("Should not requeue the message if an error that is not temporary is encountered", func() {
			defer ctrl.Finish()

			handler, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
			)
			Expect(err).To(BeNil())
			defer closeHandlerOnce.Do(closeHandler)

			replies := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				err := handler(func(ctx context.Context, message []byte, _ map[string]interface{}) (err error) {
					replies <- struct{}{}
					return errors.New("Whaaaa!")
				})
				Expect(err).To(BeNil())
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			err = sendFn(context.Background(), []byte{}, map[string]interface{}{})
			Expect(err).To(BeNil())
			noReplies := 0
			timer := time.NewTimer(13 * time.Second)
		recvLoop:
			for {
				select {
				case <-replies:
					noReplies++
					if noReplies == 2 {
						break recvLoop
					}
				case <-timer.C:
					break recvLoop
				}
			}
			Expect(noReplies).To(Equal(1))
		})
	})
})
