package test

import (
	"context"
	"errors"
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

var _ = Describe("RabbitEvents", func() {

	Describe("RabbitBatcher", func() {
		It("Should send messages after quiescence", func() {
			messagesSent := make([]string, 0)
			var l sync.Mutex
			mockSender := func(ctx context.Context, m []string, x string, ri RabbitConfig) {
				l.Lock()
				defer l.Unlock()
				messagesSent = append(messagesSent, m...)
			}
			rb := &RabbitBatcher{QuiescenceTime: time.Nanosecond, MaxDelay: time.Hour, BatchSender: mockSender, Config: &RabbitIni{}}
			go rb.Process()
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

			mockSender := func(ctx context.Context, m []string, x string, ri RabbitConfig) {
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
			go rb.Process()

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

			mockSender := func(ctx context.Context, m []string, x string, ri RabbitConfig) {
				l.Lock()
				defer l.Unlock()
				messagesSent = append(messagesSent, m...)
			}
			rb := &RabbitBatcher{
				QuiescenceTime: time.Millisecond * 4,
				MaxDelay:       time.Millisecond * 5,
				BatchSender:    mockSender,
			}
			go rb.Process()

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
			exchangeRunner := func(ctx context.Context, data []byte) error {
				Expect(ctx.Err()).To(BeNil())
				return nil
			}
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
		It("Should requeue the message once within a 10 seconds window if a processing error is encountered", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange := NewRabbitExchange(mockConfig)

			handler, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			replies := make(chan struct{})
			doOnce := make(chan struct{}, 1)
			go func() {
				handler(func(ctx context.Context, message []byte) (err error) {
					replies <- struct{}{}
					doOnce <- struct{}{}
					return NewEventProcessingError(errors.New("Whaaaaaa"))
				})
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			sendFn(context.Background(), []byte{})
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

			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange := NewRabbitExchange(mockConfig)

			handler, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			replies := make(chan struct{})
			doOnce := make(chan struct{}, 1)
			go func() {
				handler(func(ctx context.Context, message []byte) (err error) {
					replies <- struct{}{}
					doOnce <- struct{}{}
					return NewTestError(errors.New("This should result in a requeueing as well"))
				})
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			sendFn(context.Background(), []byte{})
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
		It("Should not reuqueue the message if an error that is not temporary is encountered", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange := NewRabbitExchange(mockConfig)

			handler, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			replies := make(chan struct{})
			go func() {
				handler(func(ctx context.Context, message []byte) (err error) {
					replies <- struct{}{}
					return errors.New("Whaaaa!")
				})
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			sendFn(context.Background(), []byte{})
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
	Describe("Exchange BulkReceiveFrom", func() {
		It("Should receive messages in bulk", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange := NewRabbitExchange(mockConfig)

			handler, closeHandler, err := exchange.BulkReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
				2,
				1000,
			)
			handlerCalled := make(chan struct{})
			noReplies := 0
			Expect(err).To(BeNil())
			defer closeHandler()
			go func() {
				handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
					handlerCalled <- struct{}{}
					Expect(len(messages)).To(Equal(2))
					return nil
				})
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			sendFn(context.Background(), []byte{})
			sendFn(context.Background(), []byte{})
			timer := time.NewTimer(3 * time.Second)
		recvLoop:
			for {
				select {
				case <-handlerCalled:
					noReplies++
					if noReplies == 1 {
						break recvLoop
					}
				case <-timer.C:
					break recvLoop
				}
			}
			Expect(noReplies).To(Equal(1))
		})
		It("Should receive 2 messages even if the prefetch is 3 if it takes more than maxWait to get 3 messages", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange := NewRabbitExchange(mockConfig)

			handler, closeHandler, err := exchange.BulkReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
				2,
				100,
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			handlerCalled := make(chan struct{})
			noReplies := 0
			go func() {
				handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
					handlerCalled <- struct{}{}
					Expect(len(messages)).To(BeNumerically("<=", 2))
					return nil
				})
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			sendFn(context.Background(), []byte{})
			sendFn(context.Background(), []byte{})
			time.Sleep(100 * time.Millisecond)
			sendFn(context.Background(), []byte{})
			timer := time.NewTimer(3 * time.Second)
		recvLoop:
			for {
				select {
				case <-handlerCalled:
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
		It("Should only requeue the message that resulted in temporary error", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()
			exchange := NewRabbitExchange(mockConfig)

			handler, closeHandler, err := exchange.BulkReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"requeue-test",
				3,
				1000,
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			handlerCalled := make(chan struct{})
			noReplies := 0
			go func() {
				handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
					handlerCalled <- struct{}{}
					if noReplies == 0 {
						Expect(len(messages)).To(Equal(3))
					} else {
						Expect(len(messages)).To(Equal(1))
						return nil
					}
					return []*MessageError{
						NewMessageError(messages[0], NewEventProcessingError(errors.New("Whaaaaaa"))),
						NewMessageError(messages[1], errors.New("Nooo")),
					}
				})
			}()

			sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
			sendFn(context.Background(), []byte{})
			sendFn(context.Background(), []byte{})
			sendFn(context.Background(), []byte{})
			timer := time.NewTimer(13 * time.Second)
		recvLoop:
			for {
				select {
				case <-handlerCalled:
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
	})
})
