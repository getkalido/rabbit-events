package test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"

	. "github.com/getkalido/rabbit-events"
)

var _ = Describe("Exchange", func() {
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

	Describe("RabbitExchangeImpl", func() {
		Describe("ReceiveFrom", func() {

			AfterEach(func() {
				// The queue must be deleted after the tests, in order to avoid
				// a concurrency issue where the queue exists when the next test
				// checks for it, but is deleted before the next test binds
				// to the queue.
				deleteQueue("localhost:5672", "guest", "guest", "requeue-test")
			})

			It("Should requeue the message once within a 10-second window if a processing error is encountered", func() {
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
				var noReplies int32
				timer := time.NewTimer(13 * time.Second)
			recvLoop:
				for {
					select {
					case <-replies:
						if atomic.AddInt32(&noReplies, 1) >= 2 {
							break recvLoop
						}
					case <-timer.C:
						break recvLoop
					}
				}
				Expect(noReplies).To(BeEquivalentTo(2))
			})

			It("Should requeue the message once within a 10-second window if another type of temporary error is enountered", func() {
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
				var noReplies int32
				timer := time.NewTimer(13 * time.Second)
			recvLoop:
				for {
					select {
					case <-replies:
						if atomic.AddInt32(&noReplies, 1) >= 2 {
							break recvLoop
						}
					case <-timer.C:
						break recvLoop
					}
				}
				Expect(noReplies).To(BeEquivalentTo(2))
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
				var noReplies int32
				timer := time.NewTimer(13 * time.Second)
			recvLoop:
				for {
					select {
					case <-replies:
						if atomic.AddInt32(&noReplies, 1) >= 2 {
							break recvLoop
						}
					case <-timer.C:
						break recvLoop
					}
				}
				Expect(noReplies).To(BeEquivalentTo(1))
			})
		})

		Describe("BulkReceiveFrom", func() {
			It("Should receive messages in bulk", func() {
				defer ctrl.Finish()

				handler, closeHandler, err := exchange.BulkReceiveFrom(
					"test-exchange",
					ExchangeTypeTopic,
					false,
					true,
					"test.queue",
					"requeue-test",
					11,
					1000*time.Millisecond,
					2,
				)
				handlerCalled := make(chan struct{})
				var noReplies int32
				Expect(err).To(BeNil())
				defer closeHandler()
				go func() {
					handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
						Expect(len(messages)).To(Equal(2))
						handlerCalled <- struct{}{}
						return nil
					})
				}()

				sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				timer := time.NewTimer(3 * time.Second)
			recvLoop:
				for {
					select {
					case <-handlerCalled:
						if atomic.AddInt32(&noReplies, 1) >= 1 {
							break recvLoop
						}
					case <-timer.C:
						break recvLoop
					}
				}
				Expect(noReplies).To(BeEquivalentTo(1))
			})

			It("Should receive 2 messages even if the prefetch is 3 if it takes more than maxWait to get 3 messages", func() {
				defer ctrl.Finish()

				handler, closeHandler, err := exchange.BulkReceiveFrom(
					"test-exchange",
					ExchangeTypeTopic,
					false,
					true,
					"test.queue",
					"requeue-test",
					10,
					100*time.Millisecond,
					2,
				)
				Expect(err).To(BeNil())
				defer closeHandler()
				handlerCalled := make(chan struct{})
				var noReplies int32
				go func() {
					handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
						Expect(len(messages)).To(BeNumerically("<=", 2))
						handlerCalled <- struct{}{}
						return nil
					})
				}()

				sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				time.Sleep(100 * time.Millisecond)
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				timer := time.NewTimer(3 * time.Second)
			recvLoop:
				for {
					select {
					case <-handlerCalled:
						if atomic.AddInt32(&noReplies, 1) >= 2 {
							break recvLoop
						}
					case <-timer.C:
						break recvLoop
					}
				}
				Expect(noReplies).To(BeEquivalentTo(2))
			})

			It("Should only requeue the message that resulted in temporary error", func() {
				defer ctrl.Finish()

				handler, closeHandler, err := exchange.BulkReceiveFrom(
					"test-exchange",
					ExchangeTypeTopic,
					false,
					true,
					"test.queue",
					"requeue-test",
					3,
					100*time.Millisecond,
					3,
				)
				Expect(err).To(BeNil())
				defer closeHandler()
				handlerCalled := make(chan struct{})
				var noReplies int32
				go func() {
					handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
						if atomic.LoadInt32(&noReplies) == 0 {
							Expect(len(messages)).To(Equal(3))
							handlerCalled <- struct{}{}
						} else {
							Expect(len(messages)).To(Equal(1))
							handlerCalled <- struct{}{}
							return nil
						}
						return []*MessageError{
							NewMessageError(messages[0], NewEventProcessingError(errors.New("Whaaaaaa"))),
							NewMessageError(messages[1], errors.New("Nooo")),
						}
					})
				}()

				sendFn := exchange.SendTo("test-exchange", ExchangeTypeTopic, false, true, "test.queue")
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				sendFn(context.Background(), []byte{}, map[string]interface{}{})
				timer := time.NewTimer(13 * time.Second)
			recvLoop:
				for {
					select {
					case <-handlerCalled:
						if atomic.AddInt32(&noReplies, 1) >= 2 {
							break recvLoop
						}
					case <-timer.C:
						break recvLoop
					}
				}
				Expect(noReplies).To(BeEquivalentTo(2))
			})
		})
	})
})
