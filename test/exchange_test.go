package test

import (
	"context"
	"errors"
	"sync"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/getkalido/rabbit-events"
)

var _ = Describe("Exchange", func() {
	Describe("RabbitExchangeImpl", func() {
		Describe("ReceiveFrom", func() {
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
})
