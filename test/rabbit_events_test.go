package test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/getkalido/rabbit-events"
)

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
		It("Should define 3 retry queues, one for each delay time", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5)
			exchange := NewRabbitExchange(mockConfig)

			_, closeHandler, err := exchange.ReceiveFrom(
				"test-exchange",
				ExchangeTypeTopic,
				false,
				true,
				"test.queue",
				"listener",
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			// There's no easy way to test that queues got created in rabbit
			// So let's try defining those that should already be there
			// but with diferent attributes, so this should error if the queue
			// was defined correctly
			retryExchangeName := "test-exchange-retry"
			for i := 0; i < 3; i++ {
				expiration := fmt.Sprintf("%v", int(math.Pow(10.0, float64(i+1))*1000))
				queueName := fmt.Sprintf("listener-retry-%s", expiration)
				_, _, err := exchange.ReceiveFrom(
					retryExchangeName,
					ExchangeTypeTopic,
					false,
					false,
					"test.queue",
					queueName,
				)
				Expect(err).To(Not(BeNil()))
			}
		})
		It("Should requeue the message once within a 10 seconds window if a processing error is encountered", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5)
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
					return NewEventProcessingError("Whaaaaa?")
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
		It("Should not reuqueue the message if another type of error is encountered", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672")
			mockConfig.EXPECT().GetUserName().Return("guest")
			mockConfig.EXPECT().GetPassword().Return("guest")
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5)
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
})
