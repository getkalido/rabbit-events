package test

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/getkalido/rabbit-events"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ = Describe("RabbitEvents", func() {
	GetTestConfig := func(ctrl *gomock.Controller) RabbitConfig {
		mockConfig := NewMockRabbitConfig(ctrl)
		mockConfig.EXPECT().GetHost().Return("localhost:5672").MinTimes(2)
		mockConfig.EXPECT().GetUserName().Return("guest").MinTimes(2)
		mockConfig.EXPECT().GetPassword().Return("guest").MinTimes(2)
		mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).MinTimes(2)
		return mockConfig
	}

	DeleteExchanges := func(cfg RabbitConfig, exchanges ...string) {
		RabbitDo(cfg, func(ch *amqp.Channel) {
			for _, exchange := range exchanges {
				Expect(ch.ExchangeDelete(exchange, false, false)).To(Succeed())
				Expect(ch.ExchangeDelete(exchange+"-retry", false, false)).To(Succeed())
			}
		})
	}

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
		It("Should cancel publishing midway if context is timed out", func(ctx context.Context) {
			exchange := NewRabbitExchange(&RabbitIni{})
			eventHandler := NewRabbitEventHandler(exchange, "", 0)
			emitter := eventHandler.Emit("test")
			ctx, cancel := context.WithTimeout(ctx, -time.Nanosecond)
			defer cancel()
			err := emitter(ctx, Create, nil, 54, nil, nil)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("Should cancel publishing midway if context is cancelled", func(ctx context.Context) {
			exchange := NewRabbitExchange(&RabbitIni{})
			eventHandler := NewRabbitEventHandler(exchange, "", 0)
			emitter := eventHandler.Emit("test")
			ctx, cancel := context.WithCancel(ctx)
			cancel()
			err := emitter(ctx, Create, nil, 54, nil, nil)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(context.Canceled))
		})

		It("Should not cancel publishing midway if context is not cancelled", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			exchange := NewMockRabbitExchange(ctrl)
			exchangeRunner := func(ctx context.Context, data []byte) error {
				Expect(ctx.Err()).To(BeNil())
				return nil
			}
			exchange.EXPECT().SendTo("test-exchange", ExchangeTypeTopic, true, false, "test").Return(exchangeRunner)
			eventHandler := NewRabbitEventHandler(exchange, "test-exchange", 0)
			emitter := eventHandler.Emit("test")
			ctx, cancel := context.WithTimeout(ctx, time.Hour)
			defer cancel()
			err := emitter(ctx, Create, nil, 54, nil, nil)
			Expect(err).To(BeNil())
		})
	})

	Describe("Exchange ReceiveFrom", func() {
		It("Should requeue the message once within a 10 seconds window if a processing error is encountered", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("proc-err")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			handler, closeHandler, err := exchange.ReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
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

			sendFn := exchange.SendTo(namer.Exchange(), ExchangeTypeTopic, false, true, namer.Topic())
			sendFn(ctx, []byte{})
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

		It("Should requeue the message once within a 10 seconds window if another type of temporary error is enountered", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("temp-err")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			handler, closeHandler, err := exchange.ReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
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

			sendFn := exchange.SendTo(namer.Exchange(), ExchangeTypeTopic, false, true, namer.Topic())
			sendFn(ctx, []byte{})
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

		It("Should not reuqueue the message if an error that is not temporary is encountered", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("misc-err")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			handler, closeHandler, err := exchange.ReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
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

			sendFn := exchange.SendTo(namer.Exchange(), ExchangeTypeTopic, false, true, namer.Topic())
			sendFn(ctx, []byte{})
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
		It("Should receive messages in bulk", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("receive-bulk")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			handler, closeHandler, err := exchange.BulkReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
				11,
				1000*time.Millisecond,
				2,
			)
			handlerCalled := make(chan struct{})
			noReplies := 0
			Expect(err).To(BeNil())
			defer closeHandler()
			go func() {
				handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
					Expect(messages).To(HaveLen(2))
					handlerCalled <- struct{}{}
					return nil
				})
			}()

			sendFn := exchange.SendTo(namer.Exchange(), ExchangeTypeTopic, false, true, namer.Topic())
			sendFn(ctx, []byte{})
			sendFn(ctx, []byte{})
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
		It("Should receive 2 messages even if the prefetch is 3 if it takes more than maxWait to get 3 messages", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("receive-bulk-max-wait")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			handler, closeHandler, err := exchange.BulkReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
				10,
				100*time.Millisecond,
				2,
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			handlerCalled := make(chan struct{})
			noReplies := 0
			go func() {
				handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
					Expect(len(messages)).To(BeNumerically("<=", 2))
					handlerCalled <- struct{}{}
					return nil
				})
			}()

			sendFn := exchange.SendTo(namer.Exchange(), ExchangeTypeTopic, false, true, namer.Topic())
			sendFn(ctx, []byte{})
			sendFn(ctx, []byte{})
			time.Sleep(100 * time.Millisecond)
			sendFn(ctx, []byte{})
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

		It("Should only requeue the message that resulted in temporary error", func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("receive-bulk-temp-err")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			handler, closeHandler, err := exchange.BulkReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
				3,
				100*time.Millisecond,
				3,
			)
			Expect(err).To(BeNil())
			defer closeHandler()
			handlerCalled := make(chan struct{})
			noReplies := 0
			go func() {
				handler(func(ctx context.Context, messages []amqp.Delivery) []*MessageError {
					if noReplies == 0 {
						Expect(messages).To(HaveLen(3))
						handlerCalled <- struct{}{}
					} else {
						Expect(messages).To(HaveLen(1))
						handlerCalled <- struct{}{}
						return nil
					}
					return []*MessageError{
						NewMessageError(messages[0], NewEventProcessingError(errors.New("Whaaaaaa"))),
						NewMessageError(messages[1], errors.New("Nooo")),
					}
				})
			}()

			sendFn := exchange.SendTo(namer.Exchange(), ExchangeTypeTopic, false, true, namer.Topic())
			sendFn(ctx, []byte{})
			sendFn(ctx, []byte{})
			sendFn(ctx, []byte{})
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

	Describe("Bulk send", func() {
		It("Should pipeline bulk events", NodeTimeout(10*time.Second), func(ctx context.Context) {
			ctrl := gomock.NewController(GinkgoT())
			DeferCleanup(ctrl.Finish)
			mockConfig := GetTestConfig(ctrl)
			exchange := NewRabbitExchange(mockConfig, WithRetryAutoDelete())

			namer := NewNamer("bulk-send")
			DeferCleanup(func() {
				DeleteExchanges(mockConfig, namer.Exchange())
			})
			receive, receiverClose, err := exchange.ReceiveFrom(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
				namer.Queue(),
			)
			closeReceiver := sync.OnceFunc(receiverClose)
			defer closeReceiver()
			Expect(err).ToNot(HaveOccurred())

			send := exchange.BulkSendTo(
				namer.Exchange(),
				ExchangeTypeTopic,
				false,
				true,
				namer.Topic(),
			)

			expectedMessages := [][]byte{
				[]byte("test1"),
				[]byte("test2"),
				[]byte("test3"),
			}
			waitChan := make(chan struct{})
			receivedMessageChan := make(chan []byte)
			receivedMessages := make([][]byte, 0)
			go func() {
				for b := range receivedMessageChan {
					receivedMessages = append(receivedMessages, b)
					if len(receivedMessages) == len(expectedMessages) {
						closeReceiver()
						close(waitChan)
						break
					}
				}
			}()
			go func() {
				receive(func(ctx context.Context, b []byte) error {
					select {
					case receivedMessageChan <- b:
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				})
			}()

			errs := send(ctx, expectedMessages)
			Expect(errs).To(BeNil())
			select {
			case <-waitChan:
			case <-ctx.Done():
			}

			closeReceiver()
			Expect(receivedMessages).To(ConsistOf(
				[]byte("test1"),
				[]byte("test2"),
				[]byte("test3"),
			))
		})
	})
})
