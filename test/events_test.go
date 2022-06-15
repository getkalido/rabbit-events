package test

import (
	"context"
	"encoding/json"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/getkalido/rabbit-events"
)

var _ = Describe("Events", func() {
	Describe("ImplRabbitEventHandler", func() {
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

		Describe("eventObserver", func() {
			It("Should handle consuming events", func() {
				ctrl := gomock.NewController(GinkgoT())
				defer ctrl.Finish()

				type testSerial struct {
					Thing      string
					OtherThing int64
				}

				testEvent := &Event{
					Old: &testSerial{},
					State: &testSerial{
						Thing:      "thing",
						OtherThing: 5,
					},
				}

				exchange := NewMockRabbitExchange(ctrl)

				cancelChan := make(chan struct{})
				pathMap := map[string]interface{}{
					"test.test-a": 1,
					"test.test-b": 2,
					"test.test-c": 3,
				}
				msg, err := json.Marshal(testEvent)
				Expect(err).To(BeNil())
				handleFunc := func(fn MessageHandleFunc) error {
					defer GinkgoRecover()
					err := fn(context.TODO(), msg, pathMap)
					Expect(err).To(BeNil())
					cancelChan <- struct{}{}
					return nil
				}
				cancelFunc := func() {}
				bindFunc := func(routingKey string, bindArgs map[string]interface{}) error { return nil }
				unbindFunc := func(routingKey string, bindArgs map[string]interface{}) error { return nil }
				exchange.EXPECT().ReceiveMultiple(gomock.Any(), gomock.Any()).
					Return(
						handleFunc,
						cancelFunc,
						bindFunc,
						unbindFunc,
						nil,
					)

				eventHandler := NewRabbitEventHandler(exchange, "test-exchange", 0)
				consumer, err := eventHandler.Consume("test.test-a", func() interface{} { return &testSerial{} })
				Expect(err).To(BeNil())

				unsubscribe := consumer.SubscribeUnfiltered(func(e *Event) {
					defer GinkgoRecover()
					Expect(e).To(Equal(testEvent))
				})
				defer unsubscribe()

				timeout := time.After(5 * time.Second)
				select {
				case <-cancelChan:
				case <-timeout:
				}
			})
		})

		Describe("multiEventObserver", func() {
			It("Should handle emitting mutliple events", func() {
				ctrl := gomock.NewController(GinkgoT())
				defer ctrl.Finish()

				type testSerial struct {
					Thing      string
					OtherThing int64
				}

				testObject := &testSerial{
					Thing:      "thing",
					OtherThing: 5,
				}

				paths := []string{"test.test-a", "test.test-b", "test.test-c"}
				exchange := NewMockRabbitExchange(ctrl)
				topicExchangeRunnerFactory := func(expectedPath string, expectedID int64) MessageHandleFunc {
					return func(ctx context.Context, msg []byte, pathMap map[string]interface{}) error {
						defer GinkgoRecover()
						Expect(ctx).ToNot(BeNil())
						actualEvent := Event{
							Old:   &testSerial{},
							State: &testSerial{},
						}
						err := json.Unmarshal(msg, &actualEvent)
						Expect(err).To(BeNil())
						Expect(actualEvent).ToNot(BeNil())
						Expect(actualEvent.Path).To(Equal(expectedPath))
						actualObject, ok := actualEvent.State.(*testSerial)
						Expect(ok).To(BeTrue())
						Expect(actualObject).To(Equal(testObject))
						actualID, ok := pathMap[expectedPath]
						Expect(ok).To(BeTrue())
						Expect(actualID).To(Equal(expectedID))
						return nil
					}
				}
				headersExchangeRunnerFactory := func() MessageHandleFunc {
					return func(ctx context.Context, msg []byte, pathMap map[string]interface{}) error {
						defer GinkgoRecover()
						Expect(ctx).ToNot(BeNil())
						actualEvent := Event{
							Old:   &testSerial{},
							State: &testSerial{},
						}
						err := json.Unmarshal(msg, &actualEvent)
						Expect(err).To(BeNil())
						Expect(actualEvent).ToNot(BeNil())
						_, ok := pathMap[actualEvent.Path]
						Expect(ok).To(BeTrue())
						actualObject, ok := actualEvent.State.(*testSerial)
						Expect(ok).To(BeTrue())
						Expect(actualObject).To(Equal(testObject))
						return nil
					}
				}

				exchange.EXPECT().SendTo("test-exchange-headers", ExchangeTypeHeaders, true, false, "").
					Return(headersExchangeRunnerFactory())
				exchange.EXPECT().SendTo("test-exchange", ExchangeTypeTopic, true, false, "test.test-a").
					Return(topicExchangeRunnerFactory("test.test-a", 1))
				exchange.EXPECT().SendTo("test-exchange", ExchangeTypeTopic, true, false, "test.test-b").
					Return(topicExchangeRunnerFactory("test.test-b", 2))
				exchange.EXPECT().SendTo("test-exchange", ExchangeTypeTopic, true, false, "test.test-c").
					Return(topicExchangeRunnerFactory("test.test-c", 3))

				eventHandler := NewRabbitEventHandler(exchange, "test-exchange", 0)
				emitter := eventHandler.EmitMultiple(paths)
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				defer cancel()

				pathMap := map[string]int64{
					"test.test-a": 1,
					"test.test-b": 2,
					"test.test-c": 3,
				}
				err := emitter(ctx, Create, nil, nil, testObject, pathMap)
				Expect(err).To(BeNil())
			})

			It("Should handle consuming multiple events", func() {
				ctrl := gomock.NewController(GinkgoT())
				defer ctrl.Finish()

				type testSerial struct {
					Thing      string
					OtherThing int64
				}

				testEvent := &Event{
					Old: &testSerial{},
					State: &testSerial{
						Thing:      "thing",
						OtherThing: 5,
					},
				}

				exchange := NewMockRabbitExchange(ctrl)

				cancelChan := make(chan struct{})
				pathMap := map[string]interface{}{
					"test.test-a": 1,
					"test.test-b": 2,
					"test.test-c": 3,
				}
				msg, err := json.Marshal(testEvent)
				Expect(err).To(BeNil())
				handleFunc := func(fn MessageHandleFunc) error {
					defer GinkgoRecover()
					err := fn(context.TODO(), msg, pathMap)
					Expect(err).To(BeNil())
					cancelChan <- struct{}{}
					return nil
				}
				cancelFunc := func() {}
				bindFunc := func(routingKey string, bindArgs map[string]interface{}) error { return nil }
				unbindFunc := func(routingKey string, bindArgs map[string]interface{}) error { return nil }
				exchange.EXPECT().ReceiveMultiple(gomock.Any(), gomock.Any()).
					Return(
						handleFunc,
						cancelFunc,
						bindFunc,
						unbindFunc,
						nil,
					)

				eventHandler := NewRabbitEventHandler(exchange, "test-exchange", 0).(*ImplRabbitEventHandler)
				consumer, err := eventHandler.ConsumeMultiple(func() interface{} { return &testSerial{} })
				Expect(err).To(BeNil())

				subscribePaths := map[string][]int64{
					"test.test-a": {},
					"test.test-b": {},
					"test.test-c": {},
				}
				unsubscribe, err := consumer.Subscribe(subscribePaths, func(e *Event) {
					defer GinkgoRecover()
					Expect(e).To(Equal(testEvent))
				})
				Expect(err).To(BeNil())
				defer unsubscribe()

				timeout := time.After(5 * time.Second)
				select {
				case <-cancelChan:
				case <-timeout:
				}
			})
		})
	})
})
