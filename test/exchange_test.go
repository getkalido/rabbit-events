package test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	time "time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/streadway/amqp"

	. "github.com/getkalido/rabbit-events"
)

type TestDataStrucure struct {
	V string
}

var _ = Describe("ExchangeInterface", func() {
	BeforeEach(func() {
		conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", "guest", "guest", "localhost:5672"))
		Expect(err).To(BeNil())

		ch, err := conn.Channel()
		Expect(err).To(BeNil())

		// Although we go to great efforts to ensure that we read all the messages we write in the test, a canceled test might leave things behind.
		// So we delete the queues before the tests to ensure they are clean
		_, err = ch.QueueDelete(fmt.Sprintf("%s-%s-%s", "eventChannel", "path", "processName"), false, false, false)
		Expect(err).To(BeNil())

		_, err = ch.QueueDelete(fmt.Sprintf("%s-%s-%s-%s", "eventChannel", "path", "processName", "quorum"), false, false, false)
		Expect(err).To(BeNil())

		Expect(ch.Close()).To(Succeed())
		Expect(conn.Close()).To(Succeed())
	})

	Describe("Receive", func() {
		It("Should work like core and event processors talk to each other", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672").Times(2)
			mockConfig.EXPECT().GetUserName().Return("guest").Times(2)
			mockConfig.EXPECT().GetPassword().Return("guest").Times(2)
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()

			ctx := context.Background()

			By("Listening for changes like matchServer does")
			exchange := NewRabbitExchange(mockConfig)
			handle, stop, err := exchange.Receive(ExchangeSettings{
				Name:         "eventChannel",
				Exclusive:    false,
				Durable:      true,
				AutoDelete:   false,
				ExchangeType: ExchangeTypeTopic,
				NoWait:       false,
				Args:         nil,
			}, QueueSettings{
				Name:       fmt.Sprintf("%s-%s-%s", "eventChannel", "path", "processName"),
				Durable:    true,
				Exclusive:  false,
				RoutingKey: "path",
			})
			Expect(err).To(BeNil())
			var wg sync.WaitGroup
			defer wg.Wait()
			defer stop()

			By("Sending messages like core does")
			eventHandler := NewRabbitEventHandler(
				exchange,
				"eventChannel",
				5,
			)

			emitter := eventHandler.Emit("path")

			events := make(chan *Event, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				err = handle(func(ctx context.Context, b []byte) error {
					defer GinkgoRecover()
					e := &Event{}
					e.State = &TestDataStrucure{}
					e.Old = &TestDataStrucure{}

					err := json.Unmarshal(b, e)
					if err != nil {
						return err
					}
					Eventually(events).Should(BeSent(e))
					return nil
				})
				Expect(err).To(BeNil())
			}()

			err = emitter(ctx, Create, nil, 1, &TestDataStrucure{V: "1"}, &TestDataStrucure{V: "2"})

			Eventually(events).Should(Receive(PointTo(MatchFields(IgnoreExtras, Fields{
				"State":  Equal(&TestDataStrucure{V: "2"}),
				"Old":    Equal(&TestDataStrucure{V: "1"}),
				"Action": Equal(Create),
				"Path":   Equal("path"),
				"ID":     Equal(int64(1)),
			}))))
		})

		It("Should work like core and event processors talk to each other, if match used quorum", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()
			mockConfig := NewMockRabbitConfig(ctrl)
			mockConfig.EXPECT().GetHost().Return("localhost:5672").Times(2)
			mockConfig.EXPECT().GetUserName().Return("guest").Times(2)
			mockConfig.EXPECT().GetPassword().Return("guest").Times(2)
			mockConfig.EXPECT().GetConnectTimeout().Return(time.Second * 5).AnyTimes()

			ctx := context.Background()

			By("Listening for changes like matchServer does")
			exchange := NewRabbitExchange(mockConfig)
			handle, stop, err := exchange.Receive(ExchangeSettings{
				Name:         "eventChannel",
				Exclusive:    false,
				Durable:      true,
				AutoDelete:   false,
				ExchangeType: ExchangeTypeTopic,
				NoWait:       false,
				Args:         nil,
			}, QueueSettings{
				Name:       fmt.Sprintf("%s-%s-%s-%s", "eventChannel", "path", "processName", "quorum"),
				Durable:    true,
				Exclusive:  false,
				RoutingKey: "path",
				Args: map[string]interface{}{
					"x-queue-type": "quorum",
				},
			})
			Expect(err).To(BeNil())
			var wg sync.WaitGroup
			defer wg.Wait()
			defer stop()

			By("Sending messages like core does")
			eventHandler := NewRabbitEventHandler(
				exchange,
				"eventChannel",
				5,
			)

			emitter := eventHandler.Emit("path")

			events := make(chan *Event, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				err = handle(func(ctx context.Context, b []byte) error {
					defer GinkgoRecover()
					e := &Event{}
					e.State = &TestDataStrucure{}
					e.Old = &TestDataStrucure{}

					err := json.Unmarshal(b, e)
					if err != nil {
						return err
					}
					By("match receiving the event")
					Eventually(events).Should(BeSent(e))
					return nil
				})
				Expect(err).To(BeNil())
			}()

			By("core emitting the event")
			err = emitter(ctx, Create, nil, 1, &TestDataStrucure{V: "1"}, &TestDataStrucure{V: "2"})
			By("passing the event along")
			Eventually(events).Should(Receive(PointTo(MatchFields(IgnoreExtras, Fields{
				"State":  Equal(&TestDataStrucure{V: "2"}),
				"Old":    Equal(&TestDataStrucure{V: "1"}),
				"Action": Equal(Create),
				"Path":   Equal("path"),
				"ID":     Equal(int64(1)),
			}))))

		})

	})

})
