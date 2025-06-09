package test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rabbitevents "github.com/getkalido/rabbit-events"
	gomock "github.com/golang/mock/gomock"
	fuzz "github.com/google/gofuzz"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ rabbitevents.RabbitConfig = (*TestConfig)(nil)

type TestConfig struct {
	Host              string
	UserName          string
	Password          string
	ConnectionTimeout time.Duration
}

func (c *TestConfig) GetHost() string {
	if c == nil {
		return ""
	}
	return c.Host
}

func (c *TestConfig) GetUserName() string {
	if c == nil {
		return ""
	}
	return c.UserName
}

func (c *TestConfig) GetPassword() string {
	if c == nil {
		return ""
	}
	return c.Password
}

func (c *TestConfig) GetConnectTimeout() time.Duration {
	if c == nil {
		return 0
	}
	return c.GetConnectTimeout()
}

var _ = Describe("Benchmark", Label("measurement"), Ordered, Serial, func() {
	var (
		exchangeSettings = rabbitevents.ExchangeSettings{
			Name:         "test-exchange-benchmark",
			ExchangeType: rabbitevents.ExchangeTypeTopic,
			AutoDelete:   true,
			Durable:      false,
			NoWait:       false,
			Args:         nil,
		}
		samplingCfg = gmeasure.SamplingConfig{
			N:        20,
			Duration: 1 * time.Minute,
		}
	)

	type FakeEntry struct {
		SomeNumber int     `json:"some_number"`
		SomeString string  `json:"some_string"`
		SomeBool   bool    `json:"some_bool"`
		SomeFloat  float64 `json:"some_float"`
	}

	var fakeEntries []*FakeEntry
	BeforeAll(func(specCtx SpecContext) {
		fakeEntries = make([]*FakeEntry, 0, 10_000)
		fuzzer := fuzz.NewWithSeed(GinkgoT().RandomSeed()).
			NumElements(10_000, 10_000)
		fuzzer.Fuzz(&fakeEntries)

		conn, err := amqp.Dial(
			fmt.Sprintf(
				"amqp://%s:%s@%s/",
				"guest",
				"guest",
				"localhost:5672",
			),
		)
		Expect(err).ToNot(HaveOccurred(), "Failed to connect to RabbitMQ")
		DeferCleanup(conn.Close)

		ch, err := conn.Channel()
		Expect(err).ToNot(HaveOccurred(), "Failed to open channel")
		DeferCleanup(ch.Close)

		Expect(ch.ExchangeDeclare(
			exchangeSettings.Name,
			rabbitevents.ExchangeTypeTopic,
			false,
			true,
			false,
			false,
			nil,
		)).To(Succeed())

		placeholderQueue, err := ch.QueueDeclare(
			"placeholder-queue",
			false,
			true,
			true,
			false,
			nil,
		)
		Expect(err).ToNot(HaveOccurred(), "Failed to declare placeholder queue")
		DeferCleanup(func() {
			_, err := ch.QueueDelete(placeholderQueue.Name, false, false, false)
			Expect(err).ToNot(HaveOccurred(), "Failed to delete placeholder queue")
		})

		Expect(ch.QueueBind(
			placeholderQueue.Name,
			"placeholder",
			exchangeSettings.Name,
			false,
			nil,
		)).To(Succeed())
	})

	CreateLongLivedQueue := func(
		exchangeName string,
		queueSettings rabbitevents.QueueSettings,
	) {
		GinkgoHelper()
		By("Creating queue for benchmark", func() {
			conn, err := amqp.Dial(
				fmt.Sprintf(
					"amqp://%s:%s@%s/",
					"guest",
					"guest",
					"localhost:5672",
				),
			)
			Expect(err).ToNot(HaveOccurred(), "Failed to connect to RabbitMQ")
			DeferCleanup(conn.Close)

			ch, err := conn.Channel()
			Expect(err).ToNot(HaveOccurred(), "Failed to open channel")
			DeferCleanup(ch.Close)

			_, err = ch.QueueDelete(queueSettings.Name, false, false, false)
			Expect(err).ToNot(HaveOccurred(), "Failed to delete queue %s", queueSettings.Name)

			queue, err := ch.QueueDeclare(
				queueSettings.Name,
				queueSettings.Durable,
				queueSettings.AutoDelete,
				queueSettings.Exclusive,
				queueSettings.NoWait,
				queueSettings.Args,
			)
			Expect(err).ToNot(HaveOccurred())
			DeferCleanup(func() {
				_, err := ch.QueueDelete(queue.Name, false, false, false)
				Expect(err).ToNot(HaveOccurred(), "Failed to delete queue %s", queue.Name)
			})

			Expect(ch.QueueBind(
				queue.Name,               // queue name
				queueSettings.RoutingKey, // routing key
				exchangeName,             // exchange name
				queueSettings.NoWait,     // no-wait
				queueSettings.BindArgs,   //args
			)).To(Succeed())
		})
	}

	SampledExperiment := func(
		ctx context.Context,
		experimentName string,
		exchangeSettings rabbitevents.ExchangeSettings,
		queueSettings rabbitevents.QueueSettings,
		fakeEntries []*FakeEntry,
		samplingCfg gmeasure.SamplingConfig,
		fn func(
			ctx context.Context,
			experiment *gmeasure.Experiment,
			exchange rabbitevents.RabbitExchange,
		),
	) {
		GinkgoHelper()
		ctrl := gomock.NewController(GinkgoT())
		DeferCleanup(ctrl.Finish)
		experiment := gmeasure.NewExperiment(experimentName)
		AddReportEntry(experiment.Name, experiment)
		experiment.Sample(
			func(idx int) {
				cfg := GetTestConfig(ctrl)
				exchange := rabbitevents.NewRabbitExchange(
					cfg,
					rabbitevents.WithRetryAutoDelete(),
				)
				receive, receiveClose, err := exchange.Receive(
					exchangeSettings,
					queueSettings,
				)
				receiveClose = sync.OnceFunc(receiveClose)
				DeferCleanup(receiveClose)
				Expect(err).ToNot(HaveOccurred())

				fn(ctx, experiment, exchange)
				receiveCounter := atomic.Int32{}
				timeout := time.NewTimer(5 * time.Second)
				DeferCleanup(timeout.Stop)
				go func() {
					select {
					case <-ctx.Done():
					case <-timeout.C:
					}
					receiveClose()
				}()
				experiment.MeasureDuration("emptying queue", func() {
					Expect(receive(func(_ context.Context, msg []byte) error {
						select {
						case <-ctx.Done():
							receiveClose()
							Fail("Context cancelled before receiving all messages")
							return ctx.Err()
						case <-timeout.C:
							receiveClose()
							Fail("Timed out before receiving all messages")
							return context.Canceled
						default:
							if receiveCounter.Add(1) >= int32(len(fakeEntries)) {
								receiveClose()
							}
							return nil
						}
					})).To(Succeed())
				})
				Expect(receiveCounter.Load()).To(
					BeEquivalentTo(len(fakeEntries)),
					"Expected to receive all messages",
				)
			},
			samplingCfg,
		)
	}

	It("sends messages individually", NodeTimeout(80*time.Second), func(ctx context.Context) {
		queueSettings := rabbitevents.QueueSettings{
			Name:       "test-queue-benchmark-single",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
			Args:       nil,
			RoutingKey: "test.queue-benchmark-single",
		}
		CreateLongLivedQueue(exchangeSettings.Name, queueSettings)
		SampledExperiment(
			ctx,
			"Send (singular)",
			exchangeSettings,
			queueSettings,
			fakeEntries,
			samplingCfg,
			func(ctx context.Context, experiment *gmeasure.Experiment, exchange rabbitevents.RabbitExchange) {
				send := exchange.SendTo(
					exchangeSettings.Name,
					rabbitevents.ExchangeTypeTopic,
					false,
					true,
					queueSettings.RoutingKey,
				)
				msgs := make([][]byte, 0, len(fakeEntries))
				experiment.MeasureDuration("marshalling", func() {
					for _, entry := range fakeEntries {
						b, err := json.Marshal(entry)
						Expect(err).ToNot(HaveOccurred())
						msgs = append(msgs, b)
					}
				})
				experiment.MeasureDuration("sending", func() {
					for _, entry := range msgs {
						Expect(send(ctx, entry)).To(Succeed())
					}
				})
			},
		)
	})

	It("sends messages in bulk", NodeTimeout(80*time.Second), func(ctx context.Context) {
		queueSettings := rabbitevents.QueueSettings{
			Name:       "test-queue-benchmark-bulk",
			Durable:    false,
			AutoDelete: true,
			Exclusive:  false,
			NoWait:     false,
			Args:       nil,
			RoutingKey: "test.queue-benchmark-bulk",
		}
		CreateLongLivedQueue(exchangeSettings.Name, queueSettings)
		SampledExperiment(
			ctx,
			"Send (bulk)",
			exchangeSettings,
			queueSettings,
			fakeEntries,
			samplingCfg,
			func(ctx context.Context, experiment *gmeasure.Experiment, exchange rabbitevents.RabbitExchange) {
				send := exchange.BulkSendTo(
					exchangeSettings.Name,
					rabbitevents.ExchangeTypeTopic,
					false,
					true,
					queueSettings.RoutingKey,
				)

				msgs := make([][]byte, 0, len(fakeEntries))
				experiment.MeasureDuration("marshalling", func() {
					for _, entry := range fakeEntries {
						b, err := json.Marshal(entry)
						Expect(err).ToNot(HaveOccurred())
						msgs = append(msgs, b)
					}
				})
				experiment.MeasureDuration("sending", func() {
					errs := send(ctx, msgs)
					Expect(errs).To(BeEmpty(), "Expected no errors in bulk send")
				})
			},
		)
	})
})
