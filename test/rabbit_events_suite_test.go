package test

import (
	"log/slog"
	"testing"

	rabbitEvents "github.com/getkalido/rabbit-events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	slogformatter "github.com/samber/slog-formatter"
)

func TestRabbitEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RabbitEvents Suite")
}

var _ = BeforeSuite(func() {
	oldLogger := rabbitEvents.DefaultLogger()

	rabbitEvents.SetDefaultLogger(
		slog.New(
			slogformatter.NewFormatterHandler(
				slogformatter.ErrorFormatter("reason"),
			)(
				slog.NewJSONHandler(
					GinkgoWriter,
					&slog.HandlerOptions{
						Level: slog.LevelDebug,
					},
				),
			),
		),
	)
	DeferCleanup(func() {
		rabbitEvents.SetDefaultLogger(oldLogger)
	})
})
