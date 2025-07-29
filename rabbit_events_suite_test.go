package rabbitevents_test

import (
	"log/slog"
	"testing"

	rabbitevents "github.com/getkalido/rabbit-events"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	slogformatter "github.com/samber/slog-formatter"
)

func TestRabbitEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RabbitEvents Suite")
}

var _ = BeforeSuite(func() {
	oldLogger := rabbitevents.DefaultLogger()

	rabbitevents.SetDefaultLogger(
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
		rabbitevents.SetDefaultLogger(oldLogger)
	})
})
